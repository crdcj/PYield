"""Bootstrap de forwards para a curva zero de NTN-B."""

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass

import polars as pl

from pyield import du
from pyield._internal.types import ArrayLike, DateLike, DatesLike, any_is_empty
from pyield.tpf.titulos import _utils as utils

DIA_VENCIMENTO = 15
MAX_EXPANSOES_INTERVALO = 32


def _gerar_vertices_mensais(
    data_liquidacao: dt.date, ultimo_vencimento: dt.date
) -> list[dt.date]:
    """Gera vértices mensais no dia 15."""
    ancora = utils.subtrair_meses(data_liquidacao.replace(day=DIA_VENCIMENTO), 1)
    datas = pl.date_range(ancora, ultimo_vencimento, interval="1mo", eager=True)
    return datas.filter(datas.is_between(data_liquidacao, ultimo_vencimento)).to_list()


def _taxas_zero_por_forwards(
    dias_uteis: list[int], taxas_forward: list[float]
) -> list[float]:
    """Acumula taxas zero a partir de forwards constantes por trecho."""
    taxas_zero = [taxas_forward[0]]

    for du_anterior, du_atual, taxa_forward in zip(
        dias_uteis[:-1], dias_uteis[1:], taxas_forward[1:], strict=True
    ):
        taxa_zero_anterior = taxas_zero[-1]
        fator_acumulado = (1 + taxa_zero_anterior) ** (du_anterior / 252)
        fator_forward = (1 + taxa_forward) ** ((du_atual - du_anterior) / 252)
        taxas_zero.append((fator_acumulado * fator_forward) ** (252 / du_atual) - 1)

    return taxas_zero


def _calcular_cotacao(
    fluxos: pl.DataFrame, dias_uteis: pl.Series, taxas: list[float]
) -> float:
    """Calcula a cotação dos fluxos pela taxa de cada prazo."""
    return utils.calcular_pv(
        fluxos_caixa=fluxos["valor_pagamento"],
        taxas=taxas,
        prazos=dias_uteis / 252,
    )


def _resolver_taxa_forward(
    erro: Callable[[float], float], taxa_inicial: float
) -> float:
    """Resolve uma taxa forward por bisseção."""
    erro_inicial = erro(taxa_inicial)
    if erro_inicial == 0:
        return taxa_inicial

    if erro_inicial > 0:
        limite_inferior = taxa_inicial
        limite_superior = max(1.0, 2 * taxa_inicial + 0.01)
        erro_superior = erro(limite_superior)
        for _ in range(MAX_EXPANSOES_INTERVALO):
            if erro_superior <= 0:
                break
            limite_superior = 2 * limite_superior + 1
            erro_superior = erro(limite_superior)
        if erro_superior > 0:
            raise RuntimeError(
                "Não foi possível encontrar um intervalo para a taxa forward."
            )
    else:
        limite_inferior = -0.99
        limite_superior = taxa_inicial

    return utils._metodo_bissecao(erro, limite_inferior, limite_superior)


def _taxas_forward_vertices(
    vertices: list[dt.date],
    vencimentos: list[dt.date],
    taxas_forward: list[float],
) -> list[float]:
    """Seleciona a taxa do próximo vencimento quando necessário."""
    indice_titulo = 0
    resultado = []
    for vertice in vertices:
        while vertice > vencimentos[indice_titulo]:
            indice_titulo += 1
        resultado.append(taxas_forward[indice_titulo])
    return resultado


@dataclass
class _ContextoBootstrapForwards:
    """Dados compartilhados pela calibração sequencial de forwards."""

    data_liquidacao: dt.date
    vertices: list[dt.date]
    dias_vertices: list[int]
    indice_por_data: dict[dt.date, int]
    vencimentos: list[dt.date]
    taxas_tir: list[float]
    taxas_forward: list[float]


def _calibrar_taxa_forward(
    contexto: _ContextoBootstrapForwards,
    indice_titulo: int,
) -> float:
    """Calibra um forward para reproduzir a cotação de uma NTN-B."""
    from pyield.tpf.titulos.ntnb import fluxos_caixa  # noqa: PLC0415

    vencimento = contexto.vencimentos[indice_titulo]
    fluxos = fluxos_caixa(contexto.data_liquidacao, vencimento)
    dias_fluxos = du.contar(contexto.data_liquidacao, fluxos["data_pagamento"])
    indices_fluxos = [
        contexto.indice_por_data[data] for data in fluxos["data_pagamento"]
    ]
    cotacao_alvo = _calcular_cotacao(
        fluxos, dias_fluxos, [contexto.taxas_tir[indice_titulo]] * fluxos.height
    )

    def erro(taxa_forward: float) -> float:
        contexto.taxas_forward[indice_titulo] = taxa_forward
        curva_zero = _taxas_zero_por_forwards(
            contexto.dias_vertices,
            _taxas_forward_vertices(
                contexto.vertices, contexto.vencimentos, contexto.taxas_forward
            ),
        )
        taxas_fluxos = [curva_zero[indice] for indice in indices_fluxos]
        return _calcular_cotacao(fluxos, dias_fluxos, taxas_fluxos) - cotacao_alvo

    return _resolver_taxa_forward(erro, contexto.taxas_tir[indice_titulo])


def taxas_zero(
    data_liquidacao: DateLike,
    vencimentos: DatesLike,
    taxas: ArrayLike,
    *,
    percentual: bool = False,
) -> pl.DataFrame:
    r"""
    Calcula a curva zero de NTN-B pelo bootstrap de forwards.

    Retorna apenas os vencimentos informados, com taxas em base 252 dias úteis.
    As entradas podem ser as taxas indicativas da ANBIMA obtidas por
    ``yd.ntnb.dados`` ou TIRs fornecidas pelo usuário.

    O método calcula as taxas zero a partir das TIRs observadas das NTN-B,
    calibrando uma taxa forward por trecho para que os fluxos descontados pela
    curva zero reproduzam a cotação obtida pela TIR de cada título.
    A calibração é iterativa e tem convergência condicional: depende da existência
    de um intervalo válido para cada raiz.

    Notes:
        **Racional**

        Uma TIR desconta todos os fluxos de um título por uma única taxa. Uma
        curva zero, por outro lado, precisa de uma taxa para cada data de fluxo.
        O bootstrap transforma as TIRs em uma estrutura de taxas forward e, a
        partir dela, em taxas zero.

        **Curva de forwards e taxa zero**

        A curva usa vértices mensais no dia 15. Para cada NTN-B, a taxa forward
        calibrada é mantida constante desde o vencimento anterior até o seu
        vencimento. A primeira taxa forward começa na TIR do título mais curto.

        Se \(DU_i\) é o número de dias úteis do vértice \(i\), \(f_i\) é a
        taxa forward do trecho e \(z_i\) é a taxa zero anualizada, então:

        \[
        z_0 = f_0
        \]

        \[
        (1 + z_i)^{DU_i / 252} =
        (1 + z_{i-1})^{DU_{i-1} / 252}
        (1 + f_i)^{(DU_i - DU_{i-1}) / 252}
        \]

        **Calibração sequencial**

        Para cada título, do menor para o maior vencimento, a função calcula a
        cotação-alvo \(P_i^{\mathrm{TIR}}\) descontando seus fluxos pela TIR
        observada. Em seguida, busca por bisseção o forward (f_i) que zera:

        \[
        E_i(f_i) =
        \sum_k \frac{CF_{i,k}}{(1 + z(t_{i,k}; f_i))^{DU_{i,k} / 252}}
        - P_i^{\mathrm{TIR}}
        \]

        Os forwards e taxas zero já calibrados nos títulos curtos permanecem
        fixos durante a calibração dos títulos longos. Por isso, cada etapa tem
        apenas uma incógnita e é resolvida por bisseção quando há mudança de
        sinal no intervalo pesquisado. A coluna ``taxa_forward`` retorna essa
        incógnita calibrada: em cada linha, ela é constante no trecho que termina
        no respectivo vencimento. O primeiro trecho começa na liquidação.

        **Convergência condicional**

        A bisseção é determinística quando encontra um intervalo que contém uma
        raiz. A função tenta expandir o limite superior para encontrar esse
        intervalo, mas pode não encontrá-lo para entradas incompatíveis ou
        extremos. Nesse caso, a calibração não produz uma curva e lança
        ``RuntimeError``.

        **Precisão do método**

        A calibração usa \(DU / 252\) sem truncamento e soma os valores
        presentes sem arredondamento. Isso difere de :func:`cotacao`, que
        aplica as regras ANBIMA de arredondamento dos fluxos e truncamento da
        cotação.

    Args:
        data_liquidacao: Data de liquidação.
        vencimentos: Datas de vencimento das NTN-B.
        taxas: TIRs correspondentes em formato decimal (ex.: 0.10 para 10%).
        percentual: Se True, retorna as colunas de taxa em percentual
            (10.0 para 10%). Se False, retorna em decimal (0.10 para 10%).
            As TIRs recebidas em ``taxas`` devem ser sempre decimais.
            O padrão é False.

    Returns:
        pl.DataFrame: Curva zero calibrada pelo bootstrap de forwards. Retorna vazio quando
            não restarem vencimentos posteriores à liquidação.

    Raises:
        RuntimeError: Se não for possível encontrar um intervalo válido para
            alguma taxa forward.

    Output Columns:
        - data_vencimento (Date): Data do vértice da curva.
        - dias_uteis (Int64): Dias úteis entre liquidação e vértice.
        - taxa_tir (Float64): TIR recebida na entrada, na escala escolhida.
        - taxa_forward (Float64): Parâmetro calibrado por bisseção para o trecho
            que termina no vencimento, na escala escolhida.
        - taxa_zero (Float64): Taxa zero real anualizada na escala escolhida.

    Examples:
        >>> from pyield import ntnb
        >>> # Taxas indicativas da ANBIMA na data de referência.
        >>> df = ntnb.dados("16-08-2024")
        >>> curva_percentual = ntnb.taxas_zero(
        ...     data_liquidacao="16-08-2024",
        ...     vencimentos=df["data_vencimento"],
        ...     taxas=df["taxa_indicativa"],
        ...     percentual=True,
        ... )
        >>> curva_percentual
        shape: (14, 5)
        ┌─────────────────┬────────────┬──────────┬──────────────┬───────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_tir ┆ taxa_forward ┆ taxa_zero │
        │ ---             ┆ ---        ┆ ---      ┆ ---          ┆ ---       │
        │ date            ┆ i64        ┆ f64      ┆ f64          ┆ f64       │
        ╞═════════════════╪════════════╪══════════╪══════════════╪═══════════╡
        │ 2025-05-15      ┆ 185        ┆ 6.3893   ┆ 6.3893       ┆ 6.3893    │
        │ 2026-08-15      ┆ 502        ┆ 6.6095   ┆ 6.745466     ┆ 6.614071  │
        │ 2027-05-15      ┆ 687        ┆ 6.4164   ┆ 5.853671     ┆ 6.40877   │
        │ 2028-08-15      ┆ 1002       ┆ 6.3199   ┆ 6.080954     ┆ 6.305605  │
        │ 2029-05-15      ┆ 1186       ┆ 6.1753   ┆ 5.279866     ┆ 6.145816  │
        │ …               ┆ …          ┆ …        ┆ …            ┆ …         │
        │ 2040-08-15      ┆ 4009       ┆ 5.8873   ┆ 5.6644       ┆ 5.832614  │
        │ 2045-05-15      ┆ 5196       ┆ 6.0013   ┆ 6.729968     ┆ 6.036943  │
        │ 2050-08-15      ┆ 6511       ┆ 6.0247   ┆ 6.234593     ┆ 6.076832  │
        │ 2055-05-15      ┆ 7700       ┆ 5.9926   ┆ 5.521283     ┆ 5.990856  │
        │ 2060-08-15      ┆ 9017       ┆ 6.0179   ┆ 6.498313     ┆ 6.064823  │
        └─────────────────┴────────────┴──────────┴──────────────┴───────────┘

        O bootstrap considera apenas os fluxos posteriores à liquidação:
        >>> df = ntnb.dados("15-05-2026")
        >>> curva_percentual = ntnb.taxas_zero(
        ...     data_liquidacao="18-05-2026",
        ...     vencimentos=df["data_vencimento"],
        ...     taxas=df["taxa_indicativa"],
        ...     percentual=True,
        ... )
        >>> curva_percentual
        shape: (15, 5)
        ┌─────────────────┬────────────┬──────────┬──────────────┬───────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_tir ┆ taxa_forward ┆ taxa_zero │
        │ ---             ┆ ---        ┆ ---      ┆ ---          ┆ ---       │
        │ date            ┆ i64        ┆ f64      ┆ f64          ┆ f64       │
        ╞═════════════════╪════════════╪══════════╪══════════════╪═══════════╡
        │ 2026-08-15      ┆ 64         ┆ 10.2013  ┆ 10.2013      ┆ 10.2013   │
        │ 2027-05-15      ┆ 249        ┆ 8.12     ┆ 7.395284     ┆ 8.109613  │
        │ 2028-08-15      ┆ 564        ┆ 8.1131   ┆ 8.097792     ┆ 8.103011  │
        │ 2029-05-15      ┆ 748        ┆ 8.0391   ┆ 7.808764     ┆ 8.030555  │
        │ 2030-08-15      ┆ 1062       ┆ 8.066    ┆ 8.126503     ┆ 8.058915  │
        │ …               ┆ …          ┆ …        ┆ …            ┆ …         │
        │ 2040-08-15      ┆ 3571       ┆ 7.4812   ┆ 6.670623     ┆ 7.341193  │
        │ 2045-05-15      ┆ 4758       ┆ 7.3767   ┆ 6.785801     ┆ 7.202367  │
        │ 2050-08-15      ┆ 6073       ┆ 7.3      ┆ 6.642902     ┆ 7.080976  │
        │ 2055-05-15      ┆ 7262       ┆ 7.2662   ┆ 6.768381     ┆ 7.029733  │
        │ 2060-08-15      ┆ 8579       ┆ 7.2674   ┆ 7.190818     ┆ 7.054446  │
        └─────────────────┴────────────┴──────────┴──────────────┴───────────┘

        A curva retornada contém apenas os vencimentos informados. Para estimar
        uma taxa zero em um prazo intermediário, use o método de interpolação
        desejado. Neste exemplo, ``flat_forward`` preserva a hipótese de
        forwards constantes por trecho usada no bootstrap:
        >>> curva_decimal = ntnb.taxas_zero(
        ...     data_liquidacao="18-05-2026",
        ...     vencimentos=df["data_vencimento"],
        ...     taxas=df["taxa_indicativa"],
        ... )
        >>> interpolar = yd.Interpolador(
        ...     curva_decimal["dias_uteis"],
        ...     curva_decimal["taxa_zero"],
        ...     metodo="flat_forward",
        ... )
        >>> du_intermediario = (
        ...     curva_decimal["dias_uteis"][0] + curva_decimal["dias_uteis"][1]
        ... ) // 2
        >>> 0 < interpolar(du_intermediario) < 1
        True
    """
    from pyield.tpf.titulos.ntnb import (  # noqa: PLC0415
        _validar_entradas_taxas_zero,
    )

    if any_is_empty(data_liquidacao, vencimentos, taxas):
        return pl.DataFrame()

    liquidacao, vencimentos, taxas = _validar_entradas_taxas_zero(
        data_liquidacao, vencimentos, taxas
    )
    if vencimentos.is_empty():
        return pl.DataFrame(
            schema={
                "data_vencimento": pl.Date,
                "dias_uteis": pl.Int64,
                "taxa_tir": pl.Float64,
                "taxa_forward": pl.Float64,
                "taxa_zero": pl.Float64,
            }
        )

    titulos = pl.DataFrame({"data_vencimento": vencimentos, "taxa_tir": taxas}).sort(
        "data_vencimento"
    )
    vencimentos_ordenados = titulos["data_vencimento"].to_list()
    taxas_tir = titulos["taxa_tir"].to_list()
    ultimo_vencimento = vencimentos_ordenados[-1]
    vertices = _gerar_vertices_mensais(liquidacao, ultimo_vencimento)
    dias_uteis = du.contar(liquidacao, pl.Series(vertices)).to_list()
    indice_por_data = {data: indice for indice, data in enumerate(vertices)}
    contexto = _ContextoBootstrapForwards(
        liquidacao,
        vertices,
        dias_uteis,
        indice_por_data,
        vencimentos_ordenados,
        taxas_tir,
        taxas_tir.copy(),
    )

    for indice_titulo in range(len(vencimentos_ordenados)):
        contexto.taxas_forward[indice_titulo] = _calibrar_taxa_forward(
            contexto, indice_titulo
        )

    forwards_vertices = _taxas_forward_vertices(
        vertices, vencimentos_ordenados, contexto.taxas_forward
    )
    curva_zero = _taxas_zero_por_forwards(dias_uteis, forwards_vertices)
    titulos = titulos.with_columns(
        taxa_forward=pl.Series(contexto.taxas_forward, dtype=pl.Float64)
    )
    df = pl.DataFrame(
        {
            "data_vencimento": vertices,
            "dias_uteis": dias_uteis,
            "taxa_zero": curva_zero,
        }
    )

    df = (
        df.filter(pl.col("data_vencimento").is_in(vencimentos_ordenados))
        .join(titulos, on="data_vencimento", how="left")
        .select(
            "data_vencimento",
            "dias_uteis",
            "taxa_tir",
            "taxa_forward",
            "taxa_zero",
        )
    )
    if percentual:
        df = df.with_columns(
            taxa_tir=pl.col("taxa_tir") * 100,
            taxa_zero=pl.col("taxa_zero") * 100,
            taxa_forward=pl.col("taxa_forward") * 100,
        )

    return df
