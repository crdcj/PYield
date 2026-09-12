"""Bootstrap de forwards para a curva zero de NTN-B."""

import datetime as dt
from collections.abc import Callable

import polars as pl

from pyield import du
from pyield._internal.types import ArrayLike, DateLike, DatesLike, any_is_empty
from pyield.tpf.titulos import _utils as utils

MAX_EXPANSOES_INTERVALO = 32


def _gerar_vertices_pagamentos(
    fluxos_titulos: list[pl.DataFrame],
) -> list[dt.date]:
    """Gera vértices nas datas de pagamento dos títulos."""
    datas = {data for fluxos in fluxos_titulos for data in fluxos["data_pagamento"]}
    return sorted(datas)


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


def _calibrar_taxa_forward(
    fluxos: list[tuple[int, float]],
    taxa_tir: float,
    descontos: dict[int, float],
    dias_anterior: int,
    desconto_anterior: float,
) -> float:
    """Calibra apenas os fluxos posteriores ao vencimento anterior."""
    cotacao_alvo = sum(valor / (1 + taxa_tir) ** (dias / 252) for dias, valor in fluxos)
    pv_fixo = sum(
        valor * descontos[dias] for dias, valor in fluxos if dias <= dias_anterior
    )
    trecho = [
        ((dias - dias_anterior) / 252, valor * desconto_anterior)
        for dias, valor in fluxos
        if dias > dias_anterior
    ]

    def erro(taxa_forward: float) -> float:
        return (
            pv_fixo
            + sum(valor / (1 + taxa_forward) ** prazo for prazo, valor in trecho)
            - cotacao_alvo
        )

    return _resolver_taxa_forward(erro, taxa_tir)


def _bootstrap(
    liquidacao: dt.date, titulos: pl.DataFrame, fluxos_titulos: list[pl.DataFrame]
) -> pl.DataFrame:
    """Acumula os descontos dos trechos calibrados e retorna a curva nos vencimentos."""
    vertices = _gerar_vertices_pagamentos(fluxos_titulos)
    dias_por_data = dict(
        zip(vertices, du.contar(liquidacao, pl.Series(vertices)), strict=True)
    )
    descontos = {0: 1.0}
    dias_anterior = 0
    desconto_anterior = 1.0
    taxas_forward = []
    taxas_zero = []
    indice_vertice = 0

    for vencimento, taxa_tir, fluxos in zip(
        titulos["data_vencimento"], titulos["taxa_tir"], fluxos_titulos, strict=True
    ):
        taxa_forward = _calibrar_taxa_forward(
            [(dias_por_data[data], valor) for data, valor in fluxos.iter_rows()],
            taxa_tir,
            descontos,
            dias_anterior,
            desconto_anterior,
        )
        # Guarda cada desconto uma única vez, após calibrar seu trecho.
        while indice_vertice < len(vertices) and vertices[indice_vertice] <= vencimento:
            dias = dias_por_data[vertices[indice_vertice]]
            descontos[dias] = desconto_anterior / (1 + taxa_forward) ** (
                (dias - dias_anterior) / 252
            )
            indice_vertice += 1

        dias_vencimento = dias_por_data[vencimento]
        desconto_anterior = descontos[dias_vencimento]
        taxas_forward.append(taxa_forward)
        taxas_zero.append(
            desconto_anterior ** (-252 / dias_vencimento) - 1
            if dias_vencimento
            else taxa_forward
        )
        dias_anterior = dias_vencimento

    return titulos.with_columns(
        dias_uteis=pl.Series(
            [dias_por_data[data] for data in titulos["data_vencimento"]], dtype=pl.Int64
        ),
        taxa_forward=pl.Series(taxas_forward, dtype=pl.Float64),
        taxa_zero=pl.Series(taxas_zero, dtype=pl.Float64),
    ).select("data_vencimento", "dias_uteis", "taxa_tir", "taxa_forward", "taxa_zero")


def taxas_zero(
    data_liquidacao: DateLike,
    vencimentos: DatesLike,
    taxas: ArrayLike,
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

        A curva usa como vértices as datas de pagamento dos títulos informados.
        Para cada NTN-B, a taxa forward calibrada é mantida constante desde o
        vencimento anterior até o seu vencimento. A primeira taxa forward começa
        na TIR do título mais curto.

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

    Returns:
        pl.DataFrame: Curva zero calibrada pelo bootstrap de forwards. Retorna vazio quando
            não restarem vencimentos posteriores à liquidação.

    Raises:
        RuntimeError: Se não for possível encontrar um intervalo válido para
            alguma taxa forward.

    Output Columns:
        - data_vencimento (Date): Data do vértice da curva.
        - dias_uteis (Int64): Dias úteis entre liquidação e vértice.
        - taxa_tir (Float64): TIR recebida na entrada, em formato decimal.
        - taxa_forward (Float64): Parâmetro calibrado por bisseção para o trecho
            que termina no vencimento, em formato decimal.
        - taxa_zero (Float64): Taxa zero real anualizada, em formato decimal.

    Examples:
        >>> from pyield import ntnb
        >>> import polars.selectors as cs
        >>> # Taxas indicativas da ANBIMA na data de referência.
        >>> df = ntnb.dados("16-08-2024")
        >>> curva = ntnb.taxas_zero(
        ...     data_liquidacao="16-08-2024",
        ...     vencimentos=df["data_vencimento"],
        ...     taxas=df["taxa_indicativa"],
        ... )
        >>> # Taxas em percentual.
        >>> curva.with_columns(cs.starts_with("taxa_") * 100)
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
        >>> curva = ntnb.taxas_zero(
        ...     data_liquidacao="18-05-2026",
        ...     vencimentos=df["data_vencimento"],
        ...     taxas=df["taxa_indicativa"],
        ... )
        >>> curva_percentual = curva.with_columns(cs.starts_with("taxa_") * 100)
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
        fluxos_caixa,
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
    fluxos_titulos = [
        fluxos_caixa(liquidacao, vencimento) for vencimento in vencimentos_ordenados
    ]
    return _bootstrap(liquidacao, titulos, fluxos_titulos)
