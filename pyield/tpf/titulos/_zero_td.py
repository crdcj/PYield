"""Bootstrap da curva zero de NTN-B pelo método TD."""

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass

import polars as pl

from pyield import du
from pyield._internal.types import ArrayLike, DateLike, DatesLike, any_is_empty
from pyield.tpf.titulos import _utils as utils

DIA_VENCIMENTO = 15


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
        while erro_superior > 0:
            limite_superior = 2 * limite_superior + 1
            erro_superior = erro(limite_superior)
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
    incluir_vertices: bool = False,
) -> pl.DataFrame:
    r"""
    Calcula a curva zero de NTN-B pelo bootstrap de forwards do método TD.

    O método parte das TIRs observadas das NTN-B, mas não as trata como taxas
    zero. Ele encontra uma taxa forward para cada vencimento de título para que
    os fluxos descontados pela curva zero reproduzam exatamente a cotação que a
    TIR daquele título produz.

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
        apenas uma incógnita e pode ser resolvida de forma estável por bisseção.

        **Precisão do método**

        A calibração usa \(DU / 252\) sem truncamento e soma os valores
        presentes sem arredondamento. Isso difere de :func:`cotacao`, que
        aplica as regras ANBIMA de arredondamento dos fluxos e truncamento da
        cotação.

    Args:
        data_liquidacao: Data de liquidação.
        vencimentos: Datas de vencimento das NTN-B.
        taxas: TIRs correspondentes.
        incluir_vertices: Se True, inclui todos os vértices mensais da curva.
            Padrão False, retornando apenas os vencimentos informados.

    Returns:
        pl.DataFrame: Curva zero calibrada pelo método TD.

    Output Columns:
        - data_vencimento (Date): Data do vértice da curva.
        - dias_uteis (Int64): Dias úteis entre liquidação e vértice.
        - taxa_zero (Float64): Taxa zero real anualizada.
        - taxa_forward (Float64): Taxa forward anualizada do trecho.
    """
    from pyield.tpf.titulos.ntnb import (  # noqa: PLC0415
        _validar_entradas_taxas_zero,
    )

    if any_is_empty(data_liquidacao, vencimentos, taxas):
        return pl.DataFrame()

    liquidacao, vencimentos, taxas = _validar_entradas_taxas_zero(
        data_liquidacao, vencimentos, taxas
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
    df = pl.DataFrame(
        {
            "data_vencimento": vertices,
            "dias_uteis": dias_uteis,
            "taxa_zero": curva_zero,
            "taxa_forward": forwards_vertices,
        }
    )

    if not incluir_vertices:
        df = df.filter(pl.col("data_vencimento").is_in(vencimentos_ordenados))
    return df
