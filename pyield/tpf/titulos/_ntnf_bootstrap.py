"""Bootstrap experimental da curva prefixada conjunta de LTN e NTN-F."""

import datetime as dt
import math

import polars as pl

from pyield import du
from pyield._internal import converters as cv
from pyield._internal.types import ArrayLike, DateLike, DatesLike
from pyield.interpolador import Interpolador
from pyield.tpf.titulos import _utils as utils

TOLERANCIA_PRECO = 1e-12


def _preparar_titulos(
    liquidacao: dt.date, vencimentos: DatesLike, taxas: ArrayLike, titulo: str
) -> pl.DataFrame:
    datas = cv.converter_datas(vencimentos)
    valores = pl.Series(taxas, dtype=pl.Float64)
    if len(datas) != len(valores):
        raise ValueError(f"Vencimentos e taxas de {titulo} devem ter o mesmo tamanho.")
    df = pl.DataFrame({"data_vencimento": datas, "taxa": valores})
    if (
        df.null_count().sum_horizontal().item()
        or df.filter(~pl.col("taxa").is_finite() | (pl.col("taxa") <= -1)).height
    ):
        raise ValueError(
            "Datas e taxas devem ser válidas, com taxas finitas acima de -1."
        )
    df = df.filter(pl.col("data_vencimento") > liquidacao)
    if df["data_vencimento"].n_unique() != df.height:
        raise ValueError(f"Vencimentos duplicados de {titulo}.")
    return df.with_columns(titulo=pl.lit(titulo))


def _calibrar_zero(  # noqa: PLR0913, PLR0917
    liquidacao: dt.date,
    vencimento: dt.date,
    tir: float,
    dias: list[int],
    taxas: list[float],
    prazo: int,
) -> float:
    from pyield.tpf.titulos.ntnf import fluxos_caixa  # noqa: PLC0415

    fluxos = fluxos_caixa(liquidacao, vencimento)
    prazos = du.contar(liquidacao, fluxos["data_pagamento"]).to_list()
    valores = fluxos["valor_pagamento"].to_list()
    alvo = sum(cf / (1 + tir) ** (t / 252) for cf, t in zip(valores, prazos))
    anterior = dias[-1] if dias else 0
    desconto = (1 + taxas[-1]) ** (-anterior / 252) if dias else 1.0
    interpolar = Interpolador(dias, taxas, "flat_forward") if dias else None
    conhecido = sum(
        cf / (1 + interpolar(t)) ** (t / 252)
        for cf, t in zip(valores, prazos)
        if t <= anterior and interpolar is not None
    )
    residual = alvo - conhecido
    if residual <= 0:
        raise ValueError(
            f"Preço de NTN-F {vencimento} incompatível com os descontos já fixados."
        )
    futuros = [
        (cf * desconto, (t - anterior) / (prazo - anterior))
        for cf, t in zip(valores, prazos)
        if t > anterior
    ]

    # q é o logaritmo do fator acumulado no trecho: q = log(1 + f) * DU / 252.
    # Os limites abaixo enquadram a raiz para fluxos positivos, inclusive f < 0.
    log_razao = math.log(sum(cf for cf, _ in futuros) / residual)
    limites = [log_razao / fracao for _, fracao in futuros]

    def erro(q: float) -> float:
        return sum(cf * math.exp(-q * fracao) for cf, fracao in futuros) - residual

    inferior, superior = min(limites), max(limites)
    if inferior == superior:
        q = inferior
    elif abs(erro(inferior)) < TOLERANCIA_PRECO:
        q = inferior
    elif abs(erro(superior)) < TOLERANCIA_PRECO:
        q = superior
    else:
        q = utils._metodo_bissecao(erro, inferior, superior)
    if not math.isfinite(q):
        raise RuntimeError(f"Não foi possível calibrar a NTN-F {vencimento}.")
    return math.expm1((q - math.log(desconto)) * 252 / prazo)


def taxas_zero_forwards(  # noqa: PLR0913, PLR0917
    data_liquidacao: DateLike,
    vencimentos_ltn: DatesLike,
    taxas_ltn: ArrayLike,
    vencimentos_ntnf: DatesLike,
    taxas_ntnf: ArrayLike,
    incluir_cupons: bool = False,
) -> pl.DataFrame:
    """Calcula uma curva zero experimental conjunta de LTN e NTN-F.

    Usa forwards constantes entre vencimentos, em base 252 dias úteis.
    Em vencimentos coincidentes, a LTN tem prioridade. Nos demais, a NTN-F
    determina o forward desde o vértice anterior, reproduzindo seu preço teórico.
    As taxas podem vir da ANBIMA ou ser fornecidas pelo usuário; não há consulta
    à rede. Este método é uma alternativa experimental a ``taxas_zero``.

    Args:
        data_liquidacao: Data de liquidação.
        vencimentos_ltn: Vencimentos das LTNs; aceita coleção vazia.
        taxas_ltn: Taxas zero das LTNs em formato decimal.
        vencimentos_ntnf: Vencimentos das NTN-F; aceita coleção vazia.
        taxas_ntnf: TIRs das NTN-F em formato decimal.
        incluir_cupons: Inclui também as datas dos cupons das NTN-F selecionadas.

    Returns:
        pl.DataFrame: União dos vencimentos futuros de LTN e NTN-F, ordenada.
            Retorna vazio com esquema quando não há títulos futuros.

    Output Columns:
        - data_vencimento (Date): Vértice da curva prefixada conjunta.
        - dias_uteis (Int64): Dias úteis desde a liquidação.
        - taxa_zero (Float64): Taxa zero nominal anual em formato decimal.

    Raises:
        ValueError: Entradas inválidas, vencimentos duplicados na mesma família,
            prazos úteis coincidentes ou preço incompatível com a curva anterior.
        RuntimeError: Falha na calibração numérica.

    Notes:
        A prioridade da LTN vale apenas no mesmo vencimento, não em todo o
        intervalo até a última LTN. NTN-F entre duas LTNs participa da calibração.
        Antes do primeiro vértice, a taxa zero é constante. Use todos os vértices
        retornados com ``Interpolador(..., metodo="flat_forward")`` para preservar
        os descontos dos cupons. O retorno inclui LTNs mesmo sem NTN-F nessa data,
        diferentemente do método antigo.

        A calibração usa os fluxos de ``ntnf.fluxos_caixa``, DU/252 sem truncamento
        e valores presentes sem arredondamento, como no bootstrap de NTN-B.
        Portanto, o preço-alvo pode diferir ligeiramente de ``ntnf.pu``, que aplica
        arredondamentos e truncamentos. As datas contratuais seguem a convenção
        de ``ntnf.fluxos_caixa``. Vencimentos até a liquidação são ignorados.
    """
    from pyield.tpf.titulos.ntnf import datas_pagamento  # noqa: PLC0415

    liquidacao = cv.converter_datas(data_liquidacao)
    ltn = _preparar_titulos(liquidacao, vencimentos_ltn, taxas_ltn, "LTN")
    ntnf = _preparar_titulos(liquidacao, vencimentos_ntnf, taxas_ntnf, "NTN-F")
    titulos = (
        pl.concat([ltn, ntnf])
        .unique("data_vencimento", keep="first", maintain_order=True)
        .sort("data_vencimento")
    )
    esquema = {
        "data_vencimento": pl.Date,
        "dias_uteis": pl.Int64,
        "taxa_zero": pl.Float64,
    }
    if titulos.is_empty():
        return pl.DataFrame(schema=esquema)

    datas = titulos["data_vencimento"].to_list()
    prazos = du.contar(liquidacao, titulos["data_vencimento"]).to_list()
    if prazos[0] <= 0 or any(b <= a for a, b in zip(prazos, prazos[1:])):
        raise ValueError("Vértices devem ter prazos úteis positivos e distintos.")
    dias: list[int] = []
    taxas: list[float] = []
    cupons: set[dt.date] = set()
    for (vencimento, taxa, titulo), prazo in zip(titulos.iter_rows(), prazos):
        zero = (
            taxa
            if titulo == "LTN"
            else _calibrar_zero(liquidacao, vencimento, taxa, dias, taxas, prazo)
        )
        dias.append(prazo)
        taxas.append(zero)
        if incluir_cupons and titulo == "NTN-F":
            cupons.update(datas_pagamento(liquidacao, vencimento).to_list())
    curva = pl.DataFrame(dict(zip(esquema, [datas, dias, taxas])), schema=esquema)
    if not cupons:
        return curva
    datas_saida = pl.Series(sorted(set(datas) | cupons), dtype=pl.Date)
    dias_saida = du.contar(liquidacao, datas_saida)
    interpolar = Interpolador(dias, taxas, "flat_forward")
    return pl.DataFrame(
        {
            "data_vencimento": datas_saida,
            "dias_uteis": dias_saida,
            "taxa_zero": [interpolar(t) for t in dias_saida],
        },
        schema=esquema,
    )
