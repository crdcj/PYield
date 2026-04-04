import polars as pl

import pyield._internal.converters as cv
from pyield import dus, fwd
from pyield._internal.types import DateLike, any_is_empty
from pyield.tn import utils

VALOR_FACE = 1000


def dados(data: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de LTN na ANBIMA para a data de referência.

    Args:
        data: Data da consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de LTN.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "LTN").
        - codigo_selic (Int64): Código do título no SELIC.
        - data_base (Date): Data base de emissão do título.
        - data_vencimento (Date): Data de vencimento do título.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - duration (Float64): Macaulay Duration do título (anos).
        - prazo_medio (Float64): Prazo médio do título (anos).
        - dv01 (Float64): Variação no preço para 1bp de taxa.
        - dv01_usd (Float64): DV01 convertido para USD pela PTAX do dia.
        - pu (Float64): Preço unitário (PU).
        - taxa_compra (Float64): Taxa de compra (decimal).
        - taxa_venda (Float64): Taxa de venda (decimal).
        - taxa_indicativa (Float64): Taxa indicativa (decimal).
        - taxa_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo
            método flat forward.
        - premio (Float64): prêmio sobre o DI, isto é, o spread sobre a
            taxa DI.
        - rentabilidade (Float64): Rentabilidade diária da LTN sobre o DI.

    Examples:
        >>> from pyield import ltn
        >>> df_ltn = ltn.dados("23-08-2024")  # doctest: +SKIP
    """
    df = utils.obter_tpf(data, "LTN")
    if df.is_empty():
        return df

    data_ref = cv.converter_datas(data)

    df = df.with_columns(
        dias_uteis=dus.contar_expr("data_referencia", "data_vencimento"),
    )

    df = df.with_columns(
        duration=pl.col("dias_uteis") / 252,
    ).with_columns(prazo_medio=pl.col("duration"))
    df = utils.adicionar_dv01(df, data_ref)
    df = utils.adicionar_taxa_di(df, data_ref)

    df = df.with_columns(
        premio=pl.col("taxa_indicativa") - pl.col("taxa_di"),
        rentabilidade=pl.struct("taxa_indicativa", "taxa_di").map_elements(
            lambda s: rentabilidade(s["taxa_indicativa"], s["taxa_di"]),
            return_dtype=pl.Float64,
        ),
    )

    return df.select(
        "data_referencia",
        "titulo",
        "codigo_selic",
        "data_base",
        "data_vencimento",
        "dias_uteis",
        "duration",
        "prazo_medio",
        "dv01",
        "dv01_usd",
        "pu",
        "taxa_compra",
        "taxa_venda",
        "taxa_indicativa",
        "taxa_di",
        "premio",
        "rentabilidade",
    )


def vencimentos(data: DateLike) -> pl.Series:
    """
    Busca os vencimentos disponíveis para a data de referência.

    Args:
        data: Data da consulta.

    Returns:
        pl.Series: Série de datas de vencimento disponíveis.

    Examples:
        >>> from pyield import ltn
        >>> ltn.vencimentos("22-08-2024")
        shape: (13,)
        Series: 'data_vencimento' [date]
        [
            2024-10-01
            2025-01-01
            2025-04-01
            2025-07-01
            2025-10-01
            …
            2026-10-01
            2027-07-01
            2028-01-01
            2028-07-01
            2030-01-01
        ]
    """
    return dados(data)["data_vencimento"]


def pu(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    """
    Calcula o PU (preço unitário) da LTN pelas regras da ANBIMA.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        taxa: Taxa de desconto (YTM) do título.

    Returns:
        float: PU da LTN conforme ANBIMA.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ltn
        >>> ltn.pu("05-07-2024", "01-01-2030", 0.12145)
        535.279902
    """
    # Valida e normaliza entradas
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")
    # Calcula dias úteis entre liquidação e vencimento
    dias_uteis = dus.contar(data_liquidacao, data_vencimento)

    # Calcula anos úteis truncados conforme ANBIMA
    anos_truncados = utils.truncar(dias_uteis / 252, 14)

    fator_desconto = (1 + taxa) ** anos_truncados

    # Trunca o preço em 6 casas conforme ANBIMA
    return utils.truncar(VALOR_FACE / fator_desconto, 6)


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    preco_unitario: float,
) -> float:
    """
    Calcula a taxa implícita (YTM) de uma LTN a partir do preço (PU).

    Inverte algebricamente a fórmula de ``pu()``:
    ``taxa = (1000 / pu) ^ (252 / du) - 1``

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        preco_unitario: PU do título.

    Returns:
        float: Taxa implícita (YTM) em formato decimal. Retorna NaN em
            caso de erro.

    Examples:
        >>> from pyield import ltn
        >>> ltn.taxa("05-07-2024", "01-01-2030", 535.279902)
        0.12145
        >>> ltn.taxa("13-03-2026", "01-01-2027", 895.563913)
        0.148307
    """
    if any_is_empty(data_liquidacao, data_vencimento, preco_unitario):
        return float("nan")

    if preco_unitario <= 0:
        return float("nan")

    dias_uteis = dus.contar(data_liquidacao, data_vencimento)
    anos_truncados = utils.truncar(dias_uteis / 252, 14)
    taxa_calculada = (VALOR_FACE / preco_unitario) ** (1 / anos_truncados) - 1
    return round(taxa_calculada, 6)


def rentabilidade(taxa_ltn: float, taxa_di: float) -> float:
    """
    Calcula a rentabilidade da LTN sobre a taxa de DI Futuro.

    Args:
        taxa_ltn: Taxa anualizada da LTN.
        taxa_di: Taxa anualizada do DI Futuro.

    Returns:
        float: Rentabilidade da LTN sobre o DI.

    Examples:
        Reference date: 22-08-2024
        LTN rate for 01-01-2030: 0.118746
        DI (JAN30) Settlement rate: 0.11725
        >>> from pyield import ltn
        >>> ltn.rentabilidade(0.118746, 0.11725)
        1.0120718007994287
    """
    if any_is_empty(taxa_ltn, taxa_di):
        return float("nan")
    # Cálculo das taxas diárias
    taxa_diaria_ltn = (1 + taxa_ltn) ** (1 / 252) - 1
    taxa_diaria_di = (1 + taxa_di) ** (1 / 252) - 1

    # Retorno do cálculo da rentabilidade
    return taxa_diaria_ltn / taxa_diaria_di


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da LTN em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        taxa: Taxa de desconto (YTM) do título.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ltn
        >>> ltn.dv01("26-03-2025", "01-01-2032", 0.150970)
        0.2269059999999854
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    preco_1 = pu(data_liquidacao, data_vencimento, taxa)
    preco_2 = pu(data_liquidacao, data_vencimento, taxa + 0.0001)
    return preco_1 - preco_2


def premio(
    data: DateLike,
    pontos_base: bool = False,
) -> pl.DataFrame:
    """
    Calcula o prêmio (spread) da LTN sobre o DI na data de referência.

    Definição do prêmio (forma bruta):
        premio = taxa_indicativa - taxa de ajuste do DI

    Quando ``pontos_base=False`` a coluna retorna essa diferença em formato decimal
    (ex: 0.000439 ≈ 4.39 bps). Quando ``pontos_base=True`` o valor é automaticamente
    multiplicado por 10_000 e exibido diretamente em basis points.

    Args:
        data: Data da consulta para buscar as taxas.
        pontos_base: Se True, retorna o prêmio já convertido em basis points.
            Padrão False.

    Returns:
        pl.DataFrame: DataFrame com as colunas do prêmio.

    Output Columns:
        - titulo (String): Tipo do título.
        - data_vencimento (Date): Data de vencimento.
        - premio (Float64): prêmio em decimal ou bps conforme parâmetro,
            isto é, o spread sobre o DI.

    Raises:
        ValueError: Se os dados de DI não possuem 'taxa_ajuste' ou estão vazios.

    Examples:
        >>> from pyield import ltn
        >>> ltn.premio("30-05-2025", pontos_base=True)
        shape: (13, 3)
        ┌────────┬─────────────────┬────────┐
        │ titulo ┆ data_vencimento ┆ premio │
        │ ---    ┆ ---             ┆ ---    │
        │ str    ┆ date            ┆ f64    │
        ╞════════╪═════════════════╪════════╡
        │ LTN    ┆ 2025-07-01      ┆ 4.39   │
        │ LTN    ┆ 2025-10-01      ┆ -9.0   │
        │ LTN    ┆ 2026-01-01      ┆ -4.88  │
        │ LTN    ┆ 2026-04-01      ┆ -4.45  │
        │ LTN    ┆ 2026-07-01      ┆ 0.81   │
        │ …      ┆ …               ┆ …      │
        │ LTN    ┆ 2028-01-01      ┆ 0.55   │
        │ LTN    ┆ 2028-07-01      ┆ 1.5    │
        │ LTN    ┆ 2029-01-01      ┆ 10.77  │
        │ LTN    ┆ 2030-01-01      ┆ 11.0   │
        │ LTN    ┆ 2032-01-01      ┆ 11.24  │
        └────────┴─────────────────┴────────┘
    """
    return utils.premio_pre(data, pontos_base=pontos_base).filter(
        pl.col("titulo") == "LTN"
    )


def taxas_forward(data: DateLike) -> pl.DataFrame:
    """Calcula as taxas forward da LTN para uma data de referência.

    As taxas indicativas da LTN já são spot (zero-coupon) por construção, pois o
    título não paga cupons. Portanto o cálculo de forward é direto usando a
    estrutura de vencimentos e suas taxas.

    Args:
        data: Data das taxas indicativas.

    Returns:
        pl.DataFrame: DataFrame com as taxas forward.

    Output Columns:
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - taxa_indicativa (Float64): Taxa spot (zero cupom).
        - taxa_forward (Float64): Taxa forward.

    Examples:
        >>> from pyield import ltn
        >>> ltn.taxas_forward("17-10-2025")
        shape: (13, 4)
        ┌─────────────────┬────────────┬─────────────────┬──────────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_indicativa ┆ taxa_forward │
        │ ---             ┆ ---        ┆ ---             ┆ ---          │
        │ date            ┆ i64        ┆ f64             ┆ f64          │
        ╞═════════════════╪════════════╪═════════════════╪══════════════╡
        │ 2026-01-01      ┆ 52         ┆ 0.148307        ┆ 0.148307     │
        │ 2026-04-01      ┆ 113        ┆ 0.147173        ┆ 0.146207     │
        │ 2026-07-01      ┆ 174        ┆ 0.145206        ┆ 0.141571     │
        │ 2026-10-01      ┆ 239        ┆ 0.142424        ┆ 0.13501      │
        │ 2027-04-01      ┆ 361        ┆ 0.138155        ┆ 0.129838     │
        │ …               ┆ …          ┆ …               ┆ …            │
        │ 2028-07-01      ┆ 676        ┆ 0.133411        ┆ 0.131654     │
        │ 2029-01-01      ┆ 800        ┆ 0.134254        ┆ 0.138861     │
        │ 2029-07-01      ┆ 924        ┆ 0.135264        ┆ 0.141802     │
        │ 2030-01-01      ┆ 1049       ┆ 0.135967        ┆ 0.141177     │
        │ 2032-01-01      ┆ 1553       ┆ 0.13883         ┆ 0.144812     │
        └─────────────────┴────────────┴─────────────────┴──────────────┘
    """
    if any_is_empty(data):
        return pl.DataFrame()
    df = dados(data).select(
        "data_vencimento",
        "dias_uteis",
        "taxa_indicativa",
    )
    taxas_forward = fwd.forwards(
        dias_uteis=df["dias_uteis"], taxas=df["taxa_indicativa"]
    )
    return df.with_columns(taxa_forward=taxas_forward).sort("data_vencimento")
