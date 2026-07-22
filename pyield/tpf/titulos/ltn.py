import polars as pl

from pyield import du, fwd
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf.titulos import _utils as utils

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

    df = df.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
        duration=duration_expr("data_referencia", "data_vencimento"),
    ).with_columns(
        prazo_medio=pl.col("duration"),
        dv01=dv01_expr("data_referencia", "data_vencimento", "taxa_indicativa", "pu"),
    )
    df = utils.adicionar_taxa_di(df, data)

    df = df.with_columns(
        premio=pl.col("taxa_indicativa") - pl.col("taxa_di"),
        rentabilidade=rentabilidade_expr("taxa_indicativa", "taxa_di"),
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
    Calcula o PU da LTN pela metodologia da STN para leilões primários.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        taxa: Taxa de desconto (YTM) do título em formato decimal.

    Returns:
        float: PU da LTN conforme a metodologia da STN.

    References:
        - Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos
          Públicos Federais Ofertados nos Leilões Primários.
          https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

    Examples:
        >>> from pyield import ltn
        >>> ltn.pu("05-07-2024", "01-01-2030", 0.12145)
        535.279902
        >>> ltn.pu("21-05-2008", "01-07-2010", 0.143600009)
        753.315323
    """
    # Valida e normaliza entradas
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")
    taxa = utils.normalizar_taxa_precificacao(taxa)
    # Calcula dias úteis entre liquidação e vencimento
    dias_uteis = du.contar(data_liquidacao, data_vencimento)

    # Calcula anos úteis truncados conforme a STN
    anos_truncados = utils.truncar(dias_uteis / 252, 14)

    fator_desconto = (1 + taxa) ** anos_truncados

    # Trunca o preço em 6 casas conforme a STN
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
        float: Taxa implícita (YTM) em formato decimal, truncada em oito
            casas decimais (seis casas em termos percentuais). Retorna NaN
            em caso de erro.

    Examples:
        >>> from pyield import ltn
        >>> ltn.taxa("05-07-2024", "01-01-2030", 535.279902)
        0.12145
        >>> ltn.taxa("13-03-2026", "01-01-2027", 895.563913)
        0.148307
        >>> ltn.taxa("21-05-2008", "01-07-2010", 753.3)
        0.14361101
    """
    if any_is_empty(data_liquidacao, data_vencimento, preco_unitario):
        return float("nan")

    if preco_unitario <= 0:
        return float("nan")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    anos_truncados = utils.truncar(dias_uteis / 252, 14)
    taxa_calculada = (VALOR_FACE / preco_unitario) ** (1 / anos_truncados) - 1
    return utils.truncar(taxa_calculada, 8)


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


def rentabilidade_expr(
    taxa_ltn: pl.Expr | str,
    taxa_di: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para a rentabilidade da LTN sobre o DI.

    Args:
        taxa_ltn: Nome de coluna ou expressão Polars com a taxa anualizada da
            LTN.
        taxa_di: Nome de coluna ou expressão Polars com a taxa anualizada do DI
            Futuro.

    Returns:
        pl.Expr: Expressão sem alias com a rentabilidade da LTN sobre o DI.
    """
    expr_ltn = taxa_ltn if isinstance(taxa_ltn, pl.Expr) else pl.col(taxa_ltn)
    expr_di = taxa_di if isinstance(taxa_di, pl.Expr) else pl.col(taxa_di)
    taxa_diaria_ltn = (1 + expr_ltn) ** (1 / 252) - 1
    taxa_diaria_di = (1 + expr_di) ** (1 / 252) - 1
    return taxa_diaria_ltn / taxa_diaria_di


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
    pu: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da LTN em R$.

    Representa a variação do PU informado para um aumento de 1 bp (0,01%) na
    taxa.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        taxa: Taxa de desconto (YTM) do título.
        pu: PU usado como base para o cálculo.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ltn
        >>> pu = ltn.pu("26-03-2025", "01-01-2032", 0.150970)
        >>> ltn.dv01("26-03-2025", "01-01-2032", 0.150970, pu)
        0.2269059999999794
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa, pu):
        return float("nan")

    taxa = utils.normalizar_taxa_precificacao(taxa)
    taxa_mais_1bp = round(taxa + 0.0001, 8)
    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    anos_truncados = utils.truncar(dias_uteis / 252, 14)
    preco_1 = utils.truncar(VALOR_FACE / (1 + taxa) ** anos_truncados, 6)
    preco_2 = utils.truncar(
        VALOR_FACE / (1 + taxa_mais_1bp) ** anos_truncados, 6
    )
    return pu * (1 - preco_2 / preco_1)


def duration_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para a duration da LTN em anos úteis.

    Args:
        data_liquidacao: Nome de coluna ou expressão Polars com a data de
            liquidação.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento.

    Returns:
        pl.Expr: Expressão sem alias com a duration em anos úteis.
    """
    return du.contar_expr(data_liquidacao, data_vencimento) / 252


def dv01_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
    taxa: pl.Expr | str,
    pu: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para o DV01 da LTN.

    O cálculo é aplicado linha a linha e reprifica o PU informado para um
    aumento de 1 bp na taxa.

    Args:
        data_liquidacao: Nome de coluna ou expressão Polars com a data de
            liquidação.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento.
        taxa: Nome de coluna ou expressão Polars com a taxa em formato decimal.
        pu: Nome de coluna ou expressão Polars com o PU usado como base.

    Returns:
        pl.Expr: Expressão sem alias com o DV01.
    """
    return pl.struct(
        utils.coluna_ou_expr(data_liquidacao, "data_liquidacao"),
        utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        utils.coluna_ou_expr(taxa, "taxa"),
        utils.coluna_ou_expr(pu, "pu"),
    ).map_elements(
        lambda s: dv01(
            s["data_liquidacao"],
            s["data_vencimento"],
            s["taxa"],
            s["pu"],
        ),
        return_dtype=pl.Float64,
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
    return (
        dados(data)
        .select("data_vencimento", "dias_uteis", "taxa_indicativa")
        .with_columns(taxa_forward=fwd.forwards_expr("dias_uteis", "taxa_indicativa"))
        .sort("data_vencimento")
    )
