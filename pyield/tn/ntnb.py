import datetime as dt
import logging

import polars as pl
from dateutil.relativedelta import relativedelta

import pyield._internal.converters as conversores
import pyield.interpolator as interpolador
import pyield.tn.tools as ferramentas
from pyield import anbima, bday, fwd
from pyield._internal.types import ArrayLike, DateLike, any_is_empty

"""
Constantes calculadas conforme regras da ANBIMA e em base 100.
TAXA_CUPOM = (0.06 + 1) ** 0.5 - 1  # 6% a.a. com capitalização semestral
VALOR_CUPOM = round(100 * TAXA_CUPOM, 6) -> 2.956301
VALOR_FINAL = principal + último cupom = 100 + 2.956301
DIA_CUPOM = 15
MESES_CUPOM = {2, 5, 8, 11}
"""
VALOR_CUPOM = 2.956301
VALOR_FINAL = 102.956301

logger = logging.getLogger(__name__)


def data(date: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de NTN-B para a data de referência.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de NTN-B.

    Output Columns:
        * BondType (String): Tipo do título (ex.: "NTN-B").
        * ReferenceDate (Date): Data de referência dos dados.
        * SelicCode (Int64): Código do título no SELIC.
        * IssueBaseDate (Date): Data base/emissão do título.
        * MaturityDate (Date): Data de vencimento do título.
        * BDToMat (Int64): Dias úteis entre referência e vencimento.
        * Duration (Float64): Macaulay Duration do título (anos).
        * DV01 (Float64): Variação no preço para 1bp de taxa.
        * DV01USD (Float64): DV01 convertido para USD pela PTAX do dia.
        * Price (Float64): Preço unitário (PU).
        * BidRate (Float64): Taxa de compra (decimal).
        * AskRate (Float64): Taxa de venda (decimal).
        * IndicativeRate (Float64): Taxa indicativa (decimal).
        * DIRate (Float64): Taxa DI interpolada (flat forward).

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.data("23-08-2024")
        shape: (14, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬──────────┬──────────┬────────────────┬──────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate  ┆ AskRate  ┆ IndicativeRate ┆ DIRate   │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---      ┆ ---      ┆ ---            ┆ ---      │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64      ┆ f64      ┆ f64            ┆ f64      │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪══════════╪══════════╪════════════════╪══════════╡
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.063961 ┆ 0.063667 ┆ 0.063804       ┆ 0.112749 │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.06594  ┆ 0.065635 ┆ 0.065795       ┆ 0.114963 │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.063925 ┆ 0.063601 ┆ 0.063794       ┆ 0.114888 │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.063217 ┆ 0.062905 ┆ 0.063094       ┆ 0.115595 │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.062245 ┆ 0.061954 ┆ 0.0621         ┆ 0.115665 │
        │ …             ┆ …        ┆ …         ┆ …             ┆ … ┆ …        ┆ …        ┆ …              ┆ …        │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.060005 ┆ 0.059574 ┆ 0.059797       ┆ 0.11511  │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.061107 ┆ 0.060733 ┆ 0.060923       ┆ 0.11511  │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.061304 ┆ 0.060931 ┆ 0.06114        ┆ 0.11511  │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.061053 ┆ 0.06074  ┆ 0.060892       ┆ 0.11511  │
        │ 2024-08-23    ┆ NTN-B    ┆ 760199    ┆ 2000-07-15    ┆ … ┆ 0.061211 ┆ 0.0608   ┆ 0.061005       ┆ 0.11511  │
        └───────────────┴──────────┴───────────┴───────────────┴───┴──────────┴──────────┴────────────────┴──────────┘
    """  # noqa: E501
    return anbima.tpf_data(date, "NTN-B")


def maturities(date: DateLike) -> pl.Series:
    """
    Busca os vencimentos de NTN-B disponíveis para a data de referência.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.Series: Série de datas de vencimento de NTN-B.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.maturities("16-08-2024")
        shape: (14,)
        Series: 'MaturityDate' [date]
        [
            2025-05-15
            2026-08-15
            2027-05-15
            2028-08-15
            2029-05-15
            …
            2040-08-15
            2045-05-15
            2050-08-15
            2055-05-15
            2060-08-15
        ]
    """
    return data(date)["MaturityDate"]


def _gerar_todas_datas_cupom(
    data_inicio: dt.date,
    data_fim: dt.date,
) -> pl.Series:
    """
    Gera todas as datas possíveis de cupom entre início e fim (inclusivas).

    Os cupons são pagos em 15/02, 15/05, 15/08 e 15/11.

    Args:
        data_inicio (DateLike): Data inicial.
        data_fim (DateLike): Data final.

    Returns:
        pl.Series: Série de datas de cupom no intervalo.
    """
    primeira_data_cupom = dt.date(data_inicio.year, 2, 1)

    # Gera datas no 1º dia do mês
    datas_cupom: pl.Series = pl.date_range(
        start=primeira_data_cupom, end=data_fim, interval="3mo", eager=True
    )
    # Ajusta para o dia 15
    datas_cupom = datas_cupom.dt.offset_by("14d")

    # Primeira data precisa ser após a data inicial
    return datas_cupom.filter(datas_cupom > data_inicio)


def payment_dates(
    settlement: DateLike,
    maturity: DateLike,
) -> pl.Series:
    """
    Gera todas as datas de cupom entre liquidação e vencimento (inclusivas).

    Os cupons são pagos em 15/02, 15/05, 15/08 e 15/11. A NTN-B é definida
    pela data de vencimento.

    Args:
        settlement (DateLike): Data de liquidação (exclusiva).
        maturity (DateLike): Data de vencimento.

    Returns:
        pl.Series: Série de datas de cupom no intervalo. Retorna série vazia se
            vencimento for menor ou igual à liquidação.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.payment_dates("10-05-2024", "15-05-2025")
        shape: (3,)
        Series: 'payment_dates' [date]
        [
            2024-05-15
            2024-11-15
            2025-05-15
        ]
    """
    if any_is_empty(settlement, maturity):
        return pl.Series(dtype=pl.Date)

    liquidacao = conversores.converter_datas(settlement)
    vencimento = conversores.converter_datas(maturity)

    if vencimento <= liquidacao:
        return pl.Series(dtype=pl.Date)

    data_cupom = vencimento
    datas_cupons = []

    while data_cupom > liquidacao:
        datas_cupons.append(data_cupom)
        data_cupom -= relativedelta(months=6)

    return pl.Series(name="payment_dates", values=datas_cupons).sort()


def cash_flows(
    settlement: DateLike,
    maturity: DateLike,
) -> pl.DataFrame:
    """
    Gera os fluxos de caixa da NTN-B entre liquidação e vencimento.

    Args:
        settlement (DateLike): Data de liquidação (exclusiva).
        maturity (DateLike): Data de vencimento.

    Returns:
        pl.DataFrame: DataFrame com as colunas de fluxo.

    Output Columns:
        * PaymentDate (Date): Data de pagamento do fluxo.
        * CashFlow (Float64): Valor do fluxo de caixa.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.cash_flows("10-05-2024", "15-05-2025")
        shape: (3, 2)
        ┌─────────────┬────────────┐
        │ PaymentDate ┆ CashFlow   │
        │ ---         ┆ ---        │
        │ date        ┆ f64        │
        ╞═════════════╪════════════╡
        │ 2024-05-15  ┆ 2.956301   │
        │ 2024-11-15  ┆ 2.956301   │
        │ 2025-05-15  ┆ 102.956301 │
        └─────────────┴────────────┘
    """
    if any_is_empty(settlement, maturity):
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    # Obtém as datas de cupom entre liquidação e vencimento
    liquidacao = conversores.converter_datas(settlement)
    vencimento = conversores.converter_datas(maturity)
    datas_pagamento = payment_dates(liquidacao, vencimento)

    # Retorna DataFrame vazio se não houver pagamentos (liquidação >= vencimento)
    if datas_pagamento.is_empty():
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    df = pl.DataFrame(
        {"PaymentDate": datas_pagamento},
    ).with_columns(
        pl.when(pl.col("PaymentDate") == vencimento)
        .then(VALOR_FINAL)
        .otherwise(VALOR_CUPOM)
        .alias("CashFlow")
    )

    return df


def quotation(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula a cotação da NTN-B em base 100 pelas regras da ANBIMA.

    Args:
        settlement (DateLike): Data de liquidação da operação.
        maturity (DateLike): Data de vencimento da NTN-B.
        rate (float): Taxa de desconto (YTM) usada no valor presente.

    Returns:
        float: Cotação da NTN-B truncada em 4 casas. Retorna NaN em erro.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - O cupom semestral é 2,956301, equivalente a 6% a.a. com capitalização
          semestral e arredondamento para 6 casas, conforme ANBIMA.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)
        99.3651
        >>> ntnb.quotation("31-05-2024", "15-08-2060", 0.061878)
        99.5341
        >>> ntnb.quotation("15-08-2024", "15-08-2032", 0.05929)
        100.6409
    """
    if any_is_empty(settlement, maturity, rate):
        return float("nan")

    df = cash_flows(settlement, maturity)
    if df.is_empty():
        return float("nan")

    datas_fluxo = df["PaymentDate"]
    valores_fluxo = df["CashFlow"]

    # Calcula dias úteis entre liquidação e datas de fluxo
    dias_uteis = bday.count(settlement, datas_fluxo)

    # Calcula anos úteis truncados conforme ANBIMA
    anos_uteis = ferramentas.truncate(dias_uteis / 252, 14)

    fator_desconto = (1 + rate) ** anos_uteis

    # Calcula o valor presente de cada fluxo (DCF) com arredondamento ANBIMA
    valor_presente_fluxos = (valores_fluxo / fator_desconto).round(10)

    # Retorna a cotação (soma do DCF) com truncamento ANBIMA
    return ferramentas.truncate(valor_presente_fluxos.sum(), 4)


def price(
    vna: float,
    quotation: float,
) -> float:
    """
    Calcula o preço da NTN-B pelas regras da ANBIMA.

    Args:
        vna (float): Valor nominal atualizado (VNA).
        quotation (float): Cotação da NTN-B em base 100.

    Returns:
        float: Preço da NTN-B truncado em 6 casas decimais.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.price(4299.160173, 99.3651)
        4271.864805
        >>> ntnb.price(4315.498383, 100.6409)
        4343.156412
        >>> ntnb.price(None, 99.5341)  # Entradas nulas retornam float('nan')
        nan
    """
    if any_is_empty(vna, quotation):
        return float("nan")
    return ferramentas.truncate(vna * quotation / 100, 6)


def _validar_entradas_taxa_spot(
    settlement: DateLike,
    vencimentos: ArrayLike,
    taxas: ArrayLike,
) -> tuple[dt.date, pl.Series, pl.Series]:
    # Processa e valida os dados de entrada
    liquidacao = conversores.converter_datas(settlement)
    vencimentos = conversores.converter_datas(vencimentos)

    # Validação estrutural: maturities e rates precisam ter o mesmo tamanho
    if len(vencimentos) != len(taxas):
        raise ValueError(
            "Vencimentos e taxas devem ter o mesmo tamanho. "
            f"Recebido: {len(vencimentos)} vencimentos e {len(taxas)} taxas."
        )

    # Cria DataFrame base e filtra vencimentos inválidos
    df_limpo = pl.DataFrame(
        data={"maturities": vencimentos, "rates": taxas},
        schema={"maturities": pl.Date, "rates": pl.Float64},
    ).filter(pl.col("maturities") > liquidacao)

    # Aviso sobre vencimentos filtrados
    total_filtrados = len(vencimentos) - df_limpo.height
    if total_filtrados > 0:
        logger.warning(
            "Vencimentos menores ou iguais à liquidação foram ignorados: %s removidos.",
            total_filtrados,
        )

    return liquidacao, df_limpo["maturities"], df_limpo["rates"]


def _criar_df_bootstrap(
    settlement: dt.date,
    taxas: pl.Series,
    vencimentos: pl.Series,
) -> pl.DataFrame:
    """Cria o DataFrame base para o bootstrap."""
    # Cria interpolador para taxas YTM em datas intermediárias
    interpolador_ff = interpolador.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(settlement, vencimentos),
        known_rates=taxas,
    )

    # Gera datas de cupom até o último vencimento
    ultimo_vencimento = vencimentos.max()
    assert isinstance(ultimo_vencimento, dt.date)
    todas_datas_cupom = _gerar_todas_datas_cupom(settlement, ultimo_vencimento)
    dias_uteis_ate_venc = bday.count(settlement, todas_datas_cupom)
    taxas_ytm = interpolador_ff.interpolate(dias_uteis_ate_venc)

    df = (
        pl.DataFrame(
            {
                "MaturityDate": todas_datas_cupom,
                "BDToMat": dias_uteis_ate_venc,
                "BYears": dias_uteis_ate_venc / 252,
                "YTM": taxas_ytm,
            }
        )
        .with_columns(
            Coupon=pl.lit(VALOR_CUPOM),
            SpotRate=pl.lit(None, dtype=pl.Float64),
        )
        .sort("MaturityDate")
    )
    return df


def _atualizar_taxa_spot(
    df: pl.DataFrame, vencimento: dt.date, taxa_spot: float
) -> pl.DataFrame:
    """Atualiza a taxa spot dentro do loop de bootstrap."""
    return df.with_columns(
        pl.when(pl.col("MaturityDate") == vencimento)
        .then(taxa_spot)
        .otherwise("SpotRate")
        .alias("SpotRate")
    )


def _calcular_valor_presente_cupons(
    df: pl.DataFrame,
    settlement: dt.date,
    vencimento: dt.date,
) -> float:
    """Calcula o valor presente dos cupons anteriores à maturidade."""
    datas_fluxo_anteriores = payment_dates(settlement, vencimento).to_list()[:-1]
    df_temp = df.filter(pl.col("MaturityDate").is_in(datas_fluxo_anteriores))

    return ferramentas.calculate_present_value(
        df_temp["Coupon"],
        df_temp["SpotRate"],
        df_temp["BYears"],
    )


def spot_rates(
    settlement: DateLike,
    maturities: ArrayLike,
    rates: ArrayLike,
    show_coupons: bool = False,
) -> pl.DataFrame:
    """
    Calcula as taxas spot da NTN-B usando bootstrap.

    O bootstrap determina as taxas spot a partir dos yields dos títulos,
    resolvendo iterativamente as taxas que descontam os fluxos ao preço.

    Args:
        settlement (DateLike): Data de liquidação.
        maturities (ArrayLike): Datas de vencimento dos títulos.
        rates (ArrayLike): Taxas YTM correspondentes.
        show_coupons (bool, optional): Se True, inclui datas intermediárias de cupom.
            Padrão False.

    Returns:
        pl.DataFrame: DataFrame com as taxas spot.

    Output Columns:
        * MaturityDate (Date): Data de vencimento.
        * BDToMat (Int64): Dias úteis entre liquidação e vencimento.
        * SpotRate (Float64): Taxa spot (real).

    Examples:
        >>> from pyield import ntnb
        >>> # Busca as taxas de NTN-B para uma data de referência
        >>> df = ntnb.data("16-08-2024")
        >>> # Calcula as taxas spot considerando a liquidação na data de referência
        >>> ntnb.spot_rates(
        ...     settlement="16-08-2024",
        ...     maturities=df["MaturityDate"],
        ...     rates=df["IndicativeRate"],
        ... )
        shape: (14, 3)
        ┌──────────────┬─────────┬──────────┐
        │ MaturityDate ┆ BDToMat ┆ SpotRate │
        │ ---          ┆ ---     ┆ ---      │
        │ date         ┆ i64     ┆ f64      │
        ╞══════════════╪═════════╪══════════╡
        │ 2025-05-15   ┆ 185     ┆ 0.063893 │
        │ 2026-08-15   ┆ 502     ┆ 0.066141 │
        │ 2027-05-15   ┆ 687     ┆ 0.064087 │
        │ 2028-08-15   ┆ 1002    ┆ 0.063057 │
        │ 2029-05-15   ┆ 1186    ┆ 0.061458 │
        │ …            ┆ …       ┆ …        │
        │ 2040-08-15   ┆ 4009    ┆ 0.058326 │
        │ 2045-05-15   ┆ 5196    ┆ 0.060371 │
        │ 2050-08-15   ┆ 6511    ┆ 0.060772 │
        │ 2055-05-15   ┆ 7700    ┆ 0.059909 │
        │ 2060-08-15   ┆ 9017    ┆ 0.060652 │
        └──────────────┴─────────┴──────────┘

    Notes:
        O cálculo considera:
        * Mapear todas as datas de pagamento até o último vencimento.
        * Interpolar as taxas YTM nas datas intermediárias.
        * Calcular a cotação da NTN-B para cada vencimento.
        * Calcular as taxas spot reais.
    """
    if any_is_empty(settlement, maturities, rates):
        return pl.DataFrame()

    settlement, maturities, rates = _validar_entradas_taxa_spot(
        settlement, maturities, rates
    )

    df = _criar_df_bootstrap(settlement, rates, maturities)

    # Bootstrap para calcular taxas spot
    linhas = df.to_dicts()
    primeiro_vencimento = maturities.min()
    for linha in linhas:
        vencimento = linha["MaturityDate"]

        # Taxas spot <= primeiro vencimento são YTM por definição
        if vencimento <= primeiro_vencimento:
            taxa_spot = linha["YTM"]
            df = _atualizar_taxa_spot(df, vencimento, taxa_spot)
            continue

        # Calcula taxa spot para o vencimento corrente
        valor_presente_cupons = _calcular_valor_presente_cupons(
            df, settlement, vencimento
        )
        preco_titulo = quotation(settlement, vencimento, linha["YTM"])
        fator_preco = VALOR_FINAL / (preco_titulo - valor_presente_cupons)
        taxa_spot = fator_preco ** (1 / linha["BYears"]) - 1

        df = _atualizar_taxa_spot(df, vencimento, taxa_spot)

    if not show_coupons:
        df = df.filter(pl.col("MaturityDate").is_in(maturities.to_list()))
    return df.select(["MaturityDate", "BDToMat", "SpotRate"])


def bei_rates(
    settlement: DateLike,
    ntnb_maturities: ArrayLike,
    ntnb_rates: ArrayLike,
    nominal_maturities: ArrayLike,
    nominal_rates: ArrayLike,
) -> pl.DataFrame:
    """
    Calcula a inflação implícita (BEI) para NTN-B a partir de taxas nominais e reais.

    A BEI é a inflação que iguala yields reais e nominais, baseada nas taxas spot
    das NTN-B.

    Args:
        settlement (DateLike): Data de liquidação da operação.
        ntnb_maturities (ArrayLike): Vencimentos das NTN-B.
        ntnb_rates (ArrayLike): Taxas reais (YTM) correspondentes.
        nominal_maturities (ArrayLike): Vencimentos de referência para taxas nominais.
        nominal_rates (ArrayLike): Taxas nominais (ex.: DI Futuro).

    Returns:
        pl.DataFrame: DataFrame com as BEI calculadas.

    Output Columns:
        * MaturityDate (Date): Data de vencimento.
        * BDToMat (Int64): Dias úteis entre liquidação e vencimento.
        * RIR (Float64): Taxa real (spot).
        * NIR (Float64): Taxa nominal interpolada.
        * BEI (Float64): Inflação implícita (breakeven).

    Notes:
        A BEI indica a inflação esperada pelo mercado entre liquidação e vencimento.

    Examples:
        Busca as taxas de NTN-B para uma data de referência.
        Estas são taxas YTM e as taxas spot são calculadas a partir delas.
        >>> df_ntnb = yd.ntnb.data("05-09-2024")

        Busca as taxas de ajuste do DI Futuro para a mesma data de referência:
        >>> df_di = yd.di1.data("05-09-2024")

        Calcula as BEI considerando a liquidação na data de referência:
        >>> yd.ntnb.bei_rates(
        ...     settlement="05-09-2024",
        ...     ntnb_maturities=df_ntnb["MaturityDate"],
        ...     ntnb_rates=df_ntnb["IndicativeRate"],
        ...     nominal_maturities=df_di["ExpirationDate"],
        ...     nominal_rates=df_di["SettlementRate"],
        ... )
        shape: (14, 5)
        ┌──────────────┬─────────┬──────────┬──────────┬──────────┐
        │ MaturityDate ┆ BDToMat ┆ RIR      ┆ NIR      ┆ BEI      │
        │ ---          ┆ ---     ┆ ---      ┆ ---      ┆ ---      │
        │ date         ┆ i64     ┆ f64      ┆ f64      ┆ f64      │
        ╞══════════════╪═════════╪══════════╪══════════╪══════════╡
        │ 2025-05-15   ┆ 171     ┆ 0.061748 ┆ 0.113836 ┆ 0.049059 │
        │ 2026-08-15   ┆ 488     ┆ 0.066133 ┆ 0.117126 ┆ 0.04783  │
        │ 2027-05-15   ┆ 673     ┆ 0.063816 ┆ 0.117169 ┆ 0.050152 │
        │ 2028-08-15   ┆ 988     ┆ 0.063635 ┆ 0.11828  ┆ 0.051376 │
        │ 2029-05-15   ┆ 1172    ┆ 0.062532 ┆ 0.11838  ┆ 0.052561 │
        │ …            ┆ …       ┆ …        ┆ …        ┆ …        │
        │ 2040-08-15   ┆ 3995    ┆ 0.060468 ┆ 0.11759  ┆ 0.053865 │
        │ 2045-05-15   ┆ 5182    ┆ 0.0625   ┆ 0.11759  ┆ 0.05185  │
        │ 2050-08-15   ┆ 6497    ┆ 0.063016 ┆ 0.11759  ┆ 0.051339 │
        │ 2055-05-15   ┆ 7686    ┆ 0.062252 ┆ 0.11759  ┆ 0.052095 │
        │ 2060-08-15   ┆ 9003    ┆ 0.063001 ┆ 0.11759  ┆ 0.051354 │
        └──────────────┴─────────┴──────────┴──────────┴──────────┘
    """
    if any_is_empty(
        settlement, ntnb_maturities, ntnb_rates, nominal_maturities, nominal_rates
    ):
        return pl.DataFrame()
    # Normaliza datas de entrada
    liquidacao = conversores.converter_datas(settlement)
    ntnb_maturities = conversores.converter_datas(ntnb_maturities)

    interpolador_ff = interpolador.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(liquidacao, nominal_maturities),
        known_rates=nominal_rates,
        extrapolate=True,
    )
    df_spot = spot_rates(liquidacao, ntnb_maturities, ntnb_rates)
    df = (
        df_spot.rename({"SpotRate": "RIR"})
        .with_columns(
            NIR=interpolador_ff(df_spot["BDToMat"]),
        )
        .with_columns(
            BEI=((pl.col("NIR") + 1) / (pl.col("RIR") + 1)) - 1,
        )
        .select("MaturityDate", "BDToMat", "RIR", "NIR", "BEI")
    )

    return df


def duration(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula a Macaulay duration da NTN-B em anos úteis.

    Fórmula:
                   Sum( t * CFₜ / (1 + y)ᵗ )
         MacD = ---------------------------------
                         Current Bond Price

    Onde:
        t    = tempo (anos) até o pagamento
        CFₜ = fluxo no tempo t
        y    = taxa YTM (periódica)
        Price = Soma( CFₜ / (1 + y)ᵗ )

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto usada no cálculo.

    Returns:
        float: Macaulay duration em anos úteis.

     Examples:
         >>> from pyield import ntnb
         >>> ntnb.duration("23-08-2024", "15-08-2060", 0.061005)
         15.08305431313046
    """
    if any_is_empty(settlement, maturity, rate):
        return float("nan")

    df = cash_flows(settlement, maturity)
    if df.is_empty():
        return float("nan")

    anos_uteis = bday.count(settlement, df["PaymentDate"]) / 252
    dcf = df["CashFlow"] / (1 + rate) ** anos_uteis
    duracao = (dcf * anos_uteis).sum() / dcf.sum()
    # Truncar para 14 casas decimais para repetibilidade dos resultados
    return ferramentas.truncate(duracao, 14)


def dv01(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
    vna: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto (YTM) da NTN-B.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.dv01("26-03-2025", "15-08-2060", 0.074358, 4470.979474)
        4.640875999999935
    """
    if any_is_empty(settlement, maturity, rate, vna):
        return float("nan")

    cotacao_1 = quotation(settlement, maturity, rate)
    cotacao_2 = quotation(settlement, maturity, rate + 0.0001)
    preco_1 = price(vna, cotacao_1)
    preco_2 = price(vna, cotacao_2)
    return preco_1 - preco_2


def forwards(
    date: DateLike,
    zero_coupon: bool = True,
) -> pl.DataFrame:
    """
    Calcula as taxas forward da NTN-B para a data de referência.

    Args:
        date (DateLike): Data de referência para a consulta.
        zero_coupon (bool, optional): Se True, usa taxas zero cupom no cálculo.
            Padrão True. Se False, usa as taxas YTM.

    Returns:
        pl.DataFrame: DataFrame com as taxas forward.

    Output Columns:
        * MaturityDate (Date): Data de vencimento.
        * BDToMat (Int64): Dias úteis entre referência e vencimento.
        * IndicativeRate (Float64): Taxa indicativa (spot ou YTM).
        * ForwardRate (Float64): Taxa forward calculada.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.forwards("17-10-2025", zero_coupon=True)
        shape: (13, 4)
        ┌──────────────┬─────────┬────────────────┬─────────────┐
        │ MaturityDate ┆ BDToMat ┆ IndicativeRate ┆ ForwardRate │
        │ ---          ┆ ---     ┆ ---            ┆ ---         │
        │ date         ┆ i64     ┆ f64            ┆ f64         │
        ╞══════════════╪═════════╪════════════════╪═════════════╡
        │ 2026-08-15   ┆ 207     ┆ 0.10089        ┆ 0.10089     │
        │ 2027-05-15   ┆ 392     ┆ 0.088776       ┆ 0.074793    │
        │ 2028-08-15   ┆ 707     ┆ 0.083615       ┆ 0.076598    │
        │ 2029-05-15   ┆ 891     ┆ 0.0818         ┆ 0.074148    │
        │ 2030-08-15   ┆ 1205    ┆ 0.080902       ┆ 0.077857    │
        │ …            ┆ …       ┆ …              ┆ …           │
        │ 2040-08-15   ┆ 3714    ┆ 0.076067       ┆ 0.070587    │
        │ 2045-05-15   ┆ 4901    ┆ 0.075195       ┆ 0.069811    │
        │ 2050-08-15   ┆ 6216    ┆ 0.074087       ┆ 0.064348    │
        │ 2055-05-15   ┆ 7405    ┆ 0.073702       ┆ 0.067551    │
        │ 2060-08-15   ┆ 8722    ┆ 0.073795       ┆ 0.074505    │
        └──────────────┴─────────┴────────────────┴─────────────┘
    """
    if any_is_empty(date):
        return pl.DataFrame()

    # Valida e normaliza a data
    df = data(date).select("MaturityDate", "BDToMat", "IndicativeRate")
    if zero_coupon:
        df_ref = spot_rates(
            settlement=date,
            maturities=df["MaturityDate"],
            rates=df["IndicativeRate"],
        ).rename({"SpotRate": "ReferenceRate"})
    else:
        df_ref = df.rename({"IndicativeRate": "ReferenceRate"})
    taxas_forward = fwd.forwards(bdays=df_ref["BDToMat"], rates=df_ref["ReferenceRate"])
    df_ref = df_ref.with_columns(ForwardRate=taxas_forward)
    df = df.join(
        df_ref.select("MaturityDate", "ForwardRate"),
        on="MaturityDate",
        how="inner",
    ).sort("MaturityDate")
    return df
