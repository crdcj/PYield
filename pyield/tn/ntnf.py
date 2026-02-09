import datetime as dt
import logging
import math
from collections.abc import Callable

import polars as pl
from dateutil.relativedelta import relativedelta

import pyield.converters as cv
import pyield.interpolator as ip
from pyield import anbima, bday
from pyield.tn import tools
from pyield.tn.pre import di_spreads as pre_di_spreads
from pyield.types import ArrayLike, DateLike, any_is_empty

"""
Constantes calculadas conforme regras da ANBIMA
TAXA_CUPOM = (0.10 + 1) ** 0.5 - 1  -> 10% a.a. com capitalização semestral
VALOR_FACE = 1000
VALOR_CUPOM = round(VALOR_FACE * TAXA_CUPOM, 5)
VALOR_FINAL = VALOR_FACE + VALOR_CUPOM
"""
DIA_CUPOM = 1
MESES_CUPOM = {1, 7}
VALOR_CUPOM = 48.80885
VALOR_FINAL = 1048.80885  # 1000 + 48.80885

logger = logging.getLogger(__name__)


def data(date: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de NTN-F para a data de referência.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de NTN-F.

    Output Columns:
        * BondType (String): Tipo do título (ex.: "NTN-F").
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
        * StdDev (Float64): Desvio padrão da taxa indicativa.
        * LowerBoundRateD0 (Float64): Limite inferior do intervalo (D+0).
        * UpperBoundRateD0 (Float64): Limite superior do intervalo (D+0).
        * LowerBoundRateD1 (Float64): Limite inferior do intervalo (D+1).
        * UpperBoundRateD1 (Float64): Limite superior do intervalo (D+1).
        * Criteria (String): Critério utilizado pela ANBIMA.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.data("23-08-2024")
        shape: (6, 14)
        ┌───────────────┬──────────┬───────────┬───────────────┬───┬──────────┬──────────┬────────────────┬─────────┐
        │ ReferenceDate ┆ BondType ┆ SelicCode ┆ IssueBaseDate ┆ … ┆ BidRate  ┆ AskRate  ┆ IndicativeRate ┆ DIRate  │
        │ ---           ┆ ---      ┆ ---       ┆ ---           ┆   ┆ ---      ┆ ---      ┆ ---            ┆ ---     │
        │ date          ┆ str      ┆ i64       ┆ date          ┆   ┆ f64      ┆ f64      ┆ f64            ┆ f64     │
        ╞═══════════════╪══════════╪═══════════╪═══════════════╪═══╪══════════╪══════════╪════════════════╪═════════╡
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2014-01-10    ┆ … ┆ 0.107864 ┆ 0.107524 ┆ 0.107692       ┆ 0.10823 │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2016-01-15    ┆ … ┆ 0.11527  ┆ 0.114948 ┆ 0.115109       ┆ 0.11467 │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2018-01-05    ┆ … ┆ 0.116468 ┆ 0.11621  ┆ 0.116337       ┆ 0.1156  │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2020-01-10    ┆ … ┆ 0.117072 ┆ 0.116958 ┆ 0.117008       ┆ 0.11575 │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2022-01-07    ┆ … ┆ 0.116473 ┆ 0.116164 ┆ 0.116307       ┆ 0.11554 │
        │ 2024-08-23    ┆ NTN-F    ┆ 950199    ┆ 2024-01-05    ┆ … ┆ 0.116662 ┆ 0.116523 ┆ 0.116586       ┆ 0.11531 │
        └───────────────┴──────────┴───────────┴───────────────┴───┴──────────┴──────────┴────────────────┴─────────┘
    """  # noqa
    return anbima.tpf_data(date, "NTN-F")


def maturities(date: DateLike) -> pl.Series:
    """
    Busca os vencimentos de NTN-F disponíveis para a data de referência.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.Series: Série de datas de vencimento de NTN-F.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.maturities("23-08-2024")
        shape: (6,)
        Series: 'MaturityDate' [date]
        [
            2025-01-01
            2027-01-01
            2029-01-01
            2031-01-01
            2033-01-01
            2035-01-01
        ]
    """
    return data(date)["MaturityDate"]


def payment_dates(
    settlement: DateLike,
    maturity: DateLike,
) -> pl.Series:
    """
    Gera todas as datas de cupom entre liquidação e vencimento.

    As datas são exclusivas para a liquidação e inclusivas para o vencimento.
    Os cupons são pagos em 1º de janeiro e 1º de julho. O título NTN-F é
    determinado pela data de vencimento.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.

    Returns:
        pl.Series: Série com as datas de cupom entre a liquidação (exclusiva)
            e o vencimento (inclusiva). Retorna série vazia se o vencimento
            for menor ou igual à liquidação.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.payment_dates("15-05-2024", "01-01-2027")
        shape: (6,)
        Series: 'payment_dates' [date]
        [
            2024-07-01
            2025-01-01
            2025-07-01
            2026-01-01
            2026-07-01
            2027-01-01
        ]
    """
    if any_is_empty(settlement, maturity):
        return pl.Series(dtype=pl.Date)
    # Normaliza datas
    liquidacao = cv.converter_datas(settlement)
    vencimento = cv.converter_datas(maturity)

    # Verifica se vencimento é posterior à liquidação
    if vencimento <= liquidacao:
        return pl.Series(dtype=pl.Date)

    # Inicializa variáveis do loop
    data_cupom = vencimento
    datas_cupons = []

    # Itera de trás para frente do vencimento até a liquidação
    while data_cupom > liquidacao:
        datas_cupons.append(data_cupom)
        # Retrocede 6 meses
        data_cupom -= relativedelta(months=6)

    return pl.Series(name="payment_dates", values=datas_cupons).sort()


def cash_flows(
    settlement: DateLike,
    maturity: DateLike,
    adj_payment_dates: bool = False,
) -> pl.DataFrame:
    """
    Gera os fluxos de caixa da NTN-F entre liquidação (exclusiva) e vencimento
    (inclusivo). Os fluxos incluem cupons e o pagamento final no vencimento.

    Args:
        settlement (DateLike): Data de liquidação (exclusiva).
        maturity (DateLike): Data de vencimento do título.
        adj_payment_dates (bool): Se True, ajusta as datas de pagamento para o
            próximo dia útil.

    Returns:
        pl.DataFrame: DataFrame com as colunas "PaymentDate" e "CashFlow".

    Output Columns:
        * PaymentDate (Date): Data de pagamento do fluxo.
        * CashFlow (Float64): Valor do fluxo de caixa.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.cash_flows("15-05-2024", "01-01-2027")
        shape: (6, 2)
        ┌─────────────┬────────────┐
        │ PaymentDate ┆ CashFlow   │
        │ ---         ┆ ---        │
        │ date        ┆ f64        │
        ╞═════════════╪════════════╡
        │ 2024-07-01  ┆ 48.80885   │
        │ 2025-01-01  ┆ 48.80885   │
        │ 2025-07-01  ┆ 48.80885   │
        │ 2026-01-01  ┆ 48.80885   │
        │ 2026-07-01  ┆ 48.80885   │
        │ 2027-01-01  ┆ 1048.80885 │
        └─────────────┴────────────┘
    """
    if any_is_empty(settlement, maturity):
        return pl.DataFrame()
    # Normaliza datas de entrada
    liquidacao = cv.converter_datas(settlement)
    vencimento = cv.converter_datas(maturity)

    # Obtém as datas de pagamento entre liquidação e vencimento
    datas_pagamento = payment_dates(liquidacao, vencimento)

    # Retorna DataFrame vazio se não houver pagamentos (liquidação >= vencimento)
    if datas_pagamento.is_empty():
        return pl.DataFrame(schema={"PaymentDate": pl.Date, "CashFlow": pl.Float64})

    # Define o fluxo final no vencimento e os demais como cupom
    df = pl.DataFrame(
        data={"PaymentDate": datas_pagamento},
    ).with_columns(
        pl.when(pl.col("PaymentDate") == vencimento)
        .then(VALOR_FINAL)
        .otherwise(VALOR_CUPOM)
        .alias("CashFlow")
    )

    if adj_payment_dates:
        df = df.with_columns(PaymentDate=bday.offset_expr("PaymentDate", 0))
    return df


def price(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula o preço da NTN-F pelas regras da ANBIMA, equivalente ao valor
    presente dos fluxos descontados pela taxa de YTM informada.

    Args:
        settlement (DateLike): Data de liquidação para cálculo do preço.
        maturity (DateLike): Data de vencimento do título.
        rate (float): Taxa de desconto (YTM) usada para calcular o valor presente.

    Returns:
        float: Preço da NTN-F conforme ANBIMA.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - O cupom semestral é 48,81, que representa 10% a.a. com capitalização
          semestral e arredondamento para 5 casas, conforme ANBIMA.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.price("05-07-2024", "01-01-2035", 0.11921)
        895.359254
    """
    if any_is_empty(settlement, maturity, rate):
        return float("nan")

    df_fluxos = cash_flows(settlement, maturity)
    if df_fluxos.is_empty():
        return float("nan")

    valores_fluxo = df_fluxos["CashFlow"]
    dias_uteis = bday.count(settlement, df_fluxos["PaymentDate"])
    anos_uteis = tools.truncate(dias_uteis / 252, 14)
    fatores_desconto = (1 + rate) ** anos_uteis
    # Calcula o valor presente de cada fluxo (DCF) com arredondamento ANBIMA
    dcf = (valores_fluxo / fatores_desconto).round(9)
    # Soma dos fluxos descontados com truncamento ANBIMA
    return tools.truncate(dcf.sum(), 6)


def spot_rates(  # noqa
    settlement: DateLike,
    ltn_maturities: ArrayLike,
    ltn_rates: ArrayLike,
    ntnf_maturities: ArrayLike,
    ntnf_rates: ArrayLike,
    show_coupons: bool = False,
) -> pl.DataFrame:
    """
    Calcula as taxas spot (zero cupom) para NTN-F usando bootstrap.

    O bootstrap determina as taxas spot a partir dos yields dos títulos.
    O método resolve iterativamente as taxas que descontam os fluxos ao preço.
    Usa as LTNs (zero cupom) até o último vencimento LTN disponível. Após
    isso, calcula as taxas spot a partir das NTN-F.


    Args:
        settlement (DateLike): Data de liquidação para o cálculo.
        ltn_maturities (ArrayLike): Vencimentos conhecidos de LTN.
        ltn_rates (ArrayLike): Taxas conhecidas de LTN.
        ntnf_maturities (ArrayLike): Vencimentos conhecidos de NTN-F.
        ntnf_rates (ArrayLike): Taxas conhecidas de NTN-F.
        show_coupons (bool): Se True, inclui as datas de cupom (julho).
            Padrão False.

    Returns:
        pl.DataFrame: DataFrame com colunas "MaturityDate", "BDToMat" e "SpotRate".
            "BDToMat" são os dias úteis entre liquidação e vencimento.

    Output Columns:
        * MaturityDate (Date): Data de vencimento.
        * BDToMat (Int64): Dias úteis entre liquidação e vencimento.
        * SpotRate (Float64): Taxa spot (zero cupom).

    Examples:
        >>> from pyield import ntnf, ltn
        >>> df_ltn = ltn.data("03-09-2024")
        >>> df_ntnf = ntnf.data("03-09-2024")
        >>> ntnf.spot_rates(
        ...     settlement="03-09-2024",
        ...     ltn_maturities=df_ltn["MaturityDate"],
        ...     ltn_rates=df_ltn["IndicativeRate"],
        ...     ntnf_maturities=df_ntnf["MaturityDate"],
        ...     ntnf_rates=df_ntnf["IndicativeRate"],
        ... )
        shape: (6, 3)
        ┌──────────────┬─────────┬──────────┐
        │ MaturityDate ┆ BDToMat ┆ SpotRate │
        │ ---          ┆ ---     ┆ ---      │
        │ date         ┆ i64     ┆ f64      │
        ╞══════════════╪═════════╪══════════╡
        │ 2025-01-01   ┆ 83      ┆ 0.108837 │
        │ 2027-01-01   ┆ 584     ┆ 0.119981 │
        │ 2029-01-01   ┆ 1083    ┆ 0.122113 │
        │ 2031-01-01   ┆ 1584    ┆ 0.122231 │
        │ 2033-01-01   ┆ 2088    ┆ 0.121355 │
        │ 2035-01-01   ┆ 2587    ┆ 0.121398 │
        └──────────────┴─────────┴──────────┘
    """
    if any_is_empty(settlement, ltn_maturities, ltn_rates, ntnf_maturities, ntnf_rates):
        return pl.DataFrame()
    # 1. Converter e normalizar inputs para Polars
    liquidacao = cv.converter_datas(settlement)
    vencimentos_ltn = cv.converter_datas(ltn_maturities)
    vencimentos_ntnf = cv.converter_datas(ntnf_maturities)
    if not isinstance(ltn_rates, pl.Series):
        taxas_ltn = pl.Series(ltn_rates).cast(pl.Float64)
    else:
        taxas_ltn = ltn_rates
    if not isinstance(ntnf_rates, pl.Series):
        taxas_ntnf = pl.Series(ntnf_rates).cast(pl.Float64)
    else:
        taxas_ntnf = ntnf_rates

    # 2. Criar interpoladores (aceitam pl.Series diretamente)
    interpolador_ltn = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(liquidacao, vencimentos_ltn),
        known_rates=taxas_ltn,
    )
    interpolador_ntnf = ip.Interpolator(
        method="flat_forward",
        known_bdays=bday.count(liquidacao, vencimentos_ntnf),
        known_rates=taxas_ntnf,
    )

    # 3. Gerar todas as datas de cupom até o último vencimento NTN-F
    ultimo_vencimento = vencimentos_ntnf.max()
    assert isinstance(ultimo_vencimento, dt.date)
    todas_datas_cupom = payment_dates(liquidacao, ultimo_vencimento)

    # 4. Construir DataFrame inicial
    dias_uteis_ate_venc = bday.count(liquidacao, todas_datas_cupom)
    taxas_ytm = interpolador_ntnf(dias_uteis_ate_venc)
    df = pl.DataFrame(
        {
            "MaturityDate": todas_datas_cupom,
            "BDToMat": dias_uteis_ate_venc,
            "BYears": dias_uteis_ate_venc / 252,
            "YTM": taxas_ytm,
        }
    ).with_columns(
        Coupon=pl.lit(VALOR_CUPOM),
    )

    # 5. Loop de bootstrap (iterativo por dependência sequencial)
    ultimo_vencimento_ltn = vencimentos_ltn.max()
    assert isinstance(ultimo_vencimento_ltn, dt.date)

    lista_vencimentos = df["MaturityDate"]
    lista_dias_uteis = df["BDToMat"]
    lista_anos_uteis = df["BYears"]
    lista_ytm = df["YTM"]

    taxas_spot_resolvidas: list[float | None] = []
    mapa_spot: dict[dt.date, float | None] = {}

    for i in range(len(df)):
        data_venc = lista_vencimentos[i]
        assert isinstance(data_venc, dt.date)
        dias_uteis_val = int(lista_dias_uteis[i])
        anos_uteis_val = float(lista_anos_uteis[i])
        ytm_val = float(lista_ytm[i])

        # Caso esteja antes (ou igual) ao último vencimento LTN: usar interpolador LTN
        if data_venc <= ultimo_vencimento_ltn:
            taxa_spot = interpolador_ltn(dias_uteis_val)
            taxas_spot_resolvidas.append(taxa_spot)
            mapa_spot[data_venc] = taxa_spot
            continue

        # Datas de cupom (exclui último pagamento) para este vencimento
        datas_fluxo = payment_dates(liquidacao, data_venc)[:-1]
        if len(datas_fluxo) == 0:
            # Caso improvável, mas protege contra divisão por zero mais adiante
            taxa_spot = None
            taxas_spot_resolvidas.append(taxa_spot)
            mapa_spot[data_venc] = taxa_spot
            continue

        # Recuperar SpotRates já solucionadas para estes cupons
        taxas_spot_fluxo = [mapa_spot[d] for d in datas_fluxo]
        periodos_fluxo = bday.count(liquidacao, datas_fluxo) / 252
        fluxos = [VALOR_CUPOM] * len(datas_fluxo)

        valor_presente_fluxo = tools.calculate_present_value(
            cash_flows=pl.Series(fluxos),
            rates=pl.Series(taxas_spot_fluxo),
            periods=periodos_fluxo,
        )

        preco_titulo = price(liquidacao, data_venc, ytm_val)
        fator_preco = VALOR_FINAL / (preco_titulo - valor_presente_fluxo)
        taxa_spot = fator_preco ** (1 / anos_uteis_val) - 1

        taxas_spot_resolvidas.append(taxa_spot)
        mapa_spot[data_venc] = taxa_spot

    # 6. Anexar coluna SpotRate
    df = df.with_columns(SpotRate=pl.Series(taxas_spot_resolvidas, dtype=pl.Float64))

    # 7. Selecionar colunas finais
    df = df.select(["MaturityDate", "BDToMat", "SpotRate"])

    # 8. Remover cupons (Julho) se não solicitado
    if not show_coupons:
        df = df.filter(pl.col("MaturityDate").is_in(vencimentos_ntnf.implode()))

    return df


def _encontrar_intervalo_raiz(
    func: Callable[[float], float],
) -> tuple[float, float] | None:
    """
    Encontra um intervalo [a, b] para a TAXA DE JUROS que zera a função.

    Otimizado para o contexto financeiro, buscando a taxa apenas em um
    intervalo realista. A função 'func' é a que calcula a diferença de
    preço dado uma taxa.
    """
    # --- LIMITES DE BOM SENSO PARA A *TAXA* QUE ESTAMOS PROCURANDO ---
    # Uma taxa/spread não vai ser -50% ou +200%, então limitamos a busca.
    taxa_inicial: float = 0.01
    passo: float = 0.01
    fator_crescimento: float = 1.6
    max_tentativas: int = 100

    # Limites para a TAXA (variável 'a' e 'b' da busca)
    taxa_min: float = -1.0  # Limite inferior: -100%
    taxa_max: float = 10.00  # Limite superior: 1000%
    # -----------------------------------------------------------------

    # Ponto de partida
    f0 = func(taxa_inicial)
    if abs(f0) == 0:
        return (taxa_inicial, taxa_inicial)

    # 1. Busca na direção positiva
    a, fa = taxa_inicial, f0
    b = taxa_inicial + passo
    passo_atual = passo

    for _ in range(max_tentativas):
        # Se a PRÓXIMA TAXA A SER TESTADA ('b') for irrealista, paramos.
        if b > taxa_max:
            break

        fb = func(b)
        if fa * fb < 0:
            return (a, b)

        a, fa = b, fb
        passo_atual *= fator_crescimento
        b += passo_atual

    # 2. Busca na direção negativa
    a, fa = taxa_inicial, f0
    b = taxa_inicial - passo
    passo_atual = passo

    for _ in range(max_tentativas):
        # Se a PRÓXIMA TAXA A SER TESTADA ('b') for irrealista, paramos.
        if b < taxa_min:
            break

        fb = func(b)
        if fa * fb < 0:
            return (b, a)

        a, fa = b, fb
        passo_atual *= fator_crescimento
        b -= passo_atual

    # Se a busca falhou dentro dos limites realistas
    return None


def _metodo_bissecao(func: Callable[[float], float], a: float, b: float) -> float:
    """Método da bisseção para encontrar raiz.

    Args:
        func (Callable[[float], float]): Função para a qual se busca a raiz.
        a (float): Limite inferior do intervalo.
        b (float): Limite superior do intervalo.

    Returns:
        float: Raiz aproximada de ``func`` no intervalo ``[a, b]``.

    Raises:
        ValueError: Se ``func`` não muda de sinal no intervalo ``[a, b]``.
    """
    tolerancia = 1e-8
    max_iter = 100
    fa, fb = func(a), func(b)
    if fa * fb > 0:
        logger.warning(
            "Falha no método da bisseção: a função não muda de sinal no intervalo."
        )
        return float("nan")

    for _ in range(max_iter):
        ponto_medio = (a + b) / 2
        fmeio = func(ponto_medio)
        if abs(fmeio) < tolerancia or (b - a) / 2 < tolerancia:
            return ponto_medio
        if fmeio * fa < 0:
            b, fb = ponto_medio, fmeio
        else:
            a, fa = ponto_medio, fmeio

    return (a + b) / 2


def _resolver_spread(
    func_diferenca_preco: Callable,
) -> float:
    """
    Versão robusta que encontra automaticamente um intervalo válido.
    """
    # Tenta encontrar intervalo válido
    intervalo = _encontrar_intervalo_raiz(func_diferenca_preco)

    if intervalo is None:
        logger.warning("Não foi possível encontrar intervalo de busca válido")
        return float("nan")

    a, b = intervalo
    return _metodo_bissecao(func_diferenca_preco, a, b)


def premium(  # noqa
    settlement: DateLike,
    ntnf_maturity: DateLike,
    ntnf_rate: float,
    di_expirations: DateLike,
    di_rates: ArrayLike,
) -> float:
    """
    Calcula o prêmio de uma NTN-F sobre a curva DI.

    A função compara o fator de desconto implícito da NTN-F com o da curva DI,
    determinando o prêmio líquido com base na diferença entre os fatores.

    Args:
        settlement (DateLike): Data de liquidação para o cálculo.
        ntnf_maturity (DateLike): Data de vencimento da NTN-F.
        ntnf_rate (float): Taxa de YTM da NTN-F.
        di_expirations (DateLike): Datas de vencimento da curva DI.
        di_rates (ArrayLike): Taxas DI correspondentes aos vencimentos.

    Returns:
        float: Prêmio da NTN-F sobre a curva DI, em fator. Retorna NaN em erro.

    Examples:
        >>> # Obs: apenas algumas taxas DI serão usadas no exemplo.
        >>> exp_dates = ["2025-01-01", "2030-01-01", "2035-01-01"]
        >>> di_rates = [0.10823, 0.11594, 0.11531]
        >>> premium(
        ...     settlement="23-08-2024",
        ...     ntnf_maturity="01-01-2035",
        ...     ntnf_rate=0.116586,
        ...     di_expirations=exp_dates,
        ...     di_rates=di_rates,
        ... )
        1.0099602679927115

    Notes:
        A função ajusta as datas de pagamento para dias úteis e calcula o valor
        presente dos fluxos da NTN-F usando as taxas DI.

    """
    if any_is_empty(settlement, ntnf_maturity, ntnf_rate, di_expirations, di_rates):
        return float("nan")

    if not isinstance(di_rates, pl.Series):
        taxas_di = pl.Series(di_rates)
    else:
        taxas_di = di_rates

    df_fluxos = cash_flows(settlement, ntnf_maturity, adj_payment_dates=True)
    if df_fluxos.is_empty():
        return float("nan")

    interpolador_ff = ip.Interpolator(
        "flat_forward",
        bday.count(settlement, di_expirations),  # type: ignore[arg-type]
        taxas_di,
    )

    dias_uteis_pagamento = bday.count(settlement, df_fluxos["PaymentDate"])
    df = df_fluxos.with_columns(
        BDToMat=dias_uteis_pagamento,
        BYears=dias_uteis_pagamento / 252,
        DIRate=interpolador_ff(dias_uteis_pagamento),
    )

    preco_titulo = tools.calculate_present_value(
        cash_flows=df["CashFlow"],
        rates=df["DIRate"],
        periods=df["BYears"],
    )

    if math.isnan(preco_titulo):
        return float("nan")

    def diferenca_preco(taxa: float) -> float:
        fluxos_descontados = df["CashFlow"] / (1 + taxa) ** df["BYears"]
        return fluxos_descontados.sum() - preco_titulo

    di_ytm = _resolver_spread(diferenca_preco)

    if math.isnan(di_ytm):
        return float("nan")

    fator_ntnf = (1 + ntnf_rate) ** (1 / 252)
    fator_di = (1 + di_ytm) ** (1 / 252)
    if fator_di == 1:
        return float("inf") if fator_ntnf > 1 else 0.0

    premio = (fator_ntnf - 1) / (fator_di - 1)
    return premio


def di_net_spread(  # noqa
    settlement: DateLike,
    ntnf_maturity: DateLike,
    ntnf_rate: float,
    di_expirations: ArrayLike,
    di_rates: ArrayLike,
) -> float:
    """
    Calcula o spread líquido sobre DI dado a YTM e a curva DI.

    A função determina o spread que iguala o valor presente dos fluxos ao preço
    do título. Interpola as taxas DI nas datas de pagamento e encontra o spread
    (em bps) que zera a diferença de preços.

    Args:
        settlement (DateLike): Data de liquidação para o cálculo.
        ntnf_maturity (DateLike): Data de vencimento do título.
        ntnf_rate (float): Taxa YTM do título.
        di_rates (ArrayLike): Série de taxas DI.
        di_expirations (ArrayLike): Vencimentos da curva DI.

    Returns:
        float: Spread líquido em formato decimal (ex.: 0.0012 = 12 bps).
            Retorna NaN em caso de erro.

    Examples:
        # Obs: apenas algumas taxas DI serão usadas no exemplo.
        >>> exp_dates = ["2025-01-01", "2030-01-01", "2035-01-01"]
        >>> di_rates = [0.10823, 0.11594, 0.11531]
        >>> spread = di_net_spread(
        ...     settlement="23-08-2024",
        ...     ntnf_maturity="01-01-2035",
        ...     ntnf_rate=0.116586,
        ...     di_expirations=exp_dates,
        ...     di_rates=di_rates,
        ... )
        >>> round(spread * 10_000, 2)  # Converte para bps para exibição
        12.13

        >>> # Entradas nulas retornam float('nan')
        >>> di_net_spread(
        ...     settlement=None,
        ...     ntnf_maturity="01-01-2035",
        ...     ntnf_rate=0.116586,
        ...     di_expirations=exp_dates,
        ...     di_rates=di_rates,
        ... )
        nan
    """
    # Validação de inputs
    if any_is_empty(settlement, ntnf_maturity, ntnf_rate, di_expirations, di_rates):
        return float("nan")

    # Garante di_rates como Series
    if not isinstance(di_rates, pl.Series):
        taxas_di = pl.Series(di_rates)
    else:
        taxas_di = di_rates

    # Criação do interpolador
    interpolador_ff = ip.Interpolator(
        "flat_forward",
        bday.count(settlement, di_expirations),
        taxas_di,
    )

    # Geração dos fluxos de caixa do NTN-F
    df = cash_flows(settlement, ntnf_maturity)
    if df.is_empty():
        return float("nan")

    dias_uteis_pagamento = bday.count(settlement, df["PaymentDate"])
    anos_uteis_pagamento = dias_uteis_pagamento / 252

    df = df.with_columns(
        BDaysToPayment=dias_uteis_pagamento,
        DIRateInterp=interpolador_ff(dias_uteis_pagamento),
    )

    # Extração dos dados para o cálculo numérico
    preco_titulo = price(settlement, ntnf_maturity, ntnf_rate)
    fluxos_titulo = df["CashFlow"]
    di_interpolada = df["DIRateInterp"]

    # Função de diferença de preço para o solver
    def diferenca_preco(p: float) -> float:
        fluxos_descontados = (
            fluxos_titulo / (1 + di_interpolada + p) ** anos_uteis_pagamento
        )
        return fluxos_descontados.sum() - preco_titulo

    # 7. Resolver para o spread
    return _resolver_spread(diferenca_preco)


def duration(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula a Macaulay duration de uma NTN-F em anos úteis.

    Args:
        settlement (DateLike): Data de liquidação para o cálculo.
        maturity (DateLike): Data de vencimento do título.
        rate (float): Taxa YTM usada para descontar os fluxos.

    Returns:
        float: Macaulay duration em anos úteis. Retorna NaN se inválido.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.duration("02-09-2024", "01-01-2035", 0.121785)
        6.32854218039796

        Entradas nulas retornam NaN:
        >>> ntnf.duration(None, "01-01-2035", 0.121785)
        nan
    """
    if any_is_empty(settlement, maturity, rate):
        return float("nan")

    df_fluxos = cash_flows(settlement, maturity)
    if df_fluxos.is_empty():
        return float("nan")

    anos_uteis = bday.count(settlement, df_fluxos["PaymentDate"]) / 252
    dcf = df_fluxos["CashFlow"] / (1 + rate) ** anos_uteis
    duracao = (dcf * anos_uteis).sum() / dcf.sum()
    return duracao


def dv01(
    settlement: DateLike,
    maturity: DateLike,
    rate: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) de uma NTN-F em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        settlement (DateLike): Data de liquidação.
        maturity (DateLike): Data de vencimento.
        rate (float): Taxa de desconto (YTM) do título.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.dv01("26-03-2025", "01-01-2035", 0.151375)
        0.39025200000003224

        Entradas nulas retornam NaN:
        >>> ntnf.dv01("", "01-01-2035", 0.151375)
        nan
    """
    if any_is_empty(settlement, maturity, rate):
        return float("nan")

    preco_1 = price(settlement, maturity, rate)
    preco_2 = price(settlement, maturity, rate + 0.0001)
    return preco_1 - preco_2


def di_spreads(date: DateLike, bps: bool = False) -> pl.DataFrame:
    """
    Calcula o DI Spread para títulos prefixados (LTN e NTN-F) em uma data de referência.

    Definição do spread (forma bruta):
        DISpread_raw = IndicativeRate - SettlementRate

    Quando ``bps=False`` a coluna retorna essa diferença em formato decimal
    (ex: 0.000439 ≈ 4.39 bps). Quando ``bps=True`` o valor é automaticamente
    multiplicado por 10_000 e exibido diretamente em basis points.

    Args:
        date (DateLike): Data de referência para buscar as taxas.
        bps (bool): Se True, retorna DISpread já convertido em basis points.
            Padrão False.

    Returns:
        pl.DataFrame: DataFrame com as colunas do spread.

    Output Columns:
        * BondType (String): Tipo do título.
        * MaturityDate (Date): Data de vencimento.
        * DISpread (Float64): Spread em decimal ou bps conforme parâmetro.

    Raises:
        ValueError: Se os dados de DI não possuem 'SettlementRate' ou estão vazios.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.di_spreads("30-05-2025", bps=True)
        shape: (5, 3)
        ┌──────────┬──────────────┬──────────┐
        │ BondType ┆ MaturityDate ┆ DISpread │
        │ ---      ┆ ---          ┆ ---      │
        │ str      ┆ date         ┆ f64      │
        ╞══════════╪══════════════╪══════════╡
        │ NTN-F    ┆ 2027-01-01   ┆ -3.31    │
        │ NTN-F    ┆ 2029-01-01   ┆ 14.21    │
        │ NTN-F    ┆ 2031-01-01   ┆ 21.61    │
        │ NTN-F    ┆ 2033-01-01   ┆ 11.51    │
        │ NTN-F    ┆ 2035-01-01   ┆ 22.0     │
        └──────────┴──────────────┴──────────┘
    """
    return pre_di_spreads(date, bps=bps).filter(pl.col("BondType") == "NTN-F")
