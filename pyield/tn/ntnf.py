import datetime as dt
import logging
import math

import polars as pl

import pyield._internal.converters as cv
import pyield.interpolador as ip
from pyield import dus
from pyield._internal.types import ArrayLike, DateLike, any_is_empty
from pyield.b3 import di1
from pyield.tn import utils
from pyield.tn.pre import premio as premio_pre

"""
Constantes calculadas conforme regras da ANBIMA
TAXA_CUPOM = (0.10 + 1) ** 0.5 - 1  -> 10% a.a. com capitalização semestral
VALOR_FACE = 1000
VALOR_CUPOM = round(VALOR_FACE * TAXA_CUPOM, 5)
VALOR_FINAL = VALOR_FACE + VALOR_CUPOM

A NTN-F paga dois cupons por ano (semestrais). As datas de cupom são derivadas
do vencimento (retrocedendo 6 em 6 meses), sem depender de meses fixos.
    Ex.: vencimento 01-01-2027 gera cupons em 01-07-2026, 01-01-2026, ...
"""
VALOR_CUPOM = 48.80885
VALOR_FINAL = 1048.80885  # 1000 + 48.80885

logger = logging.getLogger(__name__)


def dados(data: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de NTN-F para a data de referência.

    Args:
        data (DateLike): Data da consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de NTN-F.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "NTN-F").
        - codigo_selic (Int64): Código do título no SELIC.
        - data_base (Date): Data base de emissão do título.
        - data_vencimento (Date): Data de vencimento do título.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - prazo_medio (Float64): Prazo médio do título em dias corridos.
        - duration (Float64): Macaulay Duration do título (anos).
        - dv01 (Float64): Variação no preço para 1bp de taxa.
        - dv01_usd (Float64): DV01 convertido para USD pela PTAX do dia.
        - pu (Float64): Preço unitário (PU).
        - taxa_compra (Float64): Taxa de compra (decimal).
        - taxa_venda (Float64): Taxa de venda (decimal).
        - taxa_indicativa (Float64): Taxa indicativa (decimal).
        - taxa_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo
            método flat forward.
        - taxa_zero (Float64): Taxa zero (zero cupom via bootstrap).
        - premio (Float64): prêmio, isto é, o spread sobre o DI.
        - premio_limpo (Float64): prêmio limpo sobre a curva DI.
        - rentabilidade (Float64): Rentabilidade da NTN-F sobre a curva DI.

    Examples:
        >>> from pyield.tn import ntnf
        >>> df_ntnf = ntnf.dados("23-08-2024")  # doctest: +SKIP
    """
    df = utils.obter_tpf(data, "NTN-F")
    if df.is_empty():
        return df

    data_ref = cv.converter_datas(data)

    # Adiciona dias_uteis (dado derivado, não vem da ANBIMA)
    df = df.with_columns(
        dias_uteis=dus.contar_expr("data_referencia", "data_vencimento"),
    )

    # Adiciona duration, prazo_medio, dv01, dv01_usd e taxa_di
    df = utils.adicionar_duration(df, duration)
    df = utils.adicionar_dv01(df, data_ref)
    df = utils.adicionar_taxa_di(df, data_ref)

    # Busca dados de LTN para bootstrap das taxas spot
    df_ltn = utils.obter_tpf(data, "LTN").select("data_vencimento", "taxa_indicativa")
    df_spots = taxas_zero(
        data_liquidacao=data,
        ltn_vencimentos=df_ltn["data_vencimento"],
        ltn_taxas=df_ltn["taxa_indicativa"],
        ntnf_vencimentos=df["data_vencimento"],
        ntnf_taxas=df["taxa_indicativa"],
    ).select("data_vencimento", "taxa_zero")
    df = df.join(df_spots, on="data_vencimento", how="left")

    # Busca curva DI para cálculo da rentabilidade
    df_di = di1.dados(data, inicio_mes=True)

    # Calcula prêmios e rentabilidade para cada vencimento
    df = df.with_columns(
        premio=pl.col("taxa_indicativa") - pl.col("taxa_di"),
        premio_limpo=pl.struct("data_vencimento", "taxa_indicativa").map_elements(
            lambda row: premio_limpo(
                data_liquidacao=data,
                data_vencimento=row["data_vencimento"],
                taxa_ntnf=row["taxa_indicativa"],
                vencimentos_di=df_di["data_vencimento"],
                taxas_di=df_di["taxa_ajuste"],
            ),
            return_dtype=pl.Float64,
        ),
        rentabilidade=pl.struct("data_vencimento", "taxa_indicativa").map_elements(
            lambda row: rentabilidade(
                data_liquidacao=data,
                data_vencimento=row["data_vencimento"],
                taxa_ntnf=row["taxa_indicativa"],
                vencimentos_di=df_di["data_vencimento"],  # type: ignore[union-attr]
                taxas_di=df_di["taxa_ajuste"],
            ),
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
        "prazo_medio",
        "duration",
        "dv01",
        "dv01_usd",
        "pu",
        "taxa_compra",
        "taxa_venda",
        "taxa_indicativa",
        "taxa_di",
        "taxa_zero",
        "premio",
        "premio_limpo",
        "rentabilidade",
    )


def vencimentos(data: DateLike) -> pl.Series:
    """
    Busca os vencimentos de NTN-F disponíveis para a data de referência.

    Args:
        data (DateLike): Data da consulta.

    Returns:
        pl.Series: Série de datas de vencimento de NTN-F.

    Examples:
        >>> from pyield.tn import ntnf
        >>> ntnf.vencimentos("23-08-2024")
        shape: (6,)
        Series: 'data_vencimento' [date]
        [
            2025-01-01
            2027-01-01
            2029-01-01
            2031-01-01
            2033-01-01
            2035-01-01
        ]
    """
    return dados(data)["data_vencimento"]


def datas_pagamento(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
) -> pl.Series:
    """
    Gera todas as datas de cupom entre liquidação e vencimento.

    As datas são exclusivas para a liquidação e inclusivas para o vencimento.
    Os cupons são pagos em 1º de janeiro e 1º de julho. O título NTN-F é
    determinado pela data de vencimento.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.

    Returns:
        pl.Series: Série com as datas de cupom entre a liquidação (exclusiva)
            e o vencimento (inclusiva). Retorna série vazia se o vencimento
            for menor ou igual à liquidação.

    Examples:
        >>> from pyield.tn import ntnf
        >>> ntnf.datas_pagamento("15-05-2024", "01-01-2027")
        shape: (6,)
        Series: 'datas_pagamento' [date]
        [
            2024-07-01
            2025-01-01
            2025-07-01
            2026-01-01
            2026-07-01
            2027-01-01
        ]
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return pl.Series(name="datas_pagamento", dtype=pl.Date)
    # Normaliza datas
    liquidacao = cv.converter_datas(data_liquidacao)
    vencimento = cv.converter_datas(data_vencimento)

    # Verifica se vencimento é posterior à liquidação
    if vencimento <= liquidacao:
        return pl.Series(name="datas_pagamento", dtype=pl.Date)

    # Inicializa variáveis do loop
    data_cupom = vencimento
    datas_cupons = []

    # Itera de trás para frente do vencimento até a liquidação
    while data_cupom > liquidacao:
        datas_cupons.append(data_cupom)
        # Retrocede 6 meses
        data_cupom = utils.subtrair_meses(data_cupom, 6)

    return pl.Series(name="datas_pagamento", values=datas_cupons).sort()


def fluxos_caixa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    ajustar_datas_pagamento: bool = False,
) -> pl.DataFrame:
    """
    Gera os fluxos de caixa da NTN-F entre liquidação (exclusiva) e vencimento
    (inclusivo). Os fluxos incluem cupons e o pagamento final no vencimento.

    Args:
        data_liquidacao (DateLike): Data de liquidação (exclusiva).
        data_vencimento (DateLike): Data de vencimento do título.
        ajustar_datas_pagamento (bool): Se True, ajusta as datas de pagamento para o
            próximo dia útil.

    Returns:
        pl.DataFrame: DataFrame com as colunas `data_pagamento` e
            `valor_pagamento`.

    Output Columns:
        - data_pagamento (Date): Data de pagamento.
        - valor_pagamento (Float64): Valor do pagamento.

    Examples:
        >>> from pyield.tn import ntnf
        >>> ntnf.fluxos_caixa("15-05-2024", "01-01-2027")
        shape: (6, 2)
        ┌────────────────┬─────────────────┐
        │ data_pagamento ┆ valor_pagamento │
        │ ---            ┆ ---             │
        │ date           ┆ f64             │
        ╞════════════════╪═════════════════╡
        │ 2024-07-01     ┆ 48.80885        │
        │ 2025-01-01     ┆ 48.80885        │
        │ 2025-07-01     ┆ 48.80885        │
        │ 2026-01-01     ┆ 48.80885        │
        │ 2026-07-01     ┆ 48.80885        │
        │ 2027-01-01     ┆ 1048.80885      │
        └────────────────┴─────────────────┘
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return pl.DataFrame()
    # Normaliza datas de entrada
    liquidacao = cv.converter_datas(data_liquidacao)
    vencimento = cv.converter_datas(data_vencimento)

    # Obtém as datas de pagamento entre liquidação e vencimento
    serie_datas_pagamento = datas_pagamento(liquidacao, vencimento)

    # Retorna DataFrame vazio se não houver pagamentos (liquidação >= vencimento)
    if serie_datas_pagamento.is_empty():
        return pl.DataFrame(
            schema={"data_pagamento": pl.Date, "valor_pagamento": pl.Float64}
        )

    # Define o fluxo final no vencimento e os demais como cupom
    df = pl.DataFrame(
        data={"data_pagamento": serie_datas_pagamento},
    ).with_columns(
        pl.when(pl.col("data_pagamento") == vencimento)
        .then(VALOR_FINAL)
        .otherwise(VALOR_CUPOM)
        .alias("valor_pagamento")
    )

    if ajustar_datas_pagamento:
        df = df.with_columns(data_pagamento=dus.deslocar_expr("data_pagamento", 0))
    return df


def _calcular_pu(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    valores_fluxo = df_fluxos["valor_pagamento"]
    dias_uteis = dus.contar(data_liquidacao, df_fluxos["data_pagamento"])
    anos_uteis = utils.truncar(dias_uteis / 252, 14)
    fatores_desconto = (1 + taxa) ** anos_uteis
    vp = (valores_fluxo / fatores_desconto).round(9)
    return utils.truncar(vp.sum(), 6)


def pu(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    """
    Calcula o preço (PU) da NTN-F pelas regras da ANBIMA, equivalente ao valor
    presente dos fluxos descontados pela TIR informada.

    Args:
        data_liquidacao (DateLike): Data de liquidação para cálculo do preço.
        data_vencimento (DateLike): Data de vencimento do título.
        taxa (float): Taxa de desconto (TIR) usada para calcular o valor presente.

    Returns:
        float: Preço da NTN-F conforme ANBIMA.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - O cupom semestral é 48,81, que representa 10% a.a. com capitalização
          semestral e arredondamento para 5 casas, conforme ANBIMA.

    Examples:
        >>> from pyield.tn import ntnf
        >>> ntnf.pu("05-07-2024", "01-01-2035", 0.11921)
        895.359254
    """
    return _calcular_pu(data_liquidacao, data_vencimento, taxa)


def taxas_zero(  # noqa
    data_liquidacao: DateLike,
    ltn_vencimentos: ArrayLike,
    ltn_taxas: ArrayLike,
    ntnf_vencimentos: ArrayLike,
    ntnf_taxas: ArrayLike,
    incluir_cupons: bool = False,
) -> pl.DataFrame:
    """
    Calcula as taxas spot (zero cupom) para NTN-F usando bootstrap.

    O bootstrap determina as taxas spot a partir dos yields dos títulos.
    O método resolve iterativamente as taxas que descontam os fluxos ao preço.
    Usa as LTNs (zero cupom) até o último vencimento LTN disponível. Após
    isso, calcula as taxas spot a partir das NTN-F.


    Args:
        data_liquidacao (DateLike): Data de liquidação para o cálculo.
        ltn_vencimentos (ArrayLike): Vencimentos conhecidos de LTN.
        ltn_taxas (ArrayLike): Taxas conhecidas de LTN.
        ntnf_vencimentos (ArrayLike): Vencimentos conhecidos de NTN-F.
        ntnf_taxas (ArrayLike): Taxas conhecidas de NTN-F.
        incluir_cupons (bool): Se True, inclui as datas de cupom (julho).
            Padrão False.

    Returns:
        pl.DataFrame: DataFrame com colunas `data_vencimento`, `dias_uteis`
            e `taxa_zero`.

    Output Columns:
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre liquidação e vencimento.
        - taxa_zero (Float64): Taxa zero (zero cupom).

    Examples:
        >>> from pyield.tn import ntnf, ltn
        >>> df_ltn = ltn.dados("03-09-2024")
        >>> df_ntnf = ntnf.dados("03-09-2024")
        >>> ntnf.taxas_zero(
        ...     data_liquidacao="03-09-2024",
        ...     ltn_vencimentos=df_ltn["data_vencimento"],
        ...     ltn_taxas=df_ltn["taxa_indicativa"],
        ...     ntnf_vencimentos=df_ntnf["data_vencimento"],
        ...     ntnf_taxas=df_ntnf["taxa_indicativa"],
        ... )
        shape: (6, 3)
        ┌─────────────────┬────────────┬───────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_zero │
        │ ---             ┆ ---        ┆ ---       │
        │ date            ┆ i64        ┆ f64       │
        ╞═════════════════╪════════════╪═══════════╡
        │ 2025-01-01      ┆ 83         ┆ 0.108837  │
        │ 2027-01-01      ┆ 584        ┆ 0.119981  │
        │ 2029-01-01      ┆ 1083       ┆ 0.122113  │
        │ 2031-01-01      ┆ 1584       ┆ 0.122231  │
        │ 2033-01-01      ┆ 2088       ┆ 0.121355  │
        │ 2035-01-01      ┆ 2587       ┆ 0.121398  │
        └─────────────────┴────────────┴───────────┘
    """
    if any_is_empty(
        data_liquidacao,
        ltn_vencimentos,
        ltn_taxas,
        ntnf_vencimentos,
        ntnf_taxas,
    ):
        return pl.DataFrame()
    # 1. Converter e normalizar inputs para Polars
    liquidacao = cv.converter_datas(data_liquidacao)
    ltn_vencimentos = cv.converter_datas(ltn_vencimentos)
    ntnf_vencimentos = cv.converter_datas(ntnf_vencimentos)
    if not isinstance(ltn_taxas, pl.Series):
        serie_ltn_taxas = pl.Series(ltn_taxas).cast(pl.Float64)
    else:
        serie_ltn_taxas = ltn_taxas
    if not isinstance(ntnf_taxas, pl.Series):
        serie_ntnf_taxas = pl.Series(ntnf_taxas).cast(pl.Float64)
    else:
        serie_ntnf_taxas = ntnf_taxas

    # 2. Criar interpoladores (aceitam pl.Series diretamente)
    interpolador_ltn = ip.Interpolador(
        dias_uteis=dus.contar(liquidacao, ltn_vencimentos),
        taxas=serie_ltn_taxas,
        metodo="flat_forward",
    )
    interpolador_ntnf = ip.Interpolador(
        dias_uteis=dus.contar(liquidacao, ntnf_vencimentos),
        taxas=serie_ntnf_taxas,
        metodo="flat_forward",
    )

    # 3. Gerar todas as datas de cupom até o último vencimento NTN-F
    ultimo_vencimento = ntnf_vencimentos.max()
    assert isinstance(ultimo_vencimento, dt.date)
    todas_datas_cupom = datas_pagamento(liquidacao, ultimo_vencimento)

    # 4. Construir DataFrame inicial
    dias_uteis_ate_venc = dus.contar(liquidacao, todas_datas_cupom)
    taxas_tir = interpolador_ntnf(dias_uteis_ate_venc)
    df = pl.DataFrame(
        {
            "data_vencimento": todas_datas_cupom,
            "dias_uteis": dias_uteis_ate_venc,
            "anos_uteis": dias_uteis_ate_venc / 252,
            "taxa_tir": taxas_tir,
        }
    ).with_columns(
        cupom=pl.lit(VALOR_CUPOM),
    )

    # 5. Loop de bootstrap (iterativo por dependência sequencial)
    ultimo_vencimento_ltn = ltn_vencimentos.max()
    assert isinstance(ultimo_vencimento_ltn, dt.date)

    lista_vencimentos = df["data_vencimento"]
    lista_dias_uteis = df["dias_uteis"]
    lista_anos_uteis = df["anos_uteis"]
    lista_tir = df["taxa_tir"]

    taxas_spot_resolvidas: list[float | None] = []
    mapa_spot: dict[dt.date, float | None] = {}

    for i in range(len(df)):
        data_venc = lista_vencimentos[i]
        assert isinstance(data_venc, dt.date)
        dias_uteis_val = int(lista_dias_uteis[i])
        anos_uteis_val = float(lista_anos_uteis[i])
        tir_val = float(lista_tir[i])

        # Caso esteja antes (ou igual) ao último vencimento LTN: usar interpolador LTN
        if data_venc <= ultimo_vencimento_ltn:
            taxa_zero = interpolador_ltn(dias_uteis_val)
            taxas_spot_resolvidas.append(taxa_zero)
            mapa_spot[data_venc] = taxa_zero
            continue

        # Datas de cupom (exclui último pagamento) para este vencimento
        datas_fluxo = datas_pagamento(liquidacao, data_venc)[:-1]
        if len(datas_fluxo) == 0:
            # Caso improvável, mas protege contra divisão por zero mais adiante
            taxa_zero = None
            taxas_spot_resolvidas.append(taxa_zero)
            mapa_spot[data_venc] = taxa_zero
            continue

        # Recupera taxas spot já solucionadas para estes cupons
        taxas_spot_fluxo = [mapa_spot[d] for d in datas_fluxo]
        periodos_fluxo = dus.contar(liquidacao, datas_fluxo) / 252
        fluxos = [VALOR_CUPOM] * len(datas_fluxo)

        valor_presente_fluxo = utils.calcular_pv(
            fluxos_caixa=pl.Series(fluxos),
            taxas=pl.Series(taxas_spot_fluxo),
            prazos=periodos_fluxo,
        )

        preco_titulo = _calcular_pu(liquidacao, data_venc, tir_val)
        fator_preco = VALOR_FINAL / (preco_titulo - valor_presente_fluxo)
        taxa_zero = fator_preco ** (1 / anos_uteis_val) - 1

        taxas_spot_resolvidas.append(taxa_zero)
        mapa_spot[data_venc] = taxa_zero

    # 6. Anexa a coluna taxa_zero
    df = df.with_columns(taxa_zero=pl.Series(taxas_spot_resolvidas, dtype=pl.Float64))

    # 7. Selecionar colunas finais
    df = df.select(["data_vencimento", "dias_uteis", "taxa_zero"])

    # 8. Remover cupons (Julho) se não solicitado
    if not incluir_cupons:
        df = df.filter(pl.col("data_vencimento").is_in(ntnf_vencimentos.implode()))

    return df


def rentabilidade(  # noqa
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa_ntnf: float,
    vencimentos_di: DateLike,
    taxas_di: ArrayLike,
) -> float:
    """
    Calcula a rentabilidade de uma NTN-F sobre a curva DI.

    A função compara o fator de desconto implícito da NTN-F com o da curva DI,
    determinando quanto a NTN-F rende em relação ao DI. Interpola as taxas DI nas datas
    de pagamento e calcula o valor presente dos fluxos da NTN-F usando essas taxas.
    Encontra a TIR da curva DI que iguala o preço da NTN-F.

    Args:
        data_liquidacao (DateLike): Data de liquidação para o cálculo.
        data_vencimento (DateLike): Data de vencimento da NTN-F.
        taxa_ntnf (float): TIR da NTN-F.
        vencimentos_di (DateLike): Datas de vencimento da curva DI.
        taxas_di (ArrayLike): Taxas DI correspondentes aos vencimentos.

    Returns:
        float: Rentabilidade da NTN-F sobre a curva DI. Retorna NaN em erro.

    Examples:
        >>> # Obs: apenas algumas taxas DI serão usadas no exemplo.
        >>> exp_dates = ["2025-01-01", "2030-01-01", "2035-01-01"]
        >>> taxas_di = [0.10823, 0.11594, 0.11531]
        >>> rentabilidade(
        ...     data_liquidacao="23-08-2024",
        ...     data_vencimento="01-01-2035",
        ...     taxa_ntnf=0.116586,
        ...     vencimentos_di=exp_dates,
        ...     taxas_di=taxas_di,
        ... )
        1.0099602679927115

    Notes:
        A função ajusta as datas de pagamento para dias úteis e calcula o valor
        presente dos fluxos da NTN-F usando as taxas DI.

    """
    if any_is_empty(
        data_liquidacao,
        data_vencimento,
        taxa_ntnf,
        vencimentos_di,
        taxas_di,
    ):
        return float("nan")

    if not isinstance(taxas_di, pl.Series):
        serie_taxas_di = pl.Series(taxas_di)
    else:
        serie_taxas_di = taxas_di

    df_fluxos = fluxos_caixa(
        data_liquidacao,
        data_vencimento,
        ajustar_datas_pagamento=True,
    )
    if df_fluxos.is_empty():
        return float("nan")

    interpolador_ff = ip.Interpolador(
        dus.contar(data_liquidacao, vencimentos_di),  # type: ignore[arg-type]
        serie_taxas_di,
        "flat_forward",
    )

    dias_uteis_pagamento = dus.contar(data_liquidacao, df_fluxos["data_pagamento"])
    df = df_fluxos.with_columns(
        dias_uteis=dias_uteis_pagamento,
        anos_uteis=dias_uteis_pagamento / 252,
        taxa_di=interpolador_ff(dias_uteis_pagamento),
    )

    preco_titulo = utils.calcular_pv(
        fluxos_caixa=df["valor_pagamento"],
        taxas=df["taxa_di"],
        prazos=df["anos_uteis"],
    )

    if math.isnan(preco_titulo):
        return float("nan")

    def diferenca_preco(taxa: float) -> float:
        fluxos_descontados = df["valor_pagamento"] / (1 + taxa) ** df["anos_uteis"]
        return float(fluxos_descontados.sum()) - preco_titulo

    di_tir = utils.encontrar_raiz(diferenca_preco)

    if math.isnan(di_tir):
        return float("nan")

    fator_ntnf = (1 + taxa_ntnf) ** (1 / 252)
    fator_di = (1 + di_tir) ** (1 / 252)
    if fator_di == 1:
        return float("inf") if fator_ntnf > 1 else 0.0

    rentabilidade = (fator_ntnf - 1) / (fator_di - 1)
    return rentabilidade


def premio(data: DateLike, pontos_base: bool = False) -> pl.DataFrame:
    """
    Calcula o prêmio bruto das NTN-F sobre a curva DI na data de referência.

    Definição do prêmio (forma bruta):
        premio = taxa_indicativa - taxa de ajuste do DI

    Quando ``pontos_base=False`` a coluna retorna essa diferença em formato decimal
    (ex: 0.000439 ≈ 4.39 bps). Quando ``pontos_base=True`` o valor é automaticamente
    multiplicado por 10_000 e exibido diretamente em basis points.

    Args:
        data (DateLike): Data da consulta para buscar as taxas.
        pontos_base (bool): Se True, retorna o prêmio já convertido em basis points.
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
        >>> from pyield.tn import ntnf
        >>> ntnf.premio("30-05-2025", pontos_base=True)
        shape: (5, 3)
        ┌────────┬─────────────────┬────────┐
        │ titulo ┆ data_vencimento ┆ premio │
        │ ---    ┆ ---             ┆ ---    │
        │ str    ┆ date            ┆ f64    │
        ╞════════╪═════════════════╪════════╡
        │ NTN-F  ┆ 2027-01-01      ┆ -3.31  │
        │ NTN-F  ┆ 2029-01-01      ┆ 14.21  │
        │ NTN-F  ┆ 2031-01-01      ┆ 21.61  │
        │ NTN-F  ┆ 2033-01-01      ┆ 11.51  │
        │ NTN-F  ┆ 2035-01-01      ┆ 22.0   │
        └────────┴─────────────────┴────────┘
    """
    return premio_pre(data, pontos_base=pontos_base).filter(pl.col("titulo") == "NTN-F")


def premio_limpo(  # noqa
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa_ntnf: float,
    vencimentos_di: ArrayLike,
    taxas_di: ArrayLike,
) -> float:
    """
    Calcula o spread líquido (prêmio limpo no jargão de mercado) da NTN-F sobre a curva DI.

    A função determina o spread que iguala o valor presente dos fluxos ao preço
    do título. Interpola as taxas DI nas datas de pagamento e encontra o spread
    (em bps) que zera a diferença de preços.

    Args:
        data_liquidacao (DateLike): Data de liquidação para o cálculo.
        data_vencimento (DateLike): Data de vencimento do título.
        taxa_ntnf (float): TIR do título.
        taxas_di (ArrayLike): Série de taxas DI.
        vencimentos_di (ArrayLike): Vencimentos da curva DI.

    Returns:
        float: Spread líquido em formato decimal (ex.: 0.0012 = 12 bps).
            Retorna NaN em caso de erro.

    Examples:
        # Obs: apenas algumas taxas DI serão usadas no exemplo.
        >>> exp_dates = ["2025-01-01", "2030-01-01", "2035-01-01"]
        >>> taxas_di = [0.10823, 0.11594, 0.11531]
        >>> spread = premio_limpo(
        ...     data_liquidacao="23-08-2024",
        ...     data_vencimento="01-01-2035",
        ...     taxa_ntnf=0.116586,
        ...     vencimentos_di=exp_dates,
        ...     taxas_di=taxas_di,
        ... )
        >>> round(spread * 10_000, 2)  # Converte para bps para exibição
        12.13
    """
    if any_is_empty(
        data_liquidacao,
        data_vencimento,
        taxa_ntnf,
        vencimentos_di,
        taxas_di,
    ):
        return float("nan")

    if not isinstance(taxas_di, pl.Series):
        serie_taxas_di = pl.Series(taxas_di)
    else:
        serie_taxas_di = taxas_di

    interpolador_ff = ip.Interpolador(
        dus.contar(data_liquidacao, vencimentos_di),
        serie_taxas_di,
        "flat_forward",
    )

    df = fluxos_caixa(data_liquidacao, data_vencimento)
    if df.is_empty():
        return float("nan")

    dias_uteis_pagamento = dus.contar(data_liquidacao, df["data_pagamento"])
    anos_uteis_pagamento = dias_uteis_pagamento / 252

    df = df.with_columns(
        dias_uteis_pagamento=dias_uteis_pagamento,
        taxa_di_interpolada=interpolador_ff(dias_uteis_pagamento),
    )

    preco_titulo = _calcular_pu(data_liquidacao, data_vencimento, taxa_ntnf)
    fluxos_titulo = df["valor_pagamento"]
    di_interpolada = df["taxa_di_interpolada"]

    def diferenca_preco(p: float) -> float:
        fluxos_descontados = (
            fluxos_titulo / (1 + di_interpolada + p) ** anos_uteis_pagamento
        )
        return float(fluxos_descontados.sum()) - preco_titulo

    return utils.encontrar_raiz(diferenca_preco)


def duration(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    """
    Calcula a Macaulay duration de uma NTN-F em anos úteis.

    Args:
        data_liquidacao (DateLike): Data de liquidação para o cálculo.
        data_vencimento (DateLike): Data de vencimento do título.
        taxa (float): TIR usada para descontar os fluxos.

    Returns:
        float: Macaulay duration em anos úteis. Retorna NaN se inválido.

    Examples:
        >>> from pyield.tn import ntnf
        >>> ntnf.duration("02-09-2024", "01-01-2035", 0.121785)
        6.32854218039796
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    anos_uteis = dus.contar(data_liquidacao, df_fluxos["data_pagamento"]) / 252
    vp = df_fluxos["valor_pagamento"] / (1 + taxa) ** anos_uteis
    duration = float((vp * anos_uteis).sum()) / float(vp.sum())
    return duration


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) de uma NTN-F em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float): Taxa de desconto (TIR) do título.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield.tn import ntnf
        >>> ntnf.dv01("26-03-2025", "01-01-2035", 0.151375)
        0.39025200000003224
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    preco_1 = _calcular_pu(data_liquidacao, data_vencimento, taxa)
    preco_2 = _calcular_pu(data_liquidacao, data_vencimento, taxa + 0.0001)
    return preco_1 - preco_2


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    pu: float,
) -> float:
    """
    Calcula a TIR implícita de uma NTN-F a partir de um PU informado.

    A função inverte numericamente o cálculo de ``pu()``, encontrando a taxa
    que zera a diferença entre o preço calculado e o preço desejado.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        pu (float): Preço unitário (PU) do título.

    Returns:
        float: TIR implícita em formato decimal. Retorna NaN em caso de erro.

    Examples:
        >>> from pyield.tn import ntnf
        >>> pu = ntnf.pu("05-07-2024", "01-01-2035", 0.11921)
        >>> ntnf.taxa("13-03-2026", "01-01-2035", 820.995125)
        0.142743
    """
    if any_is_empty(data_liquidacao, data_vencimento, pu):
        return float("nan")

    if pu <= 0:
        return float("nan")

    def diferenca_preco(taxa_encontrada: float) -> float:
        return _calcular_pu(data_liquidacao, data_vencimento, taxa_encontrada) - pu

    taxa_encontrada = utils.encontrar_raiz(diferenca_preco)
    return round(taxa_encontrada, 6)
