import datetime as dt
import logging

import polars as pl

import pyield._internal.converters as conversores
from pyield import du, fwd, interpolador
from pyield._internal.types import ArrayLike, DateLike, DatesLike, any_is_empty
from pyield.tpf.titulos import _utils as utils

"""
Constantes calculadas conforme regras da ANBIMA e em base 1.
TAXA_CUPOM = (0.06 + 1) ** 0.5 - 1  # 6% a.a. com capitalização semestral
VALOR_CUPOM = round(TAXA_CUPOM, 8) -> 0.02956301
VALOR_FINAL = principal + último cupom = 1 + 0.02956301
DIA_CUPOM = 15
MESES_CUPOM = {2, 5, 8, 11}
"""
VALOR_CUPOM = 0.02956301
VALOR_FINAL = 1.02956301

logger = logging.getLogger(__name__)


def dados(data: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de NTN-B para a data de referência.

    Args:
        data (DateLike): Data da consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de NTN-B.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "NTN-B").
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
        - taxa_zero (Float64): Taxa zero real (via bootstrap das taxas indicativas).
        - taxa_forward (Float64): Taxa forward real (a partir das taxas zero).
        - inflacao_implicita (Float64): Inflação implícita (breakeven) calculada
            a partir de taxas nominais do DI Futuro e taxas zero das NTN-B.

    Examples:
        >>> from pyield import ntnb
        >>> df_ntnb = ntnb.dados("23-08-2024")  # doctest: +SKIP
    """
    from pyield.futuro import di1  # noqa: PLC0415

    df = utils.obter_tpf(data, "NTN-B")
    if df.is_empty():
        return df

    # Adiciona duration, prazo_medio e dv01
    df = df.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
        duration=duration_expr("data_referencia", "data_vencimento", "taxa_indicativa"),
    ).with_columns(
        prazo_medio=pl.col("duration"),
        dv01=dv01_expr("data_referencia", "data_vencimento", "taxa_indicativa", "pu"),
    )

    # Busca curva DI bruta e calcula taxa_zero, taxa_di e inflação implícita
    df_di = di1.dados(data)
    df_bei = implicitas(
        data_liquidacao=data,
        vencimentos_tir=df["data_vencimento"],
        taxas_tir=df["taxa_indicativa"],
        vencimentos_nominais=df_di["data_vencimento"],
        taxas_nominais=df_di["taxa_ajuste"],
    ).select(
        pl.col("data_vencimento"),
        pl.col("taxa_zero_real").alias("taxa_zero"),
        pl.col("taxa_nominal").alias("taxa_di"),
        pl.col("inflacao_implicita"),
    )

    df = df.join(df_bei, on="data_vencimento", how="left")

    # Calcula taxas forward a partir das taxas zero
    df = df.with_columns(taxa_forward=fwd.forwards_expr("dias_uteis", "taxa_zero"))

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
        "taxa_zero",
        "taxa_forward",
        "inflacao_implicita",
    )


def vencimentos(data: DateLike) -> pl.Series:
    """
    Busca os vencimentos de NTN-B disponíveis para a data de referência.

    Args:
        data (DateLike): Data da consulta.

    Returns:
        pl.Series: Série de datas de vencimento de NTN-B.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.vencimentos("16-08-2024")
        shape: (14,)
        Series: 'data_vencimento' [date]
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
    return dados(data)["data_vencimento"]


def datas_pagamento(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
) -> pl.Series:
    """
    Gera todas as datas de pagamento entre liquidação e vencimento.

    Os pagamentos são semestrais e seguem a série do título, com datas em
    15/02, 15/05, 15/08 ou 15/11. No vencimento, o fluxo inclui o último cupom
    e a amortização do principal.

    Args:
        data_liquidacao (DateLike): Data de liquidação (exclusiva).
        data_vencimento (DateLike): Data de vencimento.

    Returns:
        pl.Series: Série de datas de pagamento entre a liquidação (exclusiva)
            e o vencimento (inclusivo). Retorna série vazia se o vencimento for
            menor ou igual à liquidação.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.datas_pagamento("10-05-2024", "15-05-2025")
        shape: (3,)
        Series: 'datas_pagamento' [date]
        [
            2024-05-15
            2024-11-15
            2025-05-15
        ]

        A data de liquidação coincidente com um pagamento é exclusiva:

        >>> ntnb.datas_pagamento("15-05-2024", "15-05-2025")
        shape: (2,)
        Series: 'datas_pagamento' [date]
        [
            2024-11-15
            2025-05-15
        ]
    """
    return utils.gerar_datas_pagamento(data_liquidacao, data_vencimento)


def fluxos_caixa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
) -> pl.DataFrame:
    """
    Gera os fluxos de caixa da NTN-B entre liquidação e vencimento.

    Args:
        data_liquidacao (DateLike): Data de liquidação (exclusiva).
        data_vencimento (DateLike): Data de vencimento.

    Returns:
        pl.DataFrame: DataFrame com as colunas de fluxo.

    Output Columns:
        - data_pagamento (Date): Data de pagamento.
        - valor_pagamento (Float64): Valor do pagamento em base 1.

    Examples:
        >>> from pyield import ntnb
        >>> with pl.Config(float_precision=8):
        ...     ntnb.fluxos_caixa("10-05-2024", "15-05-2025")
        shape: (3, 2)
        ┌────────────────┬─────────────────┐
        │ data_pagamento ┆ valor_pagamento │
        │ ---            ┆ ---             │
        │ date           ┆ f64             │
        ╞════════════════╪═════════════════╡
        │ 2024-05-15     ┆ 0.02956301      │
        │ 2024-11-15     ┆ 0.02956301      │
        │ 2025-05-15     ┆ 1.02956301      │
        └────────────────┴─────────────────┘
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return pl.DataFrame(
            schema={"data_pagamento": pl.Date, "valor_pagamento": pl.Float64}
        )

    # Obtém as datas de pagamento entre liquidação e vencimento
    liquidacao = conversores.converter_datas(data_liquidacao)
    vencimento = conversores.converter_datas(data_vencimento)
    serie_datas_pagamento = datas_pagamento(liquidacao, vencimento)

    # Retorna DataFrame vazio se não houver pagamentos (liquidação >= vencimento)
    if serie_datas_pagamento.is_empty():
        return pl.DataFrame(
            schema={"data_pagamento": pl.Date, "valor_pagamento": pl.Float64}
        )

    df = pl.DataFrame(
        {"data_pagamento": serie_datas_pagamento},
    ).with_columns(
        pl.when(pl.col("data_pagamento") == vencimento)
        .then(VALOR_FINAL)
        .otherwise(VALOR_CUPOM)
        .alias("valor_pagamento")
    )

    return df


def cotacao(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
) -> float:
    """
    Calcula a cotação da NTN-B em base 1 pelas regras da ANBIMA.

    Args:
        data_liquidacao (DateLike): Data de liquidação da operação.
        data_vencimento (DateLike): Data de vencimento da NTN-B.
        taxa (float): Taxa de desconto (TIR) usada no valor presente.

    Returns:
        float: Cotação da NTN-B truncada em 6 casas. Retorna NaN em erro.

    Notes:
        A ANBIMA apresenta a cotação na escala percentual (base 100). Esta
        função retorna o fator equivalente em base 1, usado diretamente no
        cálculo do PU. O truncamento de 4 casas na escala ANBIMA equivale ao
        truncamento de 6 casas nesta representação.

        O cupom semestral divulgado pela ANBIMA como 2,956301% é armazenado
        como 0,02956301 em base 1.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)
        0.993651
        >>> ntnb.cotacao("31-05-2024", "15-08-2060", 0.061878)
        0.995341
        >>> ntnb.cotacao("15-08-2024", "15-08-2032", 0.05929)
        1.006409
        >>> ntnb.cotacao("15-05-2024", "15-05-2025", 0.10)
        0.964454
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    valores_fluxo = df_fluxos["valor_pagamento"]
    dias_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"])
    anos_uteis = utils.truncar(dias_uteis / 252, 14)
    fatores_desconto = (1 + taxa) ** anos_uteis
    # Calcula o valor presente de cada fluxo com arredondamento ANBIMA
    vp = (valores_fluxo / fatores_desconto).round(12)
    # Retorna a cotação (soma dos valores presentes) com truncamento ANBIMA
    return utils.truncar(vp.sum(), 6)


def _calcular_pu(
    vna: float,
    cotacao: float,
) -> float:
    if any_is_empty(vna, cotacao):
        return float("nan")
    return utils.truncar(vna * cotacao, 6)


def pu(
    vna: float,
    cotacao: float,
) -> float:
    """
    Calcula o preço (PU) da NTN-B pelas regras da ANBIMA.

    Args:
        vna (float): Valor nominal atualizado (VNA).
        cotacao (float): Cotação da NTN-B em base 1.

    Returns:
        float: Preço da NTN-B truncado em 6 casas decimais.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.pu(4299.160173, 0.993651)
        4271.864805
        >>> ntnb.pu(4315.498383, 1.006409)
        4343.156412
    """
    return _calcular_pu(vna, cotacao)


def _validar_entradas_taxas_zero(
    data_liquidacao: DateLike,
    vencimentos: DatesLike,
    taxas: ArrayLike,
) -> tuple[dt.date, pl.Series, pl.Series]:
    # Processa e valida os dados de entrada
    liquidacao = conversores.converter_datas(data_liquidacao)
    vencimentos = conversores.converter_datas(vencimentos)

    # Validação estrutural: vencimentos e taxas precisam ter o mesmo tamanho
    if len(vencimentos) != len(taxas):
        raise ValueError(
            "Vencimentos e taxas devem ter o mesmo tamanho. "
            f"Recebido: {len(vencimentos)} vencimentos e {len(taxas)} taxas."
        )

    # Cria DataFrame base e filtra vencimentos inválidos
    df_limpo = pl.DataFrame(
        data={"vencimentos": vencimentos, "taxas": taxas},
        schema={"vencimentos": pl.Date, "taxas": pl.Float64},
    ).filter(pl.col("vencimentos") > liquidacao)

    # Aviso sobre vencimentos filtrados
    total_filtrados = len(vencimentos) - df_limpo.height
    if total_filtrados > 0:
        logger.warning(
            "Vencimentos menores ou iguais à liquidação foram ignorados: %s removidos.",
            total_filtrados,
        )

    return liquidacao, df_limpo["vencimentos"], df_limpo["taxas"]


def _criar_df_bootstrap(
    data_liquidacao: dt.date,
    taxas: pl.Series,
    vencimentos: pl.Series,
) -> pl.DataFrame:
    """Cria o DataFrame base para o bootstrap."""
    # Cria interpolador para TIRs em datas intermediárias
    interpolador_ff = interpolador.Interpolador(
        dias_uteis=du.contar(data_liquidacao, vencimentos),
        taxas=taxas,
        metodo="flat_forward",
    )

    # Gera datas de pagamento até o último vencimento
    ultimo_vencimento = vencimentos.max()
    assert isinstance(ultimo_vencimento, dt.date)
    todas_datas_pagamento = utils.gerar_datas_pagamento(
        data_liquidacao,
        ultimo_vencimento,
        intervalo_meses=3,
    )

    return (
        pl.DataFrame({"data_vencimento": todas_datas_pagamento})
        .with_columns(dias_uteis=du.contar_expr(data_liquidacao, "data_vencimento"))
        .with_columns(
            anos_uteis=pl.col("dias_uteis") / 252,
            taxa_tir=interpolador_ff.interpolar_expr("dias_uteis"),
            cupom=pl.lit(VALOR_CUPOM),
            taxa_zero=pl.lit(None, dtype=pl.Float64),
        )
        .sort("data_vencimento")
    )


def _atualizar_taxa_zero(
    df: pl.DataFrame, vencimento: dt.date, taxa_zero: float
) -> pl.DataFrame:
    """Atualiza a taxa zero dentro do loop de bootstrap."""
    return df.with_columns(
        pl.when(pl.col("data_vencimento") == vencimento)
        .then(taxa_zero)
        .otherwise("taxa_zero")
        .alias("taxa_zero")
    )


def _calcular_valor_presente_cupons(
    df: pl.DataFrame,
    data_liquidacao: dt.date,
    vencimento: dt.date,
) -> float:
    """Calcula o valor presente dos cupons anteriores à maturidade."""
    datas_fluxo_anteriores = datas_pagamento(data_liquidacao, vencimento).to_list()[:-1]
    df_temp = df.filter(pl.col("data_vencimento").is_in(datas_fluxo_anteriores))

    return utils.calcular_pv(
        fluxos_caixa=df_temp["cupom"],
        taxas=df_temp["taxa_zero"],
        prazos=df_temp["anos_uteis"],
    )


def taxas_zero(
    data_liquidacao: DateLike,
    vencimentos: DatesLike,
    taxas: ArrayLike,
    incluir_cupons: bool = False,
) -> pl.DataFrame:
    """
    Calcula as taxas zero da NTN-B usando bootstrap.

    O bootstrap determina as taxas zero a partir dos yields dos títulos,
    resolvendo iterativamente as taxas que descontam os fluxos ao preço.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        vencimentos (DatesLike): Datas de vencimento dos títulos.
        taxas (ArrayLike): TIRs correspondentes.
        incluir_cupons (bool, optional): Se True, inclui datas intermediárias de cupom.
            Padrão False.

    Returns:
        pl.DataFrame: DataFrame com as taxas zero.

    Output Columns:
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre liquidação e vencimento.
        - taxa_zero (Float64): Taxa zero (real).

    Examples:
        >>> from pyield import ntnb
        >>> # Busca as taxas de NTN-B para uma data de referência
        >>> df = ntnb.dados("16-08-2024")
        >>> # Calcula as taxas zero considerando a liquidação na data de referência
        >>> ntnb.taxas_zero(
        ...     data_liquidacao="16-08-2024",
        ...     vencimentos=df["data_vencimento"],
        ...     taxas=df["taxa_indicativa"],
        ... )
        shape: (14, 3)
        ┌─────────────────┬────────────┬───────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_zero │
        │ ---             ┆ ---        ┆ ---       │
        │ date            ┆ i64        ┆ f64       │
        ╞═════════════════╪════════════╪═══════════╡
        │ 2025-05-15      ┆ 185        ┆ 0.063893  │
        │ 2026-08-15      ┆ 502        ┆ 0.066141  │
        │ 2027-05-15      ┆ 687        ┆ 0.064087  │
        │ 2028-08-15      ┆ 1002       ┆ 0.063057  │
        │ 2029-05-15      ┆ 1186       ┆ 0.061458  │
        │ …               ┆ …          ┆ …         │
        │ 2040-08-15      ┆ 4009       ┆ 0.058326  │
        │ 2045-05-15      ┆ 5196       ┆ 0.060371  │
        │ 2050-08-15      ┆ 6511       ┆ 0.060772  │
        │ 2055-05-15      ┆ 7700       ┆ 0.059909  │
        │ 2060-08-15      ┆ 9017       ┆ 0.060652  │
        └─────────────────┴────────────┴───────────┘

        Caso a liquidação ocorra logo após uma data de cupom, o valor presente
        dos cupons anteriores pode ser zero:
        >>> df = ntnb.dados("15-05-2026")
        >>> ntnb.taxas_zero(
        ...     data_liquidacao="18-05-2026",
        ...     vencimentos=df["data_vencimento"],
        ...     taxas=df["taxa_indicativa"],
        ...     incluir_cupons=True,
        ... )
        shape: (137, 3)
        ┌─────────────────┬────────────┬───────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_zero │
        │ ---             ┆ ---        ┆ ---       │
        │ date            ┆ i64        ┆ f64       │
        ╞═════════════════╪════════════╪═══════════╡
        │ 2026-08-15      ┆ 64         ┆ 0.102013  │
        │ 2026-11-15      ┆ 126        ┆ 0.088186  │
        │ 2027-02-15      ┆ 186        ┆ 0.083431  │
        │ 2027-05-15      ┆ 249        ┆ 0.081096  │
        │ 2027-08-15      ┆ 313        ┆ 0.081005  │
        │ …               ┆ …          ┆ …         │
        │ 2059-08-15      ┆ 8326       ┆ 0.070508  │
        │ 2059-11-15      ┆ 8392       ┆ 0.070524  │
        │ 2060-02-15      ┆ 8454       ┆ 0.07053   │
        │ 2060-05-15      ┆ 8515       ┆ 0.070547  │
        │ 2060-08-15      ┆ 8579       ┆ 0.070553  │
        └─────────────────┴────────────┴───────────┘

    Notes:
        O cálculo considera:
        - Mapear todas as datas de pagamento até o último vencimento.
        - Interpolar as TIRs nas datas intermediárias.
        - Calcular a cotação da NTN-B para cada vencimento.
        - Calcular as taxas zero reais.
    """
    if any_is_empty(data_liquidacao, vencimentos, taxas):
        return pl.DataFrame()

    data_liquidacao, vencimentos, taxas = _validar_entradas_taxas_zero(
        data_liquidacao, vencimentos, taxas
    )

    df = _criar_df_bootstrap(data_liquidacao, taxas, vencimentos)

    # Bootstrap para calcular taxas zero
    linhas = df.to_dicts()
    primeiro_vencimento = vencimentos.min()
    for linha in linhas:
        vencimento = linha["data_vencimento"]

        # Taxas zero <= primeiro vencimento são TIR por definição
        if vencimento <= primeiro_vencimento:
            taxa_zero = linha["taxa_tir"]
            df = _atualizar_taxa_zero(df, vencimento, taxa_zero)
            continue

        # Calcula taxa zero para o vencimento corrente
        valor_presente_cupons = _calcular_valor_presente_cupons(
            df, data_liquidacao, vencimento
        )
        preco_titulo = cotacao(data_liquidacao, vencimento, linha["taxa_tir"])
        fator_preco = VALOR_FINAL / (preco_titulo - valor_presente_cupons)
        taxa_zero = fator_preco ** (1 / linha["anos_uteis"]) - 1

        df = _atualizar_taxa_zero(df, vencimento, taxa_zero)

    if not incluir_cupons:
        df = df.filter(pl.col("data_vencimento").is_in(vencimentos.to_list()))
    return df.select(["data_vencimento", "dias_uteis", "taxa_zero"])


def implicitas(  # noqa: PLR0913
    data_liquidacao: DateLike,
    vencimentos_tir: DatesLike,
    taxas_tir: ArrayLike,
    vencimentos_nominais: DatesLike,
    taxas_nominais: ArrayLike,
    *,
    extrapolar: bool = False,
) -> pl.DataFrame:
    """
    Calcula a inflação implícita para NTN-B contra uma curva nominal de referência.

    A inflação implícita (breakeven) é a que iguala yields reais e nominais,
    baseada nas taxas zero das NTN-B e na curva nominal informada.

    Args:
        data_liquidacao (DateLike): Data de liquidação da operação.
        vencimentos_tir (DatesLike): Vencimentos das NTN-B usadas como vértices
            da curva de TIR real.
        taxas_tir (ArrayLike): TIRs reais observadas das NTN-B correspondentes
            aos vencimentos informados. A função calcula a curva zero real a
            partir dessas taxas.
        vencimentos_nominais (DatesLike): Vencimentos da curva nominal de
            referência.
        taxas_nominais (ArrayLike): Taxas da curva nominal de referência. Pode
            representar DI Futuro, curva soberana prefixada ou outra curva
            nominal escolhida pelo usuário.
        extrapolar (bool): Se `True`, extrapola a curva nominal fora dos
            vencimentos informados. Se `False`, vencimentos fora do intervalo da
            curva nominal retornam valores nulos nas colunas dependentes dessa
            curva. O padrão é `False`.

    Returns:
        pl.DataFrame: DataFrame com as taxas calculadas.

    Output Columns:
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre liquidação e vencimento.
        - taxa_tir_real (Float64): TIR real da NTN-B recebida na entrada.
        - taxa_zero_real (Float64): Taxa real zero via bootstrap.
        - taxa_nominal (Float64): Taxa nominal interpolada.
        - inflacao_implicita (Float64): Inflação implícita (breakeven).

    Notes:
        A inflação implícita é calculada contra a curva nominal informada. Se
        essa curva for DI Futuro, o resultado representa a implícita contra DI,
        não uma inflação soberana prefixada pura.

    Examples:
        Busca as taxas de NTN-B para uma data de referência.
        Estas são TIRs e as taxas zero são calculadas a partir delas.
        >>> df_ntnb = yd.ntnb.dados("19-06-2026")

        Busca as taxas de ajuste do DI Futuro para a mesma data de referência:
        >>> df_di = yd.di1.dados("19-06-2026")

        Calcula a inflação implícita na data de referência:
        >>> yd.ntnb.implicitas(
        ...     data_liquidacao="19-06-2026",
        ...     vencimentos_tir=df_ntnb["data_vencimento"],
        ...     taxas_tir=df_ntnb["taxa_indicativa"],
        ...     vencimentos_nominais=df_di["data_vencimento"],
        ...     taxas_nominais=df_di["taxa_ajuste"],
        ... )
        shape: (15, 6)
        ┌─────────────────┬────────────┬───────────────┬────────────────┬──────────────┬────────────────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_tir_real ┆ taxa_zero_real ┆ taxa_nominal ┆ inflacao_implicita │
        │ ---             ┆ ---        ┆ ---           ┆ ---            ┆ ---          ┆ ---                │
        │ date            ┆ i64        ┆ f64           ┆ f64            ┆ f64          ┆ f64                │
        ╞═════════════════╪════════════╪═══════════════╪════════════════╪══════════════╪════════════════════╡
        │ 2026-08-15      ┆ 41         ┆ 0.1115        ┆ 0.1115         ┆ 0.141339     ┆ 0.026846           │
        │ 2027-05-15      ┆ 226        ┆ 0.085733      ┆ 0.085642       ┆ 0.145795     ┆ 0.055407           │
        │ 2028-08-15      ┆ 541        ┆ 0.089683      ┆ 0.08971        ┆ 0.149149     ┆ 0.054545           │
        │ 2029-05-15      ┆ 725        ┆ 0.088171      ┆ 0.088129       ┆ 0.149535     ┆ 0.056432           │
        │ 2030-08-15      ┆ 1039       ┆ 0.088766      ┆ 0.088759       ┆ 0.149166     ┆ 0.055482           │
        │ …               ┆ …          ┆ …             ┆ …              ┆ …            ┆ …                  │
        │ 2040-08-15      ┆ 3548       ┆ 0.078262      ┆ 0.076087       ┆ 0.14591      ┆ 0.064886           │
        │ 2045-05-15      ┆ 4735       ┆ 0.076656      ┆ 0.073931       ┆ null         ┆ null               │
        │ 2050-08-15      ┆ 6050       ┆ 0.075659      ┆ 0.072435       ┆ null         ┆ null               │
        │ 2055-05-15      ┆ 7239       ┆ 0.074658      ┆ 0.07049        ┆ null         ┆ null               │
        │ 2060-08-15      ┆ 8556       ┆ 0.07464       ┆ 0.070832       ┆ null         ┆ null               │
        └─────────────────┴────────────┴───────────────┴────────────────┴──────────────┴────────────────────┘
    """
    if any_is_empty(
        data_liquidacao,
        vencimentos_tir,
        taxas_tir,
        vencimentos_nominais,
        taxas_nominais,
    ):
        return pl.DataFrame()
    liquidacao, vencimentos_tir, taxas_tir = _validar_entradas_taxas_zero(
        data_liquidacao, vencimentos_tir, taxas_tir
    )
    vencimentos_nominais = conversores.converter_datas(vencimentos_nominais)

    interpolador_ff = interpolador.Interpolador(
        dias_uteis=du.contar(liquidacao, vencimentos_nominais),
        taxas=taxas_nominais,
        metodo="flat_forward",
        extrapolar=extrapolar,
    )
    df_tir = pl.DataFrame(
        data={"data_vencimento": vencimentos_tir, "taxa_tir_real": taxas_tir},
        schema={"data_vencimento": pl.Date, "taxa_tir_real": pl.Float64},
    )
    taxa_nominal_expr = interpolador_ff.interpolar_expr("dias_uteis")
    df = (
        taxas_zero(liquidacao, vencimentos_tir, taxas_tir)
        .join(df_tir, on="data_vencimento", how="left")
        .select(
            "data_vencimento",
            "dias_uteis",
            "taxa_tir_real",
            taxa_zero_real=pl.col("taxa_zero"),
            taxa_nominal=taxa_nominal_expr,
            inflacao_implicita=(taxa_nominal_expr + 1) / (pl.col("taxa_zero") + 1) - 1,
        )
    )

    return df


def duration(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
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
        y    = TIR (periódica)
        Price = Soma( CFₜ / (1 + y)ᵗ )

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float): Taxa de desconto usada no cálculo.

    Returns:
        float: Macaulay duration em anos úteis.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.duration("23-08-2024", "15-08-2060", 0.061005)
        15.08305431313046
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    anos_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"]) / 252
    vp = df_fluxos["valor_pagamento"] / (1 + taxa) ** anos_uteis
    duration = float((vp * anos_uteis).sum()) / float(vp.sum())
    # Truncar para 14 casas decimais para repetibilidade dos resultados
    return utils.truncar(duration, 14)


def duration_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
    taxa: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para a duration da NTN-B.

    O cálculo é aplicado linha a linha porque a duration depende dos fluxos de
    caixa do título.

    Args:
        data_liquidacao: Nome de coluna ou expressão Polars com a data de
            liquidação.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento.
        taxa: Nome de coluna ou expressão Polars com a taxa em formato decimal.

    Returns:
        pl.Expr: Expressão sem alias com a Macaulay duration em anos úteis.
    """
    return pl.struct(
        utils.coluna_ou_expr(data_liquidacao, "data_liquidacao"),
        utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        utils.coluna_ou_expr(taxa, "taxa"),
    ).map_elements(
        lambda s: duration(
            s["data_liquidacao"],
            s["data_vencimento"],
            s["taxa"],
        ),
        return_dtype=pl.Float64,
    )


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
    pu: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B em R$.

    Representa a variação do PU informado para um aumento de 1 bp (0,01%) na
    taxa.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float): Taxa de desconto (TIR) da NTN-B.
        pu (float): PU usado como base para o cálculo.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnb
        >>> cot = ntnb.cotacao("26-03-2025", "15-08-2060", 0.074358)
        >>> pu = ntnb.pu(4470.979474, cot)
        >>> ntnb.dv01("26-03-2025", "15-08-2060", 0.074358, pu)
        4.640876692897651
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa, pu):
        return float("nan")

    cotacao_1 = cotacao(data_liquidacao, data_vencimento, taxa)
    cotacao_2 = cotacao(data_liquidacao, data_vencimento, taxa + 0.0001)
    return pu * (1 - cotacao_2 / cotacao_1)


def dv01_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
    taxa: pl.Expr | str,
    pu: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para o DV01 da NTN-B.

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


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    vna: float,
    pu: float,
) -> float:
    """
    Calcula a TIR implícita de uma NTN-B a partir do preço (PU).

    A função inverte numericamente a cadeia ``pu(vna, cotacao(...))``,
    encontrando a taxa que zera a diferença entre o preço calculado e o
    informado.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        vna (float): Valor nominal atualizado (VNA).
        pu (float): Preço unitário (PU) do título.

    Returns:
        float: TIR implícita em formato decimal. Retorna NaN em
            caso de erro.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.taxa("31-05-2024", "15-05-2035", 4299.160173, 4271.864805)
        0.06149
        >>> ntnb.taxa("15-08-2024", "15-08-2032", 4315.498383, 4343.156412)
        0.05929
    """
    if any_is_empty(data_liquidacao, data_vencimento, vna, pu):
        return float("nan")

    if pu <= 0:
        return float("nan")

    def diferenca_preco(taxa_encontrada: float) -> float:
        cotacao_calculada = cotacao(data_liquidacao, data_vencimento, taxa_encontrada)
        return _calcular_pu(vna, cotacao_calculada) - pu

    taxa_encontrada = utils.encontrar_raiz(diferenca_preco)
    return round(taxa_encontrada, 6)
