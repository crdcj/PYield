import datetime as dt
import logging

import polars as pl

import pyield._internal.converters as conversores
from pyield import dus, fwd, interpolador
from pyield._internal.types import ArrayLike, DateLike, any_is_empty
from pyield.tn import utils

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


def dados(data_referencia: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de NTN-B para a data de referência.

    Args:
        data_referencia (DateLike): Data de referência para a consulta.

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
        - dv01_usd (Float64): DV01 convertido para USD pela PTAX do dia.
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
    from pyield.b3 import di1  # noqa: PLC0415

    df = utils.obter_tpf(data_referencia, "NTN-B")
    if df.is_empty():
        return df

    data_ref = conversores.converter_datas(data_referencia)

    # Adiciona dias_uteis (dado derivado, não vem da ANBIMA)
    df = df.with_columns(
        dias_uteis=dus.contar_expr("data_referencia", "data_vencimento"),
    )

    # Adiciona duration, prazo_medio, dv01 e dv01_usd
    df = utils.adicionar_duration(df, duration)
    df = utils.adicionar_dv01(df, data_ref)

    # Busca curva DI bruta e calcula taxa_zero, taxa_di e inflação implícita
    df_di = di1.dados(data_referencia)
    df_bei = inflacao_implicita(
        data_liquidacao=data_referencia,
        ntnb_vencimentos=df["data_vencimento"],
        ntnb_taxas=df["taxa_indicativa"],
        nominal_vencimentos=df_di["data_vencimento"],
        nominal_taxas=df_di["taxa_ajuste"],
    ).select(
        pl.col("data_vencimento"),
        pl.col("taxa_zero"),
        pl.col("taxa_nominal").alias("taxa_di"),
        pl.col("inflacao_implicita"),
    )

    df = df.join(df_bei, on="data_vencimento", how="left")

    # Calcula taxas forward a partir das taxas zero
    taxas_forward = fwd.forwards(bdays=df["dias_uteis"], rates=df["taxa_zero"])
    df = df.with_columns(taxa_forward=taxas_forward)

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
        "taxa_zero",
        "taxa_forward",
        "inflacao_implicita",
    )


def vencimentos(data_referencia: DateLike) -> pl.Series:
    """
    Busca os vencimentos de NTN-B disponíveis para a data de referência.

    Args:
        data_referencia (DateLike): Data de referência para a consulta.

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
    return dados(data_referencia)["data_vencimento"]


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


def datas_pagamento(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
) -> pl.Series:
    """
    Gera todas as datas de cupom entre liquidação e vencimento (inclusivas).

    Os cupons são pagos em 15/02, 15/05, 15/08 e 15/11. A NTN-B é definida
    pela data de vencimento.

    Args:
        data_liquidacao (DateLike): Data de liquidação (exclusiva).
        data_vencimento (DateLike): Data de vencimento.

    Returns:
        pl.Series: Série de datas de cupom no intervalo. Retorna série vazia se
            vencimento for menor ou igual à liquidação.

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
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return pl.Series(name="datas_pagamento", dtype=pl.Date)

    liquidacao = conversores.converter_datas(data_liquidacao)
    vencimento = conversores.converter_datas(data_vencimento)

    if vencimento <= liquidacao:
        return pl.Series(name="datas_pagamento", dtype=pl.Date)

    data_cupom = vencimento
    datas_cupons = []

    while data_cupom > liquidacao:
        datas_cupons.append(data_cupom)
        data_cupom = utils.subtrair_meses(data_cupom, 6)

    return pl.Series(name="datas_pagamento", values=datas_cupons).sort()


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
        - valor_pagamento (Float64): Valor do pagamento.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.fluxos_caixa("10-05-2024", "15-05-2025")
        shape: (3, 2)
        ┌────────────────┬─────────────────┐
        │ data_pagamento ┆ valor_pagamento │
        │ ---            ┆ ---             │
        │ date           ┆ f64             │
        ╞════════════════╪═════════════════╡
        │ 2024-05-15     ┆ 2.956301        │
        │ 2024-11-15     ┆ 2.956301        │
        │ 2025-05-15     ┆ 102.956301      │
        └────────────────┴─────────────────┘
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return pl.DataFrame(
            schema={"data_pagamento": pl.Date, "valor_pagamento": pl.Float64}
        )

    # Obtém as datas de cupom entre liquidação e vencimento
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
    Calcula a cotação da NTN-B em base 100 pelas regras da ANBIMA.

    Args:
        data_liquidacao (DateLike): Data de liquidação da operação.
        data_vencimento (DateLike): Data de vencimento da NTN-B.
        taxa (float): Taxa de desconto (TIR) usada no valor presente.

    Returns:
        float: Cotação da NTN-B truncada em 4 casas. Retorna NaN em erro.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf
        - O cupom semestral é 2,956301, equivalente a 6% a.a. com capitalização
          semestral e arredondamento para 6 casas, conforme ANBIMA.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)
        99.3651
        >>> ntnb.cotacao("31-05-2024", "15-08-2060", 0.061878)
        99.5341
        >>> ntnb.cotacao("15-08-2024", "15-08-2032", 0.05929)
        100.6409
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    valores_fluxo = df_fluxos["valor_pagamento"]
    dias_uteis = dus.contar(data_liquidacao, df_fluxos["data_pagamento"])
    anos_uteis = utils.truncar(dias_uteis / 252, 14)
    fatores_desconto = (1 + taxa) ** anos_uteis
    # Calcula o valor presente de cada fluxo com arredondamento ANBIMA
    vp = (valores_fluxo / fatores_desconto).round(10)
    # Retorna a cotação (soma dos valores presentes) com truncamento ANBIMA
    return utils.truncar(vp.sum(), 4)


def _calcular_pu(
    vna: float,
    cotacao: float,
) -> float:
    if any_is_empty(vna, cotacao):
        return float("nan")
    return utils.truncar(vna * cotacao / 100, 6)


def pu(
    vna: float,
    cotacao: float,
) -> float:
    """
    Calcula o preço (PU) da NTN-B pelas regras da ANBIMA.

    Args:
        vna (float): Valor nominal atualizado (VNA).
        cotacao (float): Cotação da NTN-B em base 100.

    Returns:
        float: Preço da NTN-B truncado em 6 casas decimais.

    References:
        - https://www.anbima.com.br/data/files/A0/02/CC/70/8FEFC8104606BDC8B82BA2A8/Metodologias%20ANBIMA%20de%20Precificacao%20Titulos%20Publicos.pdf

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.pu(4299.160173, 99.3651)
        4271.864805
        >>> ntnb.pu(4315.498383, 100.6409)
        4343.156412
    """
    return _calcular_pu(vna, cotacao)


def _validar_entradas_taxas_zero(
    data_liquidacao: DateLike,
    vencimentos: ArrayLike,
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
        dias_uteis=dus.contar(data_liquidacao, vencimentos),
        taxas=taxas,
        metodo="flat_forward",
    )

    # Gera datas de cupom até o último vencimento
    ultimo_vencimento = vencimentos.max()
    assert isinstance(ultimo_vencimento, dt.date)
    todas_datas_cupom = _gerar_todas_datas_cupom(data_liquidacao, ultimo_vencimento)
    dias_uteis_ate_venc = dus.contar(data_liquidacao, todas_datas_cupom)
    taxas_tir = interpolador_ff.interpolar(dias_uteis_ate_venc)

    df = (
        pl.DataFrame(
            {
                "data_vencimento": todas_datas_cupom,
                "dias_uteis": dias_uteis_ate_venc,
                "anos_uteis": dias_uteis_ate_venc / 252,
                "taxa_tir": taxas_tir,
            }
        )
        .with_columns(
            cupom=pl.lit(VALOR_CUPOM),
            taxa_zero=pl.lit(None, dtype=pl.Float64),
        )
        .sort("data_vencimento")
    )
    return df


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
    vencimentos: ArrayLike,
    taxas: ArrayLike,
    incluir_cupons: bool = False,
) -> pl.DataFrame:
    """
    Calcula as taxas zero da NTN-B usando bootstrap.

    O bootstrap determina as taxas zero a partir dos yields dos títulos,
    resolvendo iterativamente as taxas que descontam os fluxos ao preço.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        vencimentos (ArrayLike): Datas de vencimento dos títulos.
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


def inflacao_implicita(
    data_liquidacao: DateLike,
    ntnb_vencimentos: ArrayLike,
    ntnb_taxas: ArrayLike,
    nominal_vencimentos: ArrayLike,
    nominal_taxas: ArrayLike,
) -> pl.DataFrame:
    """
    Calcula a inflação implícita para NTN-B a partir de taxas nominais e reais.

    A inflação implícita (breakeven) é a que iguala yields reais e nominais,
    baseada nas taxas zero das NTN-B.

    Args:
        data_liquidacao (DateLike): Data de liquidação da operação.
        ntnb_vencimentos (ArrayLike): Vencimentos das NTN-B.
        ntnb_taxas (ArrayLike): TIRs reais correspondentes.
        nominal_vencimentos (ArrayLike): Vencimentos de referência da curva nominal.
        nominal_taxas (ArrayLike): Taxas nominais de referência.

    Returns:
        pl.DataFrame: DataFrame com as taxas calculadas.

    Output Columns:
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre liquidação e vencimento.
        - taxa_zero (Float64): Taxa real zero via bootstrap.
        - taxa_nominal (Float64): Taxa nominal interpolada.
        - inflacao_implicita (Float64): Inflação implícita (breakeven).

    Notes:
        A inflação implícita indica a expectativa de mercado entre
        liquidação e vencimento.

    Examples:
        Busca as taxas de NTN-B para uma data de referência.
        Estas são TIRs e as taxas zero são calculadas a partir delas.
        >>> df_ntnb = yd.ntnb.dados("05-09-2024")

        Busca as taxas de ajuste do DI Futuro para a mesma data de referência:
        >>> df_di = yd.di1.dados("05-09-2024")

        Calcula a inflação implícita na data de referência:
        >>> yd.ntnb.inflacao_implicita(
        ...     data_liquidacao="05-09-2024",
        ...     ntnb_vencimentos=df_ntnb["data_vencimento"],
        ...     ntnb_taxas=df_ntnb["taxa_indicativa"],
        ...     nominal_vencimentos=df_di["data_vencimento"],
        ...     nominal_taxas=df_di["taxa_ajuste"],
        ... )
        shape: (14, 5)
        ┌─────────────────┬────────────┬───────────┬──────────────┬────────────────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_zero ┆ taxa_nominal ┆ inflacao_implicita │
        │ ---             ┆ ---        ┆ ---       ┆ ---          ┆ ---                │
        │ date            ┆ i64        ┆ f64       ┆ f64          ┆ f64                │
        ╞═════════════════╪════════════╪═══════════╪══════════════╪════════════════════╡
        │ 2025-05-15      ┆ 171        ┆ 0.061748  ┆ 0.113836     ┆ 0.049059           │
        │ 2026-08-15      ┆ 488        ┆ 0.066133  ┆ 0.117126     ┆ 0.04783            │
        │ 2027-05-15      ┆ 673        ┆ 0.063816  ┆ 0.117169     ┆ 0.050152           │
        │ 2028-08-15      ┆ 988        ┆ 0.063635  ┆ 0.11828      ┆ 0.051376           │
        │ 2029-05-15      ┆ 1172       ┆ 0.062532  ┆ 0.11838      ┆ 0.052561           │
        │ …               ┆ …          ┆ …         ┆ …            ┆ …                  │
        │ 2040-08-15      ┆ 3995       ┆ 0.060468  ┆ 0.11759      ┆ 0.053865           │
        │ 2045-05-15      ┆ 5182       ┆ 0.0625    ┆ 0.11759      ┆ 0.05185            │
        │ 2050-08-15      ┆ 6497       ┆ 0.063016  ┆ 0.11759      ┆ 0.051339           │
        │ 2055-05-15      ┆ 7686       ┆ 0.062252  ┆ 0.11759      ┆ 0.052095           │
        │ 2060-08-15      ┆ 9003       ┆ 0.063001  ┆ 0.11759      ┆ 0.051354           │
        └─────────────────┴────────────┴───────────┴──────────────┴────────────────────┘
    """
    if any_is_empty(
        data_liquidacao,
        ntnb_vencimentos,
        ntnb_taxas,
        nominal_vencimentos,
        nominal_taxas,
    ):
        return pl.DataFrame()
    # Normaliza datas de entrada
    liquidacao = conversores.converter_datas(data_liquidacao)
    ntnb_vencimentos = conversores.converter_datas(ntnb_vencimentos)
    nominal_vencimentos = conversores.converter_datas(nominal_vencimentos)

    interpolador_ff = interpolador.Interpolador(
        dias_uteis=dus.contar(liquidacao, nominal_vencimentos),
        taxas=nominal_taxas,
        metodo="flat_forward",
        extrapolar=True,
    )
    df_spot = taxas_zero(liquidacao, ntnb_vencimentos, ntnb_taxas)
    df = (
        df_spot.with_columns(
            taxa_nominal=interpolador_ff(df_spot["dias_uteis"]),
        )
        .with_columns(
            inflacao_implicita=(
                (pl.col("taxa_nominal") + 1) / (pl.col("taxa_zero") + 1)
            )
            - 1,
        )
        .select(
            "data_vencimento",
            "dias_uteis",
            "taxa_zero",
            "taxa_nominal",
            "inflacao_implicita",
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

    anos_uteis = dus.contar(data_liquidacao, df_fluxos["data_pagamento"]) / 252
    vp = df_fluxos["valor_pagamento"] / (1 + taxa) ** anos_uteis
    duration = float((vp * anos_uteis).sum()) / float(vp.sum())
    # Truncar para 14 casas decimais para repetibilidade dos resultados
    return utils.truncar(duration, 14)


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float,
    vna: float,
) -> float:
    """
    Calcula o DV01 (Dollar Value of 01) da NTN-B em R$.

    Representa a variação de preço para um aumento de 1 bp (0,01%) na taxa.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float): Taxa de desconto (TIR) da NTN-B.

    Returns:
        float: DV01, variação de preço para 1 bp.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.dv01("26-03-2025", "15-08-2060", 0.074358, 4470.979474)
        4.640875999999935
    """
    if any_is_empty(data_liquidacao, data_vencimento, taxa, vna):
        return float("nan")

    cotacao_1 = cotacao(data_liquidacao, data_vencimento, taxa)
    cotacao_2 = cotacao(data_liquidacao, data_vencimento, taxa + 0.0001)
    preco_1 = pu(vna, cotacao_1)
    preco_2 = pu(vna, cotacao_2)
    return preco_1 - preco_2


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


def forward(
    data_referencia: DateLike,
    usar_taxa_zero: bool = True,
) -> pl.DataFrame:
    """
    Calcula as taxas forward da NTN-B para a data de referência.

    Args:
        data_referencia (DateLike): Data de referência para a consulta.
        usar_taxa_zero (bool, optional): Se True, usa taxas zero cupom no cálculo.
            Padrão True. Se False, usa as TIRs.

    Returns:
        pl.DataFrame: DataFrame com as taxas forward.

    Output Columns:
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - taxa_indicativa (Float64): Taxa indicativa (spot ou TIR).
        - taxa_forward (Float64): Taxa forward calculada.

    Examples:
        >>> from pyield import ntnb
        >>> ntnb.forward("17-10-2025", usar_taxa_zero=True)
        shape: (13, 4)
        ┌─────────────────┬────────────┬─────────────────┬──────────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_indicativa ┆ taxa_forward │
        │ ---             ┆ ---        ┆ ---             ┆ ---          │
        │ date            ┆ i64        ┆ f64             ┆ f64          │
        ╞═════════════════╪════════════╪═════════════════╪══════════════╡
        │ 2026-08-15      ┆ 207        ┆ 0.10089         ┆ 0.10089      │
        │ 2027-05-15      ┆ 392        ┆ 0.088776        ┆ 0.074793     │
        │ 2028-08-15      ┆ 707        ┆ 0.083615        ┆ 0.076598     │
        │ 2029-05-15      ┆ 891        ┆ 0.0818          ┆ 0.074148     │
        │ 2030-08-15      ┆ 1205       ┆ 0.080902        ┆ 0.077857     │
        │ …               ┆ …          ┆ …               ┆ …            │
        │ 2040-08-15      ┆ 3714       ┆ 0.076067        ┆ 0.070587     │
        │ 2045-05-15      ┆ 4901       ┆ 0.075195        ┆ 0.069811     │
        │ 2050-08-15      ┆ 6216       ┆ 0.074087        ┆ 0.064348     │
        │ 2055-05-15      ┆ 7405       ┆ 0.073702        ┆ 0.067551     │
        │ 2060-08-15      ┆ 8722       ┆ 0.073795        ┆ 0.074505     │
        └─────────────────┴────────────┴─────────────────┴──────────────┘
    """
    if any_is_empty(data_referencia):
        return pl.DataFrame()

    # Valida e normaliza a data
    df = dados(data_referencia).select(
        "data_vencimento", "dias_uteis", "taxa_indicativa"
    )
    if usar_taxa_zero:
        df_ref = taxas_zero(
            data_liquidacao=data_referencia,
            vencimentos=df["data_vencimento"],
            taxas=df["taxa_indicativa"],
        ).rename({"taxa_zero": "taxa_referencia"})
    else:
        df_ref = df.rename({"taxa_indicativa": "taxa_referencia"})
    taxas_forward = fwd.forwards(
        bdays=df_ref["dias_uteis"], rates=df_ref["taxa_referencia"]
    )
    df_ref = df_ref.with_columns(taxa_forward=taxas_forward)
    df = df.join(
        df_ref.select("data_vencimento", "taxa_forward"),
        on="data_vencimento",
        how="inner",
    ).sort("data_vencimento")
    return df
