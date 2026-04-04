import polars as pl

import pyield._internal.converters as cv
from pyield import dus
from pyield._internal.types import DateLike
from pyield.tn import ntnf, utils


def taxas_zero(data: DateLike) -> pl.DataFrame:
    """
    Cria a curva PRE (taxas zero cupom) para títulos prefixados brasileiros.

    Combina taxas de LTN (já zero cupom) com taxas spot derivadas de NTN-F
    via bootstrap.

    Args:
        data: Data da consulta.

    Returns:
        pl.DataFrame: DataFrame com as colunas da curva PRE.

    Output Columns:
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - taxa_zero (Float64): Taxa zero (zero cupom).

    Raises:
        ValueError: Se algum vencimento não puder ser processado.

    Examples:
        >>> from pyield import pre
        >>> pre.taxas_zero("18-06-2025")
        shape: (17, 3)
        ┌─────────────────┬────────────┬───────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_zero │
        │ ---             ┆ ---        ┆ ---       │
        │ date            ┆ i64        ┆ f64       │
        ╞═════════════════╪════════════╪═══════════╡
        │ 2025-07-01      ┆ 8          ┆ 0.14835   │
        │ 2025-10-01      ┆ 74         ┆ 0.147463  │
        │ 2026-01-01      ┆ 138        ┆ 0.147752  │
        │ 2026-04-01      ┆ 199        ┆ 0.147947  │
        │ 2026-07-01      ┆ 260        ┆ 0.147069  │
        │ …               ┆ …          ┆ …         │
        │ 2030-01-01      ┆ 1135       ┆ 0.137279  │
        │ 2031-01-01      ┆ 1387       ┆ 0.138154  │
        │ 2032-01-01      ┆ 1639       ┆ 0.13876   │
        │ 2033-01-01      ┆ 1891       ┆ 0.1393    │
        │ 2035-01-01      ┆ 2390       ┆ 0.141068  │
        └─────────────────┴────────────┴───────────┘
    """
    # Busca dados de LTN (zero cupom)
    df_ltn = utils.obter_tpf(data, "LTN").select("data_vencimento", "taxa_indicativa")

    # Busca dados de NTN-F (com cupom)
    df_ntnf = utils.obter_tpf(data, "NTN-F").select(
        "data_vencimento", "taxa_indicativa"
    )

    # Verifica se há dados para ambos os tipos
    if df_ltn.is_empty() and df_ntnf.is_empty():
        return pl.DataFrame(
            schema={
                "data_vencimento": pl.Date,
                "dias_uteis": pl.Int64,
                "taxa_zero": pl.Float64,
            }
        )

    # Se só há NTN-F, não é possível fazer bootstrap sem LTN
    if df_ltn.is_empty():
        raise ValueError(
            "Não é possível construir a curva PRE sem taxas de LTN para bootstrap"
        )

    # Se só há LTN, retorna direto (LTN já são zero cupom)
    if df_ntnf.is_empty():
        df = _processar_ltn_adicionais(data, df_ltn)
    else:
        # Usa spot_rates de NTN-F para calcular zero cupom
        df_spots = ntnf.taxas_zero(
            data_liquidacao=data,
            ltn_vencimentos=df_ltn["data_vencimento"],
            ltn_taxas=df_ltn["taxa_indicativa"],
            ntnf_vencimentos=df_ntnf["data_vencimento"],
            ntnf_taxas=df_ntnf["taxa_indicativa"],
            incluir_cupons=False,
        )

        # Encontra vencimentos de LTN que não estão no resultado de NTN-F
        ltn_mask = ~df_ltn["data_vencimento"].is_in(
            df_spots["data_vencimento"].to_list()
        )
        ltn_not_in_ntnf = df_ltn.filter(ltn_mask)

        if not ltn_not_in_ntnf.is_empty():
            # Processa vencimentos de LTN adicionais
            ltn_subset = _processar_ltn_adicionais(
                data,
                ltn_not_in_ntnf,
            )

            # Combina LTN e NTN-F
            df = pl.concat([df_spots, ltn_subset])
        else:
            df = df_spots

    # Validação final
    _validar_resultado_final(df)

    # Ordena por vencimento
    return df.sort("data_vencimento")


def _processar_ltn_adicionais(
    data_referencia: DateLike,
    ltn_nao_em_ntnf: pl.DataFrame,
) -> pl.DataFrame:
    """Processa vencimentos de LTN fora do bootstrap de NTN-F."""
    # Calcula dias úteis de forma vetorizada
    dias_uteis = dus.contar(data_referencia, ltn_nao_em_ntnf["data_vencimento"])

    # Cria DataFrame de resultado
    return pl.DataFrame(
        {
            "data_vencimento": ltn_nao_em_ntnf["data_vencimento"],
            "dias_uteis": dias_uteis,
            "taxa_zero": ltn_nao_em_ntnf["taxa_indicativa"],
        }
    )


def _validar_resultado_final(df: pl.DataFrame) -> None:
    """Valida o DataFrame final combinado."""
    if df["dias_uteis"].is_null().any():
        raise ValueError("Resultado final contém NaN na coluna dias_uteis")

    if df["taxa_zero"].is_null().any():
        raise ValueError("Resultado final contém NaN na coluna taxa_zero")


def premio(
    data: DateLike,
    pontos_base: bool = False,
) -> pl.DataFrame:
    """
    Calcula o prêmio dos títulos prefixados (LTN e NTN-F) sobre o DI.

    Em linguagem de mercado, esse valor é chamado de prêmio. Em termos
    descritivos, trata-se do spread sobre o DI.

    Definição do prêmio:
        premio = taxa indicativa do PRE - taxa de ajuste do DI

    Quando ``pontos_base=False`` a coluna retorna essa diferença em formato
    decimal (ex: 0.000439 ≈ 4.39 bps). Quando ``pontos_base=True`` o valor é
    automaticamente
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
        - premio (Float64): prêmio em decimal ou bps conforme parâmetro
            (spread sobre o DI).

    Examples:
        >>> from pyield import pre
        >>> pre.premio("30-05-2025", pontos_base=True)
        shape: (18, 3)
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
        │ NTN-F  ┆ 2027-01-01      ┆ -3.31  │
        │ NTN-F  ┆ 2029-01-01      ┆ 14.21  │
        │ NTN-F  ┆ 2031-01-01      ┆ 21.61  │
        │ NTN-F  ┆ 2033-01-01      ┆ 11.51  │
        │ NTN-F  ┆ 2035-01-01      ┆ 22.0   │
        └────────┴─────────────────┴────────┘
    """
    # Busca taxas dos títulos (LTN e NTN-F) e adiciona taxa_di
    df = utils.obter_tpf(data, "PRE").select(
        "titulo", "data_vencimento", "taxa_indicativa"
    )
    if df.is_empty():
        return df.select(
            pl.lit("").alias("titulo"),
            pl.lit(None, dtype=pl.Date).alias("data_vencimento"),
            pl.lit(None, dtype=pl.Float64).alias("premio"),
        ).clear()
    data_ref = cv.converter_datas(data)
    df = utils.adicionar_taxa_di(df, data_ref)
    df = (
        df.with_columns(premio=pl.col("taxa_indicativa") - pl.col("taxa_di"))
        .select("titulo", "data_vencimento", "premio")
        .sort("titulo", "data_vencimento")
    )

    if pontos_base:
        df = df.with_columns(pl.col("premio") * 10_000)

    return df
