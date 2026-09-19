import polars as pl

from pyield import du
from pyield._internal.types import DateLike

from . import _utils


def premios_pre(data: DateLike) -> pl.DataFrame:
    """Calcula o prêmio dos títulos prefixados (LTN e NTN-F) sobre o DI.

    Em linguagem de mercado, esse valor é chamado de prêmio. Em termos
    descritivos, trata-se do spread sobre o DI.

    Definição do prêmio:
        premio = taxa indicativa do PRE - taxa de ajuste do DI

    A coluna retorna essa diferença em formato decimal (ex: 0.000439 ≈
    4.39 bps). Para exibir o prêmio em pontos-base, multiplique a coluna
    ``premio`` por 10_000 no DataFrame retornado. No exemplo abaixo, essa
    coluna é sobrescrita apenas para facilitar a leitura em pontos-base.

    Args:
        data: Data da consulta para buscar as taxas.

    Returns:
        DataFrame com as colunas do prêmio. Retorna DataFrame vazio se
        não houver dados.

    Output Columns:
        * titulo (String): tipo do título.
        * data_vencimento (Date): data de vencimento.
        * premio (Float64): prêmio em formato decimal (spread sobre o DI).

    Examples:
        >>> # Exemplo em pontos-base para facilitar a leitura
        >>> yd.tpf.premios_pre("30-05-2025").with_columns(
        ...     premio=pl.col("premio") * 10_000
        ... )
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
    df = _utils.obter_tpf(data, "PRE").select(
        "titulo", "data_vencimento", "taxa_indicativa"
    )
    if df.is_empty():
        return df.select(
            pl.lit("").alias("titulo"),
            pl.lit(None, dtype=pl.Date).alias("data_vencimento"),
            pl.lit(None, dtype=pl.Float64).alias("premio"),
        ).clear()
    df = _utils.adicionar_taxa_di(df, data)
    return (
        df.with_columns(premio=pl.col("taxa_indicativa") - pl.col("taxa_di"))
        .select("titulo", "data_vencimento", "premio")
        .sort("titulo", "data_vencimento")
    )


def curva_pre(data: DateLike) -> pl.DataFrame:
    """Constrói a curva PRE (taxas zero cupom prefixadas).

    Combina taxas de LTN (já zero cupom) com taxas spot derivadas de NTN-F
    via bootstrap. O resultado é a curva de juros prefixada brasileira expressa
    em taxas zero cupom.

    Fonte: ANBIMA (taxas indicativas de LTN e NTN-F).

    Args:
        data: Data de referência.

    Returns:
        DataFrame com a curva PRE para a data solicitada. Retorna DataFrame
        vazio se não houver dados de LTN disponíveis.

    Output Columns:
        * data_vencimento (Date): data de vencimento do vértice.
        * dias_uteis (Int64): dias úteis entre a data de referência e o vencimento.
        * taxa_zero (Float64): taxa zero cupom anualizada (base 252), em formato
          decimal.

    Raises:
        ValueError: Se houver NTN-F sem dados de LTN para bootstrap.

    Examples:
        >>> import polars.selectors as cs
        >>> curva = yd.tpf.curva_pre("18-06-2025")
        >>> curva.with_columns(cs.starts_with("taxa_") * 100)
        shape: (17, 3)
        ┌─────────────────┬────────────┬───────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_zero │
        │ ---             ┆ ---        ┆ ---       │
        │ date            ┆ i64        ┆ f64       │
        ╞═════════════════╪════════════╪═══════════╡
        │ 2025-07-01      ┆ 8          ┆ 14.835    │
        │ 2025-10-01      ┆ 74         ┆ 14.7463   │
        │ 2026-01-01      ┆ 138        ┆ 14.7752   │
        │ 2026-04-01      ┆ 199        ┆ 14.7947   │
        │ 2026-07-01      ┆ 260        ┆ 14.7069   │
        │ …               ┆ …          ┆ …         │
        │ 2030-01-01      ┆ 1135       ┆ 13.7279   │
        │ 2031-01-01      ┆ 1387       ┆ 13.815381 │
        │ 2032-01-01      ┆ 1639       ┆ 13.876    │
        │ 2033-01-01      ┆ 1891       ┆ 13.930017 │
        │ 2035-01-01      ┆ 2390       ┆ 14.106777 │
        └─────────────────┴────────────┴───────────┘
    """
    from pyield.tpf.titulos import ntnf  # noqa: PLC0415

    df_ltn = _utils.obter_tpf(data, "LTN").select("data_vencimento", "taxa_indicativa")
    df_ntnf = _utils.obter_tpf(data, "NTN-F").select(
        "data_vencimento", "taxa_indicativa"
    )

    if df_ltn.is_empty() and df_ntnf.is_empty():
        return pl.DataFrame(
            schema={
                "data_vencimento": pl.Date,
                "dias_uteis": pl.Int64,
                "taxa_zero": pl.Float64,
            }
        )

    if df_ltn.is_empty():
        raise ValueError(
            "Não é possível construir a curva PRE sem taxas de LTN para bootstrap"
        )

    if df_ntnf.is_empty():
        df = _processar_ltn_adicionais(data, df_ltn)
    else:
        df_spots = ntnf.taxas_zero(
            data_liquidacao=data,
            vencimentos_ltn=df_ltn["data_vencimento"],
            taxas_ltn=df_ltn["taxa_indicativa"],
            vencimentos_ntnf=df_ntnf["data_vencimento"],
            taxas_ntnf=df_ntnf["taxa_indicativa"],
            incluir_cupons=False,
        )

        ltn_mask = ~df_ltn["data_vencimento"].is_in(
            df_spots["data_vencimento"].to_list()
        )
        ltn_not_in_ntnf = df_ltn.filter(ltn_mask)

        if not ltn_not_in_ntnf.is_empty():
            ltn_subset = _processar_ltn_adicionais(data, ltn_not_in_ntnf)
            df = pl.concat([df_spots, ltn_subset])
        else:
            df = df_spots

    _validar_resultado_final(df)
    return df.sort("data_vencimento")


def _processar_ltn_adicionais(
    data_referencia: DateLike,
    ltn_nao_em_ntnf: pl.DataFrame,
) -> pl.DataFrame:
    """Processa vencimentos de LTN fora do bootstrap de NTN-F."""
    dias_uteis = du.contar(data_referencia, ltn_nao_em_ntnf["data_vencimento"])
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
