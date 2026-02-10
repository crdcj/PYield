import polars as pl

from pyield import anbima, bday
from pyield.anbima import tpf
from pyield.tn import ntnf
from pyield._internal.types import DateLike


def spot_rates(date: DateLike) -> pl.DataFrame:
    """
    Cria a curva PRE (taxas zero cupom) para títulos prefixados brasileiros.

    Combina taxas de LTN (já zero cupom) com taxas spot derivadas de NTN-F
    via bootstrap.

    Args:
        date (DateLike): Data de referência para a consulta.

    Returns:
        pl.DataFrame: DataFrame com as colunas da curva PRE.

    Output Columns:
        * MaturityDate (Date): Data de vencimento.
        * BDToMat (Int64): Dias úteis entre referência e vencimento.
        * SpotRate (Float64): Taxa spot (zero cupom).

    Raises:
        ValueError: Se algum vencimento não puder ser processado.

    Examples:
        >>> from pyield import pre
        >>> pre.spot_rates("18-06-2025")
        shape: (17, 3)
        ┌──────────────┬─────────┬──────────┐
        │ MaturityDate ┆ BDToMat ┆ SpotRate │
        │ ---          ┆ ---     ┆ ---      │
        │ date         ┆ i64     ┆ f64      │
        ╞══════════════╪═════════╪══════════╡
        │ 2025-07-01   ┆ 8       ┆ 0.14835  │
        │ 2025-10-01   ┆ 74      ┆ 0.147463 │
        │ 2026-01-01   ┆ 138     ┆ 0.147752 │
        │ 2026-04-01   ┆ 199     ┆ 0.147947 │
        │ 2026-07-01   ┆ 260     ┆ 0.147069 │
        │ …            ┆ …       ┆ …        │
        │ 2030-01-01   ┆ 1135    ┆ 0.137279 │
        │ 2031-01-01   ┆ 1387    ┆ 0.138154 │
        │ 2032-01-01   ┆ 1639    ┆ 0.13876  │
        │ 2033-01-01   ┆ 1891    ┆ 0.1393   │
        │ 2035-01-01   ┆ 2390    ┆ 0.141068 │
        └──────────────┴─────────┴──────────┘
    """
    # Busca dados de LTN (zero cupom)
    df_ltn = anbima.tpf_data(date, "LTN")

    # Busca dados de NTN-F (com cupom)
    df_ntnf = anbima.tpf_data(date, "NTN-F")

    # Verifica se há dados para ambos os tipos
    if df_ltn.is_empty() and df_ntnf.is_empty():
        return pl.DataFrame(
            schema={
                "MaturityDate": pl.Date,
                "BDToMat": pl.Int64,
                "SpotRate": pl.Float64,
            }
        )

    # Se só há NTN-F, não é possível fazer bootstrap sem LTN
    if df_ltn.is_empty():
        raise ValueError(
            "Não é possível construir a curva PRE sem taxas de LTN para bootstrap"
        )

    # Se só há LTN, retorna direto (LTN já são zero cupom)
    if df_ntnf.is_empty():
        df = _processar_ltn_adicionais(date, df_ltn)
    else:
        # Usa spot_rates de NTN-F para calcular zero cupom
        df_spots = ntnf.spot_rates(
            settlement=date,
            ltn_maturities=df_ltn["MaturityDate"],
            ltn_rates=df_ltn["IndicativeRate"],
            ntnf_maturities=df_ntnf["MaturityDate"],
            ntnf_rates=df_ntnf["IndicativeRate"],
            show_coupons=False,
        )

        # Encontra vencimentos de LTN que não estão no resultado de NTN-F
        ltn_mask = ~df_ltn["MaturityDate"].is_in(df_spots["MaturityDate"].to_list())
        ltn_not_in_ntnf = df_ltn.filter(ltn_mask)

        if not ltn_not_in_ntnf.is_empty():
            # Processa vencimentos de LTN adicionais
            ltn_subset = _processar_ltn_adicionais(date, ltn_not_in_ntnf)

            # Combina LTN e NTN-F
            df = pl.concat([df_spots, ltn_subset])
        else:
            df = df_spots

    # Validação final
    _validar_resultado_final(df)

    # Ordena por vencimento
    return df.sort("MaturityDate")


def _processar_ltn_adicionais(
    date: DateLike, ltn_nao_em_ntnf: pl.DataFrame
) -> pl.DataFrame:
    """Processa vencimentos de LTN fora do bootstrap de NTN-F."""
    # Calcula dias úteis de forma vetorizada
    dias_uteis = bday.count(date, ltn_nao_em_ntnf["MaturityDate"])

    # Cria DataFrame de resultado
    return pl.DataFrame(
        {
            "MaturityDate": ltn_nao_em_ntnf["MaturityDate"],
            "BDToMat": dias_uteis,
            "SpotRate": ltn_nao_em_ntnf["IndicativeRate"],
        }
    )


def _validar_resultado_final(df: pl.DataFrame) -> None:
    """Valida o DataFrame final combinado."""
    if df["BDToMat"].is_null().any():
        raise ValueError("Resultado final contém NaN na coluna BDToMat")

    if df["SpotRate"].is_null().any():
        raise ValueError("Resultado final contém NaN na coluna SpotRate")


def di_spreads(date: DateLike, bps: bool = False) -> pl.DataFrame:
    """
    Calcula o DI Spread para títulos prefixados (LTN e NTN-F) em uma data de referência.

    spread = taxa indicativa do PRE - taxa de ajuste do DI

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

    Examples:
        >>> from pyield import pre
        >>> pre.di_spreads("30-05-2025", bps=True)
        shape: (18, 3)
        ┌──────────┬──────────────┬──────────┐
        │ BondType ┆ MaturityDate ┆ DISpread │
        │ ---      ┆ ---          ┆ ---      │
        │ str      ┆ date         ┆ f64      │
        ╞══════════╪══════════════╪══════════╡
        │ LTN      ┆ 2025-07-01   ┆ 4.39     │
        │ LTN      ┆ 2025-10-01   ┆ -9.0     │
        │ LTN      ┆ 2026-01-01   ┆ -4.88    │
        │ LTN      ┆ 2026-04-01   ┆ -4.45    │
        │ LTN      ┆ 2026-07-01   ┆ 0.81     │
        │ …        ┆ …            ┆ …        │
        │ NTN-F    ┆ 2027-01-01   ┆ -3.31    │
        │ NTN-F    ┆ 2029-01-01   ┆ 14.21    │
        │ NTN-F    ┆ 2031-01-01   ┆ 21.61    │
        │ NTN-F    ┆ 2033-01-01   ┆ 11.51    │
        │ NTN-F    ┆ 2035-01-01   ┆ 22.0     │
        └──────────┴──────────────┴──────────┘
    """
    # Busca taxas dos títulos (LTN e NTN-F)
    df = (
        tpf.tpf_data(date, "PRE")
        .with_columns(DISpread=pl.col("IndicativeRate") - pl.col("DIRate"))
        .select("BondType", "MaturityDate", "DISpread")
        .sort("BondType", "MaturityDate")
    )

    if bps:
        df = df.with_columns(pl.col("DISpread") * 10_000)

    return df
