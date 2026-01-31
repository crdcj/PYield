import polars as pl

from pyield import anbima, bday
from pyield.anbima import tpf
from pyield.tn import ntnf
from pyield.types import DateLike


def spot_rates(date: DateLike) -> pl.DataFrame:
    """
    Create the PRE curve (zero coupon rates) for Brazilian fixed rate bonds.

    This function combines LTN rates (which are already zero coupon) with
    spot rates derived from NTN-F bonds using the bootstrap method.

    Args:
        date (DateLike): The reference date for fetching the data.

    Returns:
        pl.DataFrame: DataFrame with columns "MaturityDate", "BDToMat", and "SpotRate".
                     Contains zero coupon rates for all available maturities.

    Raises:
        ValueError: If any maturity date cannot be processed or business days cannot be
            calculated.

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
    # Fetch LTN data (zero coupon bonds)
    df_ltn = anbima.tpf_data(date, "LTN")

    # Fetch NTN-F data (coupon bonds)
    df_ntnf = anbima.tpf_data(date, "NTN-F")

    # Check if we have data for both bond types
    if df_ltn.is_empty() and df_ntnf.is_empty():
        return pl.DataFrame(
            schema={
                "MaturityDate": pl.Date,
                "BDToMat": pl.Int64,
                "SpotRate": pl.Float64,
            }
        )

    # If we only have NTN-F data, we can't bootstrap without LTN rates
    if df_ltn.is_empty():
        raise ValueError(
            "Cannot construct PRE curve without LTN rates for bootstrapping"
        )

    # If we only have LTN data, return it directly (LTN are already zero coupon)
    if df_ntnf.is_empty():
        df_combined = _process_additional_ltn(date, df_ltn)
    else:
        # Use the existing spot_rates function to calculate zero coupon rates
        df_spots = ntnf.spot_rates(
            settlement=date,
            ltn_maturities=df_ltn["MaturityDate"],
            ltn_rates=df_ltn["IndicativeRate"],
            ntnf_maturities=df_ntnf["MaturityDate"],
            ntnf_rates=df_ntnf["IndicativeRate"],
            show_coupons=False,
        )

        # Find LTN maturities that are not in the NTN-F result
        ltn_mask = ~df_ltn["MaturityDate"].is_in(df_spots["MaturityDate"].to_list())
        ltn_not_in_ntnf = df_ltn.filter(ltn_mask)

        if not ltn_not_in_ntnf.is_empty():
            # Process additional LTN maturities
            ltn_subset = _process_additional_ltn(date, ltn_not_in_ntnf)

            # Combine LTN and NTN-F derived spot rates
            df_combined = pl.concat([df_spots, ltn_subset])
        else:
            df_combined = df_spots

    # Final validation - ensure no NaN values in the result
    _validate_final_result(df_combined)

    # Sort by maturity date and return
    return df_combined.sort("MaturityDate")


def _process_additional_ltn(
    date: DateLike, ltn_not_in_ntnf: pl.DataFrame
) -> pl.DataFrame:
    """Process additional LTN maturities not covered by NTN-F bootstrap."""
    # Calculate business days using vectorized operation
    bdays = bday.count(date, ltn_not_in_ntnf["MaturityDate"])

    # Create result DataFrame
    return pl.DataFrame(
        {
            "MaturityDate": ltn_not_in_ntnf["MaturityDate"],
            "BDToMat": bdays,
            "SpotRate": ltn_not_in_ntnf["IndicativeRate"],
        }
    )


def _validate_final_result(df_combined: pl.DataFrame) -> None:
    """Validate the final combined DataFrame."""
    if df_combined["BDToMat"].is_null().any():
        raise ValueError("Final result contains NaN values in BDToMat column")

    if df_combined["SpotRate"].is_null().any():
        raise ValueError("Final result contains NaN values in SpotRate column")


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
            Default False.

    Returns:
        pl.DataFrame com colunas:
            - BondType
            - MaturityDate
            - DISpread (decimal ou bps conforme parâmetro)

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
    # Fetch bond rates, filtering for LTN and NTN-F types
    df = (
        tpf.tpf_data(date, "PRE")
        .with_columns(DISpread=pl.col("IndicativeRate") - pl.col("DIRate"))
        .select("BondType", "MaturityDate", "DISpread")
        .sort("BondType", "MaturityDate")
    )

    if bps:
        df = df.with_columns(pl.col("DISpread") * 10_000)

    return df
