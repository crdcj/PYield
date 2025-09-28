import pandas as pd

from pyield import anbima, bday
from pyield.date_converter import DateScalar
from pyield.tn import ntnf


def spot_rates(date: DateScalar) -> pd.DataFrame:
    """
    Create the PRE curve (zero coupon rates) for Brazilian fixed rate bonds.

    This function combines LTN rates (which are already zero coupon) with
    spot rates derived from NTN-F bonds using the bootstrap method.

    Args:
        date (DateScalar): The reference date for fetching the data.

    Returns:
        pd.DataFrame: DataFrame with columns "MaturityDate", "BDToMat", and "SpotRate".
                     Contains zero coupon rates for all available maturities.

    Raises:
        ValueError: If any maturity date cannot be processed or business days cannot be
            calculated.

    Examples:
        >>> from pyield import pre
        >>> pre.spot_rates("18-06-2025")
           MaturityDate  BDToMat  SpotRate
        0    2025-07-01        8   0.14835
        1    2025-10-01       74  0.147463
        2    2026-01-01      138  0.147752
        3    2026-04-01      199  0.147947
        4    2026-07-01      260  0.147069
        5    2026-10-01      325  0.144733
        6    2027-01-01      387  0.142496
        7    2027-04-01      447  0.140924
        8    2027-07-01      510  0.139024
        9    2028-01-01      638  0.136595
        10   2028-07-01      762  0.135664
        11   2029-01-01      886  0.136484
        12   2030-01-01     1135  0.137279
        13   2031-01-01     1387  0.138154
        14   2032-01-01     1639   0.13876
        15   2033-01-01     1891    0.1393
        16   2035-01-01     2390  0.141068
    """
    # Fetch LTN data (zero coupon bonds)
    df_ltn = anbima.tpf_data(date, "LTN")

    # Fetch NTN-F data (coupon bonds)
    df_ntnf = anbima.tpf_data(date, "NTN-F")

    # Check if we have data for both bond types
    if df_ltn.empty and df_ntnf.empty:
        return pd.DataFrame(columns=["MaturityDate", "BDToMat", "SpotRate"])

    # If we only have LTN data, return it directly
    if df_ntnf.empty:
        pass
        # return _process_ltn_only(date, df_ltn)

    # If we only have NTN-F data, we can't bootstrap without LTN rates
    if df_ltn.empty:
        raise ValueError(
            "Cannot construct PRE curve without LTN rates for bootstrapping"
        )

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
    ltn_mask = ~df_ltn["MaturityDate"].isin(df_spots["MaturityDate"])
    ltn_not_in_ntnf = df_ltn.loc[
        ltn_mask
    ].copy()  # Use .loc and .copy() to avoid warning

    if not ltn_not_in_ntnf.empty:
        # Process additional LTN maturities
        ltn_subset = _process_additional_ltn(date, ltn_not_in_ntnf)

        # Ensure consistent data types
        ltn_subset["BDToMat"] = ltn_subset["BDToMat"].astype("int64[pyarrow]")
        df_spots["BDToMat"] = df_spots["BDToMat"].astype("int64[pyarrow]")

        # Combine LTN and NTN-F derived spot rates
        df_combined = pd.concat([df_spots, ltn_subset], ignore_index=True)
    else:
        df_combined = df_spots.copy()
        df_combined["BDToMat"] = df_combined["BDToMat"].astype("int64[pyarrow]")

    # Final validation - ensure no NaN values in the result
    _validate_final_result(df_combined)

    # Sort by maturity date and return
    return df_combined.sort_values("MaturityDate").reset_index(drop=True)


def _process_additional_ltn(
    date: DateScalar, ltn_not_in_ntnf: pd.DataFrame
) -> pd.DataFrame:
    """Process additional LTN maturities not covered by NTN-F bootstrap."""
    # Validate and calculate business days
    bdays_list = []
    for idx, maturity in enumerate(ltn_not_in_ntnf["MaturityDate"]):
        if pd.isna(maturity):
            raise ValueError(
                f"Additional LTN row {idx} has invalid (NaT) maturity date"
            )

        try:
            bdays = bday.count(date, maturity)
            if pd.isna(bdays):
                raise ValueError(
                    f"Business days calculation returned NaN for additional LTN"
                    f"maturity {maturity}"
                )
            bdays_list.append(bdays)
        except Exception as e:
            raise ValueError(
                "Failed to calculate business days for additional LTN"
                f" maturity {maturity}: {str(e)}"
            )

    # Create result DataFrame (avoiding the warning by working with a proper copy)
    result = ltn_not_in_ntnf[["MaturityDate", "IndicativeRate"]].copy()
    result["BDToMat"] = bdays_list
    result = result.rename(columns={"IndicativeRate": "SpotRate"})

    return result[["MaturityDate", "BDToMat", "SpotRate"]]


def _validate_final_result(df_combined: pd.DataFrame) -> None:
    """Validate the final combined DataFrame."""
    if df_combined["BDToMat"].isna().any():
        raise ValueError("Final result contains NaN values in BDToMat column")

    if df_combined["SpotRate"].isna().any():
        raise ValueError("Final result contains NaN values in SpotRate column")
