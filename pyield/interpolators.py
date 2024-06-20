import bisect


def interpolate_flat_forward(
    prev_rate: float, prev_bdays: int, next_rate: float, next_bdays: int, bdays: int
) -> float:
    """Performs interest rate interpolation using the flat forward interpolation method
    considering a base of 252 business days.

    The interpolation is done between two known pairs of vertices (prev_rate,
    prev_business_days) and (next_rate, next_business_days), for a third vertex defined
    by the number of business days 'business_days' where 'prev_business_days <
    business_days < next_business_days'. This third vertex is the point in time for
    which the interest rate is being calculated.

    Args:
    - prev_rate (float): Interest rate of the previous vertex.
    - prev_bdays (int): Number of business days of the previous vertex.
    - next_rate (float): Interest rate of the next vertex.
    - next_bdays (int): Number of business days of the next vertex.
    - bdays (int): Number of business days for which the interest rate is to be
      interpolated.

    Example:
    interpolated_rate = interpolate_flat_forward(0.045, 30, 0.05, 60, 45)

    Returns:
        float: The interpolated interest rate at the given `business_days`.
    """
    a = (1 + prev_rate) ** (prev_bdays / 252)
    b = (1 + next_rate) ** (next_bdays / 252)
    c = (bdays - prev_bdays) / (next_bdays - prev_bdays)

    return (a * (b / a) ** c) ** (252 / bdays) - 1


def find_and_interpolate_flat_forward(
    bdays: int,
    known_bdays: list[int],
    known_rates: list[float],
) -> float:
    """
    Finds the appropriate interpolation point and returns the interest rate
    interpolated by the flat forward method from that point. Uses the `bisect` module
    for binary search in ordered lists.


    Args:
        business_days (int): Number of business days for which the flat forward interest
            rate is to be calculated.
        known_business_days (List[int]): List of business days where interest rates are
            known.
        known_rates (List[float]): List of known interest rates.

    Notes:
        - It is assumed that `known_business_days` and `known_rates` are sorted and have
          the same size.
        - The method uses 252 business days per year in the interpolation, which is the
          standard in the Brazilian market.
        - Special cases are handled for situations where `business_days` is less than
          the first known business day, greater than the last known business day, or
          exactly equal to a known business day, avoiding the need for interpolation.

    Example:
    known_bdays = [30, 60, 90]
    known_rates = [0.045, 0.05, 0.055]
    interpolated_rate = find_and_interpolate_flat_forward(45, known_bdays, known_rates)

    Returns:
        float: The interest rate interpolated by the flat forward method for the given
            number of business days.
    """
    # Special cases
    if bdays <= known_bdays[0]:
        return known_rates[0]
    elif bdays >= known_bdays[-1]:
        return known_rates[-1]
    # Do not interpolate vertex whose rate is known
    elif bdays in known_bdays:
        return known_rates[known_bdays.index(bdays)]

    # Find i such that known_business_days[i-1] < business_days < known_business_days[i]
    i = bisect.bisect_left(known_bdays, bdays)

    return interpolate_flat_forward(
        known_rates[i - 1],
        known_bdays[i - 1],
        known_rates[i],
        known_bdays[i],
        bdays,
    )
