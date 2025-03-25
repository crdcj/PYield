[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)

# PYield: Brazilian Fixed Income Toolkit

PYield is a Python library designed for the analysis of Brazilian fixed income instruments. Leveraging the power of popular Python libraries like Pandas and Requests, PYield simplifies the process of obtaining and processing data from key sources such as ANBIMA, BCB, IBGE and B3.

Documentation: [https://crdcj.github.io/PYield/](https://crdcj.github.io/PYield/)

## Key Features

- **Data Collection**: Automated fetching of data from ANBIMA and B3.
- **Data Processing**: Efficient processing and normalization of fixed income data.
- **Analysis Tools**: Built-in functions for common analysis tasks in fixed income markets.
- **Easy Integration**: Seamless integration with pandas data analysis workflows.
- **Type Hints**: Full support for static type checking, enhancing development experience and code quality.

## Installation

You can install PYield using pip:
```sh
pip install pyield
```
## Custom Types

### DateScalar
`DateScalar` and `DateArray` are a type alias used across PYield to represent different types of date inputs. It includes several common date formats, allowing for flexible date handling within the library. 

The accepted formats in `DateScalar` are:

- `datetime.date`
- `datetime.datetime`
- `str` (in the format `DD-MM-YYYY` as used in Brazil)
- `np.datetime64`
- `pd.Timestamp`

### DateArray
The accepted formats in `DateArray` are:
- `pd.Series`
- `pd.DatetimeIndex`
- `np.ndarray`
- `list[DateScalar]`
- `tuple[DateScalar]`

Referencing `DateScalar` and `DateArray` in function arguments simplifies the code by allowing any of these date formats to be used interchangeably.

### Important Note on Date Formats
When using date strings in PYield functions, please ensure that the **date format is day-first** (e.g., "31-05-2024"). This format was chosen to be consistent with the Brazilian date convention.

For production code, it is recommended to parse date strings with `pandas.to_datetime` using an **explicit format** to avoid ambiguity and ensure consistency. For example:
```python
import pandas as pd
# Converting a date string to a pandas Timestamp with a specific format
date = pd.to_datetime("2024/31/05", format="%Y/%d/%m")
date = pd.to_datetime("05-31-2024", format="%m-%d-%Y")
```
## How to use PYield
### Brazilian Treasury Bonds Tools
```python
>>> from pyield import ntnb, ntnf, ltn

# Get ANBIMA data for a given date
>>> ltn.data("23-08-2024")
    ReferenceDate BondType MaturityDate  IndicativeRate       Price
0     2024-08-23      LTN   2024-10-01        0.104416  989.415342
1     2024-08-23      LTN   2025-01-01        0.107171  964.293046
2     2024-08-23      LTN   2025-04-01        0.110866  938.943013
3     2024-08-23      LTN   2025-07-01        0.113032  913.849158
4     2024-08-23      LTN   2025-10-01        0.114374  887.394285
5     2024-08-23      LTN   2026-01-01        0.114654  863.026594
6     2024-08-23      LTN   2026-04-01        0.114997  840.232741
7     2024-08-23      LTN   2026-07-01        0.115265  818.020491
8     2024-08-23      LTN   2026-10-01        0.115357  795.185488
9     2024-08-23      LTN   2027-07-01        0.115335  733.981131
10    2024-08-23      LTN   2028-01-01        0.115694  693.647778
11    2024-08-23      LTN   2028-07-01        0.116417    655.6398
12    2024-08-23      LTN   2030-01-01        0.117436  554.331151

# Calculate the quotation of a NTN-B bond as per ANBIMA's rules
>>> ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)
99.3651
>>> ntnb.quotation("31-05-2024", "15-08-2060", 0.061878)
99.5341

# Calculate the DI Spread of NTN-F and LTN bonds in a given date
>>> ntnf.di_spreads("23-08-2024")
    MaturityDate  DISpread
0   2025-01-01     -5.38
1   2027-01-01      4.39
2   2029-01-01      7.37
3   2031-01-01     12.58
4   2033-01-01      7.67
5   2035-01-01     12.76

```

### Business Days Tools (Brazilian holidays are automatically considered)
```python
>>> from pyield import bday
# Count the number of business days between two dates
# Start date is included, end date is excluded
>>> bday.count(start='29-12-2023', end='02-01-2024')
1

# Get the next business day after a given date (offset=1)
>>> bday.offset(dates="29-12-2023", offset=1)
Timestamp('2024-01-02 00:00:00')

# Get the next business day if it is not a business day (offset=0)
>>> bday.offset(dates="30-12-2023", offset=0)
Timestamp('2024-01-02 00:00:00')

# Since 29-12-2023 is a business day, it returns the same date (offset=0)
>>> bday.offset(dates="29-12-2023", offset=0)
Timestamp('2023-12-29 00:00:00')

# Generate a pandas series with the business days between two dates
>>> bday.generate(start='29-12-2023', end='03-01-2024')
0   2023-12-29
1   2024-01-02
2   2024-01-03
```

### Futures Data
```python
>>> import pyield as yd

# Fetch historical DI Futures data from B3
>>> yd.futures(contract_code="DI1", date='08-03-2024')
TradeDate  TickerSymbol ExpirationDate BDaysToExp ... LastRate LastAskRate LastBidRate SettlementRate
2024-03-08       DI1J24     2024-04-01         15 ...   10.952      10.952      10.956         10.956
2024-03-08       DI1K24     2024-05-02         37 ...   10.776      10.774      10.780         10.777
2024-03-08       DI1M24     2024-06-03         58 ...   10.604      10.602      10.604         10.608
       ...          ...            ...        ... ...      ...         ...         ...            ...
2024-03-08       DI1F37     2037-01-02       3213 ...     <NA>        <NA>        <NA>         10.859
2024-03-08       DI1F38     2038-01-04       3462 ...     <NA>        <NA>        <NA>         10.859
2024-03-08       DI1F39     2039-01-03       3713 ...     <NA>        <NA>        <NA>         10.85

# Fetch current DI Futures data from B3 (15 minutes delay)
>>> yd.futures(contract_code="DI1", date="21-03-2024")  # when the date used is the current date and market is open
LastUpdatee      TickerSymbol ExpirationDate BDaysToExp ... MaxRate LastAskRate LastBidRate LastRate
2024-04-21 13:37:39       DI1K24     2024-05-02          7 ... 0.10660     0.10652     0.10660  0.10660
2024-04-21 13:37:39       DI1M24     2024-06-03         28 ... 0.10518     0.10510     0.10516  0.10518
2024-04-21 13:37:39       DI1N24     2024-07-01         48 ... 0.10480     0.10456     0.10462  0.10460
                ...          ...            ...        ... ...     ...         ...         ...      ...
2024-04-21 13:37:39       DI1F37     2037-01-02       3183 ...    <NA>        <NA>     0.11600     <NA>
2024-04-21 13:37:39       DI1F38     2038-01-04       3432 ...    <NA>        <NA>     0.11600     <NA>
2024-04-21 13:37:39       DI1F39     2039-01-03       3683 ...    <NA>        <NA>        <NA>     <NA>
```

### Indicators Data
```python
>>> from pyield import bc

# Fetch the SELIC target rates from the Central Bank of Brazil
>>> bc.selic_over_series("26-01-2025", "30-01-2025")  # No data on 26-01-2025 (sunday)
        Date   Value
0 2025-01-27  0.1215
1 2025-01-28  0.1215
2 2025-01-29  0.1215
3 2025-01-30  0.1315

# Fetch the SELIC target rate for a specific date
>>> bc.selic_over("27-01-2025")
        Date   Value
0.1215  # 12.15%
```

### Projections Data
```python
>>> from pyield import anbima
# Fetch current month projection for IPCA from IBGE API
>>> ipca = anbima.ipca_projection()
>>> print(ipca)
IndicatorProjection(
    last_updated=Timestamp('2024-04-19 18:55:00'),
    reference_month_ts=Timestamp('2024-04-01 00:00:00'),
    reference_month_br='ABR/2024',
    projected_value=0.0035  # 0.35%
)
>>> ipca.projected_value
0.0035  # 0.35%
```

### Interpolation Tools
```python
>>> from pyield import Interpolator
# Interpolate interest rates for specific business days using the Interpolator class.

# Initialize the Interpolator with known business days and interest rates.
>>> known_bdays = [30, 60, 90]
>>> known_rates = [0.045, 0.05, 0.055]
>>> linear_interpolator = Interpolator("linear", known_bdays, known_rates)

# Interpolate the interest rate for a given number of business days.
>>> linear_interpolator(45)
0.0475

# Use the flat forward method for interpolation.
>>> ff_interpolator = Interpolator("flat_forward", known_bdays, known_rates)
>>> ff_interpolator(45)
0.04833068080970859
```
