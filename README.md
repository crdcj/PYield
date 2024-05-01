[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.11-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](#license)

# PYield: Brazilian Fixed Income Toolkit

## Introduction

Welcome to PYield, a Python library designed for the analysis of fixed income instruments in Brazil. This library is tailored for financial analysts, researchers, and enthusiasts interested in the Brazilian fixed income market. Leveraging the power of popular Python libraries like Pandas and Requests, PYield simplifies the process of obtaining and processing data from key sources such as ANBIMA, BCB, IBGE and B3.

## Features

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

## How to use PYield

### Business Days Tools (Brazilian holidays are automatically considered)
```python
>>> import pyield as yd

# Count the number of business days between two dates
# Start date is included, end date is excluded
>>> yd.count_bdays(start='2023-12-29', end='2024-01-02')
1

# Get the next business day after a given date (offset=1)
>>> yd.offset_bdays(dates="2023-12-29", offset=1)
Timestamp('2024-01-02 00:00:00')

# Get the next business day if it is not a business day (offset=0)
>>> yd.offset_bdays(dates="2023-12-30", offset=0)
Timestamp('2024-01-02 00:00:00')

# Since 2023-12-29 is a business day, it returns the same date (offset=0)
>>> yd.offset_bdays(dates="2023-12-29", offset=0)
Timestamp('2023-12-29 00:00:00')

# Generate a pandas series with the business days between two dates
>>> yd.generate_bdays(start='2023-12-29', end='2024-01-03')
0   2023-12-29
1   2024-01-02
2   2024-01-03
dtype: datetime64[ns]
```

### Futures Data
```python
# Fetch current DI Futures data from B3 (15 minutes delay)
>>> yd.fetch_asset(asset_code="DI1")
TradeTime      TickerSymbol ExpirationDate BDaysToExp ... MaxRate LastAskRate LastBidRate CurrentRate
2024-04-21 13:37:39       DI1K24     2024-05-02          7 ... 0.10660     0.10652     0.10660  0.10660
2024-04-21 13:37:39       DI1M24     2024-06-03         28 ... 0.10518     0.10510     0.10516  0.10518
2024-04-21 13:37:39       DI1N24     2024-07-01         48 ... 0.10480     0.10456     0.10462  0.10460
                ...          ...            ...        ... ...     ...         ...         ...      ...
2024-04-21 13:37:39       DI1F37     2037-01-02       3183 ...    <NA>        <NA>     0.11600     <NA>
2024-04-21 13:37:39       DI1F38     2038-01-04       3432 ...    <NA>        <NA>     0.11600     <NA>
2024-04-21 13:37:39       DI1F39     2039-01-03       3683 ...    <NA>        <NA>        <NA>     <NA>

# Fetch historical DI Futures data from B3
>>> yd.fetch_asset(asset_code="DI1", reference_date='2024-03-08')
TradeDate  TickerSymbol ExpirationDate BDaysToExp ... LastRate LastAskRate LastBidRate SettlementRate
2024-03-08       DI1J24     2024-04-01         15 ...   10.952      10.952      10.956         10.956
2024-03-08       DI1K24     2024-05-02         37 ...   10.776      10.774      10.780         10.777
2024-03-08       DI1M24     2024-06-03         58 ...   10.604      10.602      10.604         10.608
       ...          ...            ...        ... ...      ...         ...         ...            ...
2024-03-08       DI1F37     2037-01-02       3213 ...     <NA>        <NA>        <NA>         10.859
2024-03-08       DI1F38     2038-01-04       3462 ...     <NA>        <NA>        <NA>         10.859
2024-03-08       DI1F39     2039-01-03       3713 ...     <NA>        <NA>        <NA>         10.85
```

### Treasury Bonds Data
```python
# Fetch a DataFrame with the NTN-B data from ANBIMA
# Anbima data is available for the last 5 working days
# Obs: Anbima members have access to the full history
>>> yd.fetch_asset(asset_code="NTN-B", reference_date='2024-04-12')

BondType ReferenceDate MaturityDate BidRate AskRate IndicativeRate       Price
   NTN-B    2024-04-12   2024-08-15 0.07540 0.07504        0.07523 4,271.43565
   NTN-B    2024-04-12   2025-05-15 0.05945 0.05913        0.05930 4,361.34391
   NTN-B    2024-04-12   2026-08-15 0.05927 0.05897        0.05910 4,301.40082
     ...           ...          ...     ...     ...            ...         ...
   NTN-B    2024-04-12   2050-08-15 0.06039 0.06006        0.06023 4,299.28233
   NTN-B    2024-04-12   2055-05-15 0.06035 0.05998        0.06017 4,367.13360
   NTN-B    2024-04-12   2060-08-15 0.06057 0.06016        0.06036 4,292.26323
```

### Spreads Calculation
```python
# Calculate the spread between two DI Futures contracts and the pre-fix bonds
>>> yd.calculate_spreads(spread_type="di_vs_pre", reference_date="2024-4-11")

BondType ReferenceDate MaturityDate  DISpread
     LTN    2024-04-11   2024-07-01    -20.28
     LTN    2024-04-11   2024-10-01    -10.19
     LTN    2024-04-11   2025-01-01    -15.05
   ...      ...           ...           ...
   NTN-F    2024-04-11   2031-01-01     -0.66
   NTN-F    2024-04-11   2033-01-01     -5.69
   NTN-F    2024-04-11   2035-01-01     -1.27
```

### Indicators Data
```python
# Fetch the SELIC target rate from the Central Bank of Brazil
>>> yd.fetch_indicator(indicator_code="SELIC", reference_date='2024-04-12')
0.1075  # 10.75%

# Fetch the IPCA monthly inflation rate from IBGE
>>> yd.fetch_indicator(indicator_code="IPCA", reference_date='2024-03-18')
0.16  # 0.16%

# If no data is yet available for the indicator, the function returns None
>>> yd.fetch_indicator(indicator_code="IPCA", reference_date='2024-04-10')
None
```

### Projections Data
```python
# Fetch current month projection for IPCA from IBGE API
>>> ipca = yd.fetch_projection(projection_code="IPCA_CM")
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

