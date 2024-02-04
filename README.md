[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.11-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](#license)

# PYield: Brazilian Fixed Income Analysis Library

## Introduction

Welcome to PYield, a Python library designed for the analysis of fixed income instruments in Brazil. This library is tailored for financial analysts, researchers, and enthusiasts interested in the Brazilian fixed income market. Leveraging the power of popular Python libraries like Pandas and Requests, PYield simplifies the process of obtaining and processing data from key sources such as Tesouro Nacional (TN), Banco Central (BC), ANBIMA, and B3.
## Features

- Data Collection: Automated fetching of data from TN, BC, ANBIMA, and B3.
- Data Processing: Efficient processing and normalization of fixed income data.
- Analysis Tools: Built-in functions for common analysis tasks in fixed income markets.
- Easy Integration: Seamless integration with Python data analysis workflows.

## Installation

You can install PYield using pip:
```sh
pip install pyield
```
## How to use PYield
### Getting DI Futures Data
```python
import pyield as pyd

# Get a pandas dataframe with the DI raw data from B3 (first date available is 05-06-1991)
>>> yd.get_di(reference_date='2024-01-15', raw=True)
VENCTO CONTR. ABERT.(1) ... ÚLT.OF. COMPRA  ÚLT.OF. VENDA
   G24           796903 ...         0.11650         0.11656
   H24           548377 ...         0.11346         0.11352
   ...              ... ...            ...            ...

# Get a pandas dataframe with the DI processed data from B3 (default)
>>> yd.get_di(reference_date='2024-01-15')
contract_code expiration bdays ... last_offer  settlement_rate
          G24 2024-02-01    13 ...     0.11656           0.11650
          H24 2024-03-01    32 ...     0.11352           0.11349
          ...        ...   ... ...        ...              ...
```
### Business Days Tools (Brazilian holidays are automatically considered)
```python
# Generate a pandas series with the business days between two dates
>>> yd.generate_bdays(start='2023-12-29', end='2024-01-03')
0   2023-12-29
1   2024-01-02
2   2024-01-03
dtype: datetime64[ns]

# Get the next business day after a given date (offset=1)
>>> yd.offset_bdays(dates="2023-12-29", offset=1)
Timestamp('2024-01-02 00:00:00')

# Get the next business day if it is not a business day (offset=0)
>>> yd.offset_bdays(dates="2023-12-30", offset=0)
Timestamp('2024-01-02 00:00:00')

# Since 2023-12-29 is a business day, it returns the same date (offset=0)
>>> yd.offset_bdays(dates="2023-12-29", offset=0)
Timestamp('2023-12-29 00:00:00')


# Count the number of business days between two dates
# Start date is included, end date is excluded
>>> yd.count_bdays(start='2023-12-29', end='2024-01-02')
1

```

## Documentation

For detailed documentation on all features and functionalities, please visit PYield Documentation.
Contributing

Contributions to PYield are welcome! Please read our Contributing Guidelines for details on how to submit pull requests, report issues, or suggest enhancements.
License

PYield is licensed under the MIT License.
Acknowledgments

PYield was developed with the support of the Python community and financial analysts in Brazil. Special thanks to the maintainers of Pandas and Requests for their invaluable libraries.
