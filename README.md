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
>>> yd.get_di(trade_date='2024-03-08', raw=True)
VENCTO  CONTR. ABERT.(1)  CONTR. FECH.(2)  NÚM. NEGOC.  ...     AJUSTE  VAR. PTOS.  ÚLT.OF. COMPRA  ÚLT.OF. VENDA
   J24           3608626          3608626          294  ... 99,383.080       0,17-          10.952         10.956
   K24            607759           607759          179  ... 98,508.490       0,03+          10.774         10.780
   M24            339641           339641          194  ... 97,706.210       1,68-          10.602         10.604
   ...               ...              ...          ...  ...        ...         ...             ...            ...
   F37              3517             3517            0  ... 26,863.980     336,32-           0.000          0.000
   F38              2200             2200            0  ... 24,262.320     327,41-           0.000          0.000
   F39                 0                0            0  ... 21,894.700     317,01-           0.000          0.000

# Get a pandas dataframe with the DI processed data from B3 (default)
>>> yd.get_di(trade_date='2024-03-08')
 TradeDate ExpirationCode ExpirationDate  BDToExpiration  ...  LastRate  LastAskRate  LastBidRate  SettlementRate
2024-03-08            J24     2024-04-01              15  ...    10.952       10.952       10.956          10.956
2024-03-08            K24     2024-05-02              37  ...    10.776       10.774       10.780          10.777
2024-03-08            M24     2024-06-03              58  ...    10.604       10.602       10.604          10.608
       ...            ...            ...             ...  ...       ...          ...          ...             ...
2024-03-08            F37     2037-01-02            3213  ...      <NA>         <NA>         <NA>          10.859
2024-03-08            F38     2038-01-04            3462  ...      <NA>         <NA>         <NA>          10.859
2024-03-08            F39     2039-01-03            3713  ...      <NA>         <NA>         <NA>          10.85
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
