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
## Usage

Here is a quick example of how to use PYield:

```python
import pyield as yd

# Get DI raw data from B3
df_di_raw = yd.di(reference_date='28-12-2023', raw=True)

# Get DI processed data from B3 (default)
df_di = yd.di(reference_date='28-12-2023')
```

## Example Output

Below is an example of the output from PYield when fetching DI data from B3 for the reference date of 28-12-2023:
```text
contract_code expiration  bdays  ...  last_bid  last_offer  settlement_rate
          F24 2024-01-02      2  ...    11.636      11.650           11.634
          G24 2024-02-01     24  ...    11.644      11.646           11.648
          H24 2024-03-01     43  ...      -         11.426           11.425
          J24 2024-04-01     63  ...    11.285      11.290           11.290
          K24 2024-05-02     85  ...      -           -              11.121
          M24 2024-06-03    106  ...    10.900      11.000           10.960
          ...        ...    ...  ...       ...         ...              ...
          F33 2033-01-03   2260  ...    10.350      10.380           10.363
          F34 2034-01-02   2511  ...      -           -              10.365
          F35 2035-01-02   2759  ...      -           -              10.389
          F36 2036-01-02   3008  ...      -           -              10.395
          F37 2037-01-02   3261  ...      -           -              10.414
          F38 2038-01-04   3510  ...      -           -              10.414
```

## Documentation

For detailed documentation on all features and functionalities, please visit PYield Documentation.
Contributing

Contributions to PYield are welcome! Please read our Contributing Guidelines for details on how to submit pull requests, report issues, or suggest enhancements.
License

PYield is licensed under the MIT License.
Acknowledgments

PYield was developed with the support of the Python community and financial analysts in Brazil. Special thanks to the maintainers of Pandas and Requests for their invaluable libraries.
