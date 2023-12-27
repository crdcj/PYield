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
df_di_raw = yd.di(reference_date='15-12-2023', raw=True)

# Get DI processed data from B3 (default)
df_di = yd.di(reference_date='15-12-2023')
```

## Example Output

Below is an example of the output from PYield when fetching DI data from B3 for the reference date of 26-12-2023:
```text
contract_code   maturity  bdays  ...  closing_rate  last_bid  last_offer  settlement_rate
          F24 2024-01-01      4  ...        11.642    11.642      11.646           11.644
          G24 2024-02-01     26  ...        11.650    11.648      11.652           11.652
          H24 2024-03-01     45  ...        11.440    11.432      11.440           11.436
          J24 2024-04-01     65  ...        11.300    11.290      11.300           11.300
          K24 2024-05-01     87  ...        11.140         -      11.140           11.133
          M24 2024-06-01    108  ...        10.965    10.950      10.965           10.955
          ...        ...    ...  ...           ...       ...         ...              ...
          F33 2033-01-01   2262  ...        10.330    10.320      10.340           10.331
          F34 2034-01-01   2513  ...        10.330    10.290      10.390           10.320
          F35 2035-01-01   2761  ...             -     9.990           -           10.344
          F36 2036-01-01   3010  ...             -         -           -           10.363
          F37 2037-01-01   3263  ...             -         -           -           10.382
          F38 2038-01-01   3512  ...             -    10.380           -           10.382
```

## Documentation

For detailed documentation on all features and functionalities, please visit PYield Documentation.
Contributing

Contributions to PYield are welcome! Please read our Contributing Guidelines for details on how to submit pull requests, report issues, or suggest enhancements.
License

PYield is licensed under the MIT License.
Acknowledgments

PYield was developed with the support of the Python community and financial analysts in Brazil. Special thanks to the maintainers of Pandas and Requests for their invaluable libraries.
