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

## Documentation

For detailed documentation on all features and functionalities, please visit PYield Documentation.
Contributing

Contributions to PYield are welcome! Please read our Contributing Guidelines for details on how to submit pull requests, report issues, or suggest enhancements.
License

PYield is licensed under the MIT License.
Acknowledgments

PYield was developed with the support of the Python community and financial analysts in Brazil. Special thanks to the maintainers of Pandas and Requests for their invaluable libraries.
