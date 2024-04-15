from pathlib import Path
from typing import Literal

import pandas as pd

from . import web, xml


def fetch_df(
    trade_date: pd.Timestamp,
    source_type: Literal["bmf", "b3", "b3s"] = "bmf",
    return_raw: bool = False,
) -> pd.DataFrame:
    """
    Fetches DI futures data for a specified trade date from B3.

     Retrieves and processes DI futures data from B3 for a given trade date. This
     function serves as the primary method for accessing DI data, with options to
     specify the source of the data and whether to return raw data.

     Args:
        trade_date (pd.Timestamp): The trade date for which to fetch DI data.
        source_type (Literal["bmf", "b3", "b3s"], optional): Indicates the source of
            the data. Defaults to "bmf". Options include:
                - "bmf": Fetches data from the old BM&FBOVESPA website. Fastest option.
                - "b3": Fetches data from the complete Price Report (XML file) provided
                    by B3.
                - "b3s": Fetches data from the simplified Price Report (XML file)
                    provided by B3. Faster than "b3" but less detailed.
        return_raw (bool, optional): If True, returns the raw DI data without
            processing.

     Returns:
         pd.DataFrame: A DataFrame containing the DI futures data for the specified
         trade date. Format and content depend on the source_type and return_raw flag.

     Examples:
         # Fetch DI data for the previous business day using default settings
         >>> get_di()

         # Fetch DI data for a specific trade date from the simplified B3 Price Report
         >>> get_di("2023-12-28", source_type="b3s")

     Notes:
         - Complete Price Report XML files are about 5 MB in size.
         - Simplified Price Report XML files are significantly smaller, around 50 kB.
         - For file specifications, refer to the B3 documentation: [B3 File Specs](https://www.b3.com.br/data/files/16/70/29/9C/6219D710C8F297D7AC094EA8/Catalogo_precos_v1.3.pdf)
    """

    if source_type == "bmf":
        return web.fetch_di(trade_date, return_raw)
    elif source_type in ["b3", "b3s"]:
        return xml.read_xml(trade_date, source_type, return_raw)
    else:
        raise ValueError("source_type must be either 'bmf', 'b3' or 'b3s'.")


def read_file(file_path: Path, return_raw: bool = False) -> pd.DataFrame:
    """
    Reads DI futures data from a file and returns it as a pandas DataFrame.

    This function opens and reads a DI futures data file, returning the contents as a
    pandas DataFrame. It supports reading from both XML files provided by B3, wich
    are the simplified and complete Price Reports.

    Args:
        file_path (Path): The file path to the DI data file. This should be a valid
            Path object pointing to the location of the file.
        return_raw (bool, optional): If set to True, the function returns the raw data
            without applying any transformation or processing. Useful for cases where
            raw data inspection or custom processing is needed. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the processed or raw DI futures data,
            depending on the `return_raw` flag.

    Examples:
        >>> read_di(Path("path/to/di_data_file.xml"))
        # returns a DataFrame with the DI futures data

        >>> read_di(Path("path/to/di_data_file.xml"), return_raw=True)
        # returns a DataFrame with the raw DI futures data, without processing

    Note:
        The ability to process and return raw data is primarily intended for advanced
        users who require access to the data in its original form for custom analyses.
    """
    return xml.read_di(file_path, return_raw=return_raw)
