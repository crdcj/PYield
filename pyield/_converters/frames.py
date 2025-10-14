from typing import Literal

import pandas as pd
import polars as pl


def format_output(
    data: pl.DataFrame | pl.Series,
    return_format: Literal["pandas", "polars"],
) -> pd.DataFrame | pd.Series | pl.DataFrame | pl.Series:
    """Converte um objeto Polars para o formato de saída especificado."""
    if return_format == "polars":
        return data
    if return_format == "pandas":
        return data.to_pandas(use_pyarrow_extension_array=True)
    raise ValueError(f"Formato inválido: '{return_format}'.")
