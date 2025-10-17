import datetime as dt

import numpy as np
import pandas as pd
import polars as pl

DateScalar = str | np.datetime64 | pd.Timestamp | dt.datetime | dt.date
DateArray = (
    pd.DatetimeIndex
    | pd.Series
    | pl.Series
    | np.ndarray
    | list[DateScalar]
    | tuple[DateScalar, ...]
)
FloatArray = np.ndarray | list[float] | tuple[float, ...] | pd.Series | pl.Series
