import pandas as pd
import polars as pl
import pytest

import pyield


@pytest.fixture(autouse=True)
def add_np(doctest_namespace):
    doctest_namespace["yd"] = pyield
    doctest_namespace["pd"] = pd


@pytest.fixture(scope="session", autouse=True)
def set_pandas_display_options():
    """
    Define as opções de exibição do Pandas para a sessão de testes.
    Isso afeta como os DataFrames são impressos nos doctests.
    """
    pl.Config.set_tbl_width_chars(150)  # largura grande


def pytest_configure(config):
    config.addinivalue_line("doctest_optionflags", "ELLIPSIS")
    config.addinivalue_line("doctest_optionflags", "NORMALIZE_WHITESPACE")
