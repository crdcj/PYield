import pandas as pd
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
    pd.set_option("display.max_columns", 8)  # Exibe até 8 colunas
    pd.set_option("display.width", 1000)  # Define a largura máxima do display
    pd.set_option("display.max_rows", 20)  # Exibe até 20 linhas
    pd.set_option("display.max_colwidth", None)


def pytest_configure(config):
    config.addinivalue_line("doctest_optionflags", "ELLIPSIS")
    config.addinivalue_line("doctest_optionflags", "NORMALIZE_WHITESPACE")
