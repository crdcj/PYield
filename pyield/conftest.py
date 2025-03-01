import pytest

import pyield


@pytest.fixture(autouse=True)
def add_np(doctest_namespace):
    doctest_namespace["yd"] = pyield


def pytest_configure(config):
    config.addinivalue_line("doctest_optionflags", "ELLIPSIS")
    config.addinivalue_line("doctest_optionflags", "NORMALIZE_WHITESPACE")
