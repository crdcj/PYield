import pytest

import pyield


@pytest.fixture(autouse=True)
def add_np(doctest_namespace):
    doctest_namespace["yd"] = pyield
