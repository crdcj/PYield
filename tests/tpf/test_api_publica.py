from types import ModuleType

import pytest

import pyield as yd


@pytest.mark.parametrize("nome", ["lft", "ltn", "ntnb", "ntnb1", "ntnbp", "ntnc", "ntnf"])
def test_titulos_expostos_apenas_na_raiz(nome):
    modulo = getattr(__import__("pyield", fromlist=[nome]), nome)
    assert isinstance(modulo, ModuleType)
    assert modulo is getattr(yd, nome)
    assert callable(modulo.pu)
    assert nome in yd.__all__
    assert nome not in yd.tpf.__all__
    assert not hasattr(yd.tpf, nome)
