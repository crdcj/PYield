from types import ModuleType

import pytest

import pyield as yd
from pyield import rmd


@pytest.mark.parametrize("nome", ["lft", "ltn", "ntnb", "ntnb1", "ntnbp", "ntnc", "ntnf"])
def test_titulos_expostos_apenas_na_raiz(nome):
    modulo = getattr(__import__("pyield", fromlist=[nome]), nome)
    assert isinstance(modulo, ModuleType)
    assert modulo is getattr(yd, nome)
    assert callable(modulo.pu)
    assert nome in yd.__all__
    assert nome not in yd.tpf.__all__
    assert not hasattr(yd.tpf, nome)


def test_rmd_exposto_como_funcao_na_raiz():
    assert rmd is yd.rmd
    assert callable(rmd)
    assert "rmd" in yd.__all__
    assert "rmd" not in yd.tpf.__all__
    assert not callable(getattr(yd.tpf, "rmd", None))
    with pytest.raises(ValueError, match="não disponível"):
        rmd(aba="inexistente")
