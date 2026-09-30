import pyield as yd
import pyield.cpm as modulo_cpm
from pyield.cpm import _contratos, _probabilidades  # noqa: PLC2701


def test_cpm_e_um_namespace_de_topo() -> None:
    assert yd.cpm is modulo_cpm
    assert yd.cpm.contratos is _contratos.contratos
    assert yd.cpm.probabilidades is _probabilidades.probabilidades
    assert not hasattr(yd.cpm, "data")
    assert not hasattr(yd.cpm, "probabilities")


def test_cpm_nao_e_exposto_em_selic() -> None:
    assert not hasattr(yd.selic, "cpm")
    assert not hasattr(yd.selic, "probabilities")
