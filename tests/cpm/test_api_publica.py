import pyield as yd
import pyield.cpm as modulo_cpm
import pyield.cpm.probabilidades as modulo_probabilidades


def test_cpm_e_um_namespace_de_topo() -> None:
    assert yd.cpm is modulo_cpm
    assert yd.cpm.probabilidades is modulo_probabilidades
    assert not hasattr(yd.cpm, "probabilities")


def test_cpm_nao_e_exposto_em_selic() -> None:
    assert not hasattr(yd.selic, "cpm")
    assert not hasattr(yd.selic, "probabilities")
