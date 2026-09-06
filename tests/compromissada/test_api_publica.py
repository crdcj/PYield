"""Testes da superfície pública de operações compromissadas."""

import pyield as yd
import pyield.compromissada as modulo_compromissada


def test_compromissadas_exposta_na_raiz():
    assert yd.compromissadas is modulo_compromissada.compromissadas
    assert callable(yd.compromissadas)
    assert "compromissadas" in yd.__all__


def test_compromissadas_nao_e_exposta_em_selic():
    assert not hasattr(yd.selic, "compromissada")
    assert not hasattr(yd.selic, "compromissadas")
