import pytest

from pyield import Interpolador


def test_interpolador_rejeita_curva_sem_vertices_validos():
    with pytest.raises(ValueError, match="ao menos um vértice válido"):
        Interpolador([1, 2], [None, float("nan")], "flat_forward")
