import pyield as yd
from pyield import anbima, b3, bc


def test_namespace_futuro_exposto_na_raiz():
    assert callable(yd.futuro.historico)
    assert callable(yd.futuro.intradia)
    assert callable(yd.futuro.enriquecer)
    assert callable(yd.futuro.datas_disponiveis)


def test_namespace_tpf_exposto_na_raiz():
    assert callable(yd.tpf.taxas)
    assert callable(yd.tpf.vencimentos)
    assert callable(yd.tpf.estoque)
    assert callable(yd.tpf.leilao)
    assert callable(yd.tpf.secundario_intradia)
    assert callable(yd.tpf.secundario_mensal)


def test_indicadores_simples_expostos_na_raiz():
    assert callable(yd.di_over)
    assert callable(yd.di1.dados)
    assert callable(yd.di1.interpolar_taxa)
    assert callable(yd.di1.interpolar_taxas)
    assert callable(yd.di1.datas_disponiveis)
    assert callable(yd.ptax)
    assert callable(yd.ptax_serie)
    assert callable(yd.selic_meta)
    assert callable(yd.selic_meta_serie)
    assert callable(yd.selic_over)
    assert callable(yd.selic_over_serie)


def test_aliases_de_fonte_removidos_da_api_publica():
    assert "di_over" not in b3.__all__
    assert "futuro" not in b3.__all__
    assert "futuro_intradia" not in b3.__all__
    assert "futuro_datas_disponiveis" not in b3.__all__
    assert "futuro_enriquecer" not in b3.__all__
    assert "di1" not in b3.__all__

    assert not callable(getattr(b3, "di_over", None))
    assert not callable(getattr(b3, "futuro", None))
    assert not hasattr(b3, "futuro_intradia")
    assert not hasattr(b3, "futuro_datas_disponiveis")
    assert not hasattr(b3, "futuro_enriquecer")

    assert "ptax" not in bc.__all__
    assert "ptax_serie" not in bc.__all__
    assert "selic_meta" not in bc.__all__
    assert "selic_meta_serie" not in bc.__all__
    assert "selic_over" not in bc.__all__
    assert "selic_over_serie" not in bc.__all__
    assert "tpf_intradia" not in bc.__all__
    assert "tpf_mensal" not in bc.__all__
    assert "vna_lft" not in bc.__all__

    assert not callable(getattr(bc, "ptax", None))
    assert not callable(getattr(bc, "ptax_serie", None))
    assert not callable(getattr(bc, "selic_meta", None))
    assert not callable(getattr(bc, "selic_meta_serie", None))
    assert not callable(getattr(bc, "selic_over", None))
    assert not callable(getattr(bc, "selic_over_serie", None))
    assert not callable(getattr(bc, "tpf_intradia", None))
    assert not callable(getattr(bc, "tpf_mensal", None))
    assert not callable(getattr(bc, "vna_lft", None))

    assert "imaq" not in anbima.__all__
    assert "tpf" not in anbima.__all__
    assert "tpf_vencimentos" not in anbima.__all__
    assert "tpf_fonte" in anbima.__all__
    assert "leilao" not in yd.tn.__all__
