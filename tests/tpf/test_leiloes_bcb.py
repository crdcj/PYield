import datetime as dt
import importlib
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import pyield as yd

modulo_bcb = importlib.import_module("pyield.tpf._leiloes_bcb")
modulo_tn = importlib.import_module("pyield.tpf.leiloes")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "leiloes_bcb_20250819.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "leiloes_bcb_20250819.parquet"

# PTAX do dia 2025-08-19 usada na geração do Parquet de referência
DF_PTAX_REFERENCIA = pl.DataFrame(
    {"data_ref": [dt.date(2025, 8, 19)], "ptax": [5.4716]},
    schema={"data_ref": pl.Date, "ptax": pl.Float64},
)


@pytest.fixture
def resultado(monkeypatch) -> pl.DataFrame:
    monkeypatch.setattr(modulo_bcb, "_buscar_csv", lambda *_: CAMINHO_CSV.read_bytes())
    monkeypatch.setattr(modulo_tn, "_buscar_ptax", lambda *_: DF_PTAX_REFERENCIA)
    return yd.tpf.leiloes_bcb(data="19-08-2025")


def test_pipeline_leiloes_bcb(resultado):
    """leiloes_bcb com monkeypatch deve reproduzir o Parquet de referência."""
    assert_frame_equal(resultado, pl.read_parquet(CAMINHO_PARQUET))


def test_colunas_alinhadas_ao_tesouro(resultado):
    """Colunas comuns têm o mesmo nome e tipo das de yd.tpf.leiloes."""
    exclusivas_bcb = {"tipo_publico", "codigo_selic"}
    comuns = [c for c in modulo_tn.ORDEM_FINAL_COLUNAS if c in resultado.columns]
    assert set(resultado.columns) - set(comuns) == exclusivas_bcb
    assert [c for c in resultado.columns if c not in exclusivas_bcb] == comuns


def test_leiloes_bcb_rejeita_modos_temporais_ambiguos():
    with pytest.raises(ValueError, match="data não pode ser combinado"):
        yd.tpf.leiloes_bcb(data="19-08-2025", inicio="19-08-2025")
    with pytest.raises(ValueError, match="fim só pode ser usado"):
        yd.tpf.leiloes_bcb(fim="19-08-2025")
