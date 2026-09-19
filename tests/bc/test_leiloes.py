import datetime as dt
import importlib
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal

modulo_leiloes = importlib.import_module("pyield.bc.leiloes")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "leiloes_20250819.csv"
CAMINHO_PARQUET = DIRETORIO_DADOS / "leiloes_20250819.parquet"

# PTAX do dia 2025-08-19 usada na geração do Parquet de referência
DF_PTAX_REFERENCIA = pl.DataFrame(
    {"data_ref": [dt.date(2025, 8, 19)], "ptax": [5.4716]},
    schema={"data_ref": pl.Date, "ptax": pl.Float64},
)


def test_pipeline_leiloes(monkeypatch):
    """leiloes com monkeypatch deve produzir o Parquet de referência."""
    monkeypatch.setattr(
        modulo_leiloes,
        "buscar_csv",
        lambda *_: CAMINHO_CSV.read_bytes(),
    )
    monkeypatch.setattr(modulo_leiloes, "_buscar_ptax", lambda *_: DF_PTAX_REFERENCIA)
    resultado = modulo_leiloes.leiloes(
        inicio="19-08-2025",
        fim="19-08-2025",
    )
    referencia = pl.read_parquet(CAMINHO_PARQUET)
    colunas_dv01 = ["dv01_1v", "dv01_2v", "dv01_total"]
    assert_frame_equal(
        resultado.drop(colunas_dv01), referencia.drop(colunas_dv01), check_exact=True
    )
    # A escala dos fluxos pode alterar os últimos bits do DV01 agregado.
    assert_frame_equal(
        resultado.select(colunas_dv01),
        referencia.select(colunas_dv01),
        rel_tol=1e-12,
        abs_tol=1e-12,
    )
