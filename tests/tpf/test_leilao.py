import datetime as dt
import importlib
import json
from pathlib import Path

import polars as pl
import pytest

modulo_leiloes = importlib.import_module("pyield.tpf.leiloes")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_JSON = DIRETORIO_DADOS / "leilao_20251023.json"
CAMINHO_JSON_20260616 = DIRETORIO_DADOS / "leilao_20260616.json"
CAMINHO_PARQUET = DIRETORIO_DADOS / "leilao_20251023.parquet"

# PTAX dos dias 22, 23 e 24/10/2025 usada na geração do Parquet de referência
DF_PTAX_REFERENCIA = pl.DataFrame(
    {
        "data_ref": [
            dt.date(2025, 10, 22),
            dt.date(2025, 10, 23),
            dt.date(2025, 10, 24),
        ],
        "ptax": [5.3898, 5.384, 5.3797],
    },
    schema={"data_ref": pl.Date, "ptax": pl.Float64},
)


def test_pipeline_leiloes_por_data(monkeypatch):
    """tpf.leiloes() com monkeypatch deve produzir o parquet de referência."""
    monkeypatch.setattr(
        modulo_leiloes,
        "_buscar_dados_leiloes",
        lambda *_, **__: json.loads(CAMINHO_JSON.read_bytes()),
    )
    monkeypatch.setattr(
        modulo_leiloes,
        "_buscar_ptax",
        lambda *_, **__: DF_PTAX_REFERENCIA,
    )
    resultado = modulo_leiloes.leiloes(data="23-10-2025")
    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))


def test_leiloes_processa_colunas_novas(monkeypatch):
    """Colunas novas da API do Tesouro devem ser expostas no DataFrame final."""
    monkeypatch.setattr(
        modulo_leiloes,
        "_buscar_dados_leiloes",
        lambda *_, **__: json.loads(CAMINHO_JSON_20260616.read_bytes()),
    )
    monkeypatch.setattr(
        modulo_leiloes,
        "_buscar_ptax",
        lambda *_, **__: pl.DataFrame(
            {
                "data_ref": [dt.date(2026, 6, 16)],
                "ptax": [5.078],
            },
            schema={"data_ref": pl.Date, "ptax": pl.Float64},
        ),
    )

    resultado = modulo_leiloes.leiloes(data="16-06-2026")
    linha_lft = resultado.filter(
        (pl.col("titulo") == "LFT")
        & (pl.col("data_vencimento") == dt.date(2032, 6, 1))
    ).row(0, named=True)
    quantidade_liquidada_1v = 1_000_000
    quantidade_liquidada_2v = 3_000

    assert resultado["tipo_ocorrencia"].unique().to_list() == ["Ordinário"]
    assert linha_lft["quantidade_liquidada_1v"] == quantidade_liquidada_1v
    assert linha_lft["quantidade_liquidada_2v"] == quantidade_liquidada_2v


def test_leiloes_inicio_filtra_localmente(monkeypatch):
    """inicio aplica o recorte por data_1v."""
    dados_23 = json.loads(CAMINHO_JSON.read_bytes())
    dados_28 = [{**registro, "data_leilao": "28/10/2025"} for registro in dados_23]
    monkeypatch.setattr(
        modulo_leiloes,
        "_buscar_dados_leiloes",
        lambda *_, **__: dados_23 + dados_28,
    )
    monkeypatch.setattr(
        modulo_leiloes,
        "_buscar_ptax",
        lambda *_, **__: pl.DataFrame(
            {
                "data_ref": [
                    dt.date(2025, 10, 24),
                    dt.date(2025, 10, 27),
                    dt.date(2025, 10, 28),
                ],
                "ptax": [5.37, 5.36, 5.35],
            },
            schema={"data_ref": pl.Date, "ptax": pl.Float64},
        ),
    )

    resultado = modulo_leiloes.leiloes(inicio="24-10-2025")

    assert resultado["data_1v"].unique().to_list() == [dt.date(2025, 10, 28)]


def test_leiloes_rejeita_modos_temporais_ambiguos():
    with pytest.raises(ValueError, match="data não pode ser combinado"):
        modulo_leiloes.leiloes(data="23-10-2025", inicio="01-10-2025")

    with pytest.raises(ValueError, match="fim só pode ser usado"):
        modulo_leiloes.leiloes(fim="23-10-2025")

    with pytest.raises(ValueError, match="inicio deve ser menor"):
        modulo_leiloes.leiloes(inicio="24-10-2025", fim="23-10-2025")
