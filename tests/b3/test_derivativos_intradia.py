import datetime as dt
import importlib
import json
from pathlib import Path

modulo_derivativos = importlib.import_module("pyield.b3.derivativos_intradia")
modulo_futuro_intradia = importlib.import_module("pyield.futuro.intradia")

DIRETORIO_DADOS = Path(__file__).parent / "data"
DATA_REFERENCIA = dt.date(2026, 3, 10)
HORARIO_REFERENCIA = dt.datetime(2026, 3, 10, 12, 0)
TAMANHO_CODIGO_NEGOCIACAO_FUTURO = 6


def _carregar_json_scty(contrato: str) -> list[dict]:
    """Carrega a lista Scty do JSON bruto de referência."""
    caminho = DIRETORIO_DADOS / f"derivativos_intradia_20260310_{contrato}.json"
    with open(caminho, encoding="utf-8") as f:
        return json.load(f)["Scty"]


def _buscar_json_intradia_mock(contrato: str) -> list[dict]:
    return _carregar_json_scty(contrato)


def _data_referencia_mock() -> dt.date:
    return DATA_REFERENCIA


def _horario_referencia_mock() -> dt.datetime:
    return HORARIO_REFERENCIA


def test_derivativo_intradia_preserva_payload_misto(monkeypatch):
    """Módulo bruto deve preservar mercados mistos do payload do dia."""
    monkeypatch.setattr(
        modulo_derivativos,
        "_buscar_json_intradia",
        _buscar_json_intradia_mock,
    )

    resultado = modulo_derivativos.derivativo_intradia("DOL")
    total_esperado = len(_carregar_json_scty("DOL"))

    assert resultado.height == total_esperado
    assert resultado["codigo_mercado"].unique().sort().to_list() == [
        "FUT",
        "OPTEXER",
        "SOPT",
        "SPOT",
    ]


def test_derivativo_intradia_suporta_colunas_opcionais_ausentes(monkeypatch):
    """Módulo bruto não deve quebrar quando o payload não tem book de ofertas."""
    monkeypatch.setattr(
        modulo_derivativos,
        "_buscar_json_intradia",
        _buscar_json_intradia_mock,
    )

    resultado = modulo_derivativos.derivativo_intradia("DDI")
    total_esperado = len(_carregar_json_scty("DDI"))

    assert resultado.height == total_esperado
    assert "preco_oferta_compra" not in resultado.columns
    assert "preco_oferta_venda" not in resultado.columns


def test_derivativo_intradia_nao_descarta_fro_sem_curprc(monkeypatch):
    """FRO deve continuar válido mesmo sem coluna curPrc no payload."""
    monkeypatch.setattr(
        modulo_derivativos,
        "_buscar_json_intradia",
        _buscar_json_intradia_mock,
    )

    resultado = modulo_derivativos.derivativo_intradia("FRO")
    total_esperado = len(_carregar_json_scty("FRO"))

    assert resultado.height == total_esperado
    assert "preco_ultimo" not in resultado.columns
    assert resultado["codigo_mercado"].unique().to_list() == ["FUT"]


def test_futuro_intradia_filtra_apenas_futuros(monkeypatch):
    """Camada de futuro deve consumir o bruto e manter apenas FUT."""
    monkeypatch.setattr(
        modulo_derivativos,
        "_buscar_json_intradia",
        _buscar_json_intradia_mock,
    )
    monkeypatch.setattr(modulo_futuro_intradia, "intradia_disponivel", lambda: True)
    monkeypatch.setattr(modulo_derivativos.relogio, "agora", _horario_referencia_mock)
    monkeypatch.setattr(
        modulo_futuro_intradia,
        "derivativo_intradia",
        modulo_derivativos.derivativo_intradia,
    )
    monkeypatch.setattr(
        modulo_futuro_intradia.du, "ultimo_dia_util", _data_referencia_mock
    )

    resultado = modulo_futuro_intradia.intradia("DOL")

    codigos_fut_esperados = [
        item["symb"]
        for item in _carregar_json_scty("DOL")
        if item.get("mkt", {}).get("cd") == "FUT"
    ]
    codigos_fut_esperados.sort()

    assert resultado.height == len(codigos_fut_esperados)
    assert resultado["codigo_negociacao"].sort().to_list() == codigos_fut_esperados
    assert all(
        len(codigo) == TAMANHO_CODIGO_NEGOCIACAO_FUTURO
        for codigo in resultado["codigo_negociacao"].to_list()
    )
