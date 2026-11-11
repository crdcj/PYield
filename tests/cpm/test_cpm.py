"""Testes de yd.cpm.contratos.

O fluxo público usa capturas brutas locais e o Parquet histórico de referência.
"""

from pathlib import Path

import polars as pl
import pytest
import requests
from polars.testing import assert_frame_equal

import pyield as yd
import pyield.cpm._contratos as modulo_contratos  # noqa: PLC2701

DIRETORIO_DADOS = Path(__file__).parent / "data"
pytestmark = pytest.mark.usefixtures("calendario_copom_local")
TOTAL_CPM_20261030 = 47


# ── Parsing do código de negociação ──────────────────────────────────────


@pytest.mark.parametrize(
    ("ticker", "mes_esperado", "ano_esperado", "tipo_esperado", "bps_esperado"),
    [
        ("CPMF25C099500", 1, 2025, "call", -50),
        ("CPMZ25C100000", 12, 2025, "call", 0),
        ("CPMH26C099250", 3, 2026, "call", -75),
        ("CPMH26C099750", 3, 2026, "call", -25),
        ("CPMH26C100000", 3, 2026, "call", 0),
        ("CPMH26C100250", 3, 2026, "call", 25),
        ("CPMF25P099500", 1, 2025, "put", -50),
    ],
)
def test_parse_ticker_valid(  # noqa: PLR0913, PLR0917
    monkeypatch,
    ticker,
    mes_esperado,
    ano_esperado,
    tipo_esperado,
    bps_esperado,
):
    monkeypatch.setattr(
        modulo_contratos.boletim,
        "buscar",
        lambda *args, **kwargs: pl.DataFrame({"codigo_negociacao": [ticker]}),
    )
    monkeypatch.setattr(
        modulo_contratos,
        "_buscar_csv",
        lambda data: "\ufeffInstrumento financeiro;Preço de referência\n".encode(),
    )
    linha = yd.cpm.contratos("02-01-2025").row(0, named=True)
    assert linha["data_fim_reuniao"].month == mes_esperado
    assert linha["data_fim_reuniao"].year == ano_esperado
    assert linha["tipo_opcao"] == tipo_esperado
    assert linha["variacao_strike_bps"] == bps_esperado


def test_contratos_vazios_preservam_schema(monkeypatch):
    monkeypatch.setattr(
        modulo_contratos.boletim, "buscar", lambda *a, **kw: pl.DataFrame()
    )
    df = yd.cpm.contratos("02-01-2025")

    assert len(df) == 0
    assert df.columns == [
        "data_referencia",
        "codigo_negociacao",
        "data_fim_reuniao",
        "data_expiracao",
        "tipo_opcao",
        "variacao_strike_bps",
        "preco_ajuste",
        "dias_uteis",
    ]
    assert df["data_referencia"].dtype == pl.Date
    assert df["preco_ajuste"].dtype == pl.Float64
    assert df["variacao_strike_bps"].dtype == pl.Int32
    assert df["dias_uteis"].dtype == pl.Int32


def test_contratos_reproduzem_referencia_historica(monkeypatch, cpm_fixture):
    resposta = requests.Response()
    resposta.status_code = 200
    resposta._content = (DIRETORIO_DADOS / "SPRD250129.zip").read_bytes()
    monkeypatch.setattr(
        modulo_contratos.boletim._SESSAO, "get", lambda *a, **kw: resposta
    )
    monkeypatch.setattr(
        modulo_contratos,
        "_buscar_csv",
        lambda _: (DIRETORIO_DADOS / "consolidados_20250129.csv").read_bytes(),
    )

    resultado = yd.cpm.contratos("29-01-2025")

    # A B3 já não publica preços para esta data. O Parquet preserva os preços
    # capturados à época, usados nos testes de probabilidades. O parsing de
    # preços publicados é exercitado em test_rede.py, pelo POST do CPM.
    assert resultado["preco_ajuste"].null_count() == resultado.height
    assert_frame_equal(
        resultado.drop("preco_ajuste"),
        cpm_fixture.drop("preco_ajuste"),
        check_exact=True,
    )


def test_precos_referencia_ignoram_tipo_das_demais_colunas():
    # Em 30/10/2026, "Preço de abertura" só tem inteiros nas primeiras linhas e
    # "27,68" na linha 187; inferir o tipo dessa coluna gerava ComputeError.
    csv_bytes = (DIRETORIO_DADOS / "consolidados_20261030.csv").read_bytes()

    precos = modulo_contratos._parsear_precos_referencia(csv_bytes)

    assert precos.height == TOTAL_CPM_20261030
    assert precos.schema == {"codigo_negociacao": pl.String, "preco_ajuste": pl.Float64}
    # Valores conferidos nas linhas do CSV bruto da B3.
    esperado = {
        "CPMX26C099000": 0.0,
        "CPMX26C099500": 0.3,
        "CPMX26C099750": 26.0,
        "CPMX26C100000": 72.5,
        "CPMZ26C099750": 12.0,
    }
    obtido = precos.filter(pl.col("codigo_negociacao").is_in(list(esperado)))
    assert dict(obtido.iter_rows()) == esperado
