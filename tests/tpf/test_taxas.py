import datetime as dt
import importlib
import zipfile as zf
from pathlib import Path

import polars as pl
import pytest
import requests

import pyield as yd

modulo_taxas = importlib.import_module("pyield.anbima.taxas")
modulo_tpf_taxas = importlib.import_module("pyield.tpf._taxas")

DIRETORIO_DADOS = Path(__file__).parent / "data"
CAMINHO_CSV = DIRETORIO_DADOS / "tpf_20260206.txt"
CAMINHO_PARQUET = DIRETORIO_DADOS / "tpf_20260206.parquet"

COLUNAS_PUBLICAS = [
    "titulo",
    "data_referencia",
    "codigo_selic",
    "data_base",
    "data_vencimento",
    "pu",
    "taxa_compra",
    "taxa_venda",
    "taxa_indicativa",
]


def test_ler():
    resultado = modulo_taxas.ler(CAMINHO_CSV)

    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))


def test_ler_bytes_e_zip(tmp_path):
    caminho = tmp_path / "ms140512.exe"
    with zf.ZipFile(caminho, "w") as arquivo_zip:
        arquivo_zip.writestr("ms140512.txt", CAMINHO_CSV.read_bytes())

    df_arquivo = modulo_taxas.ler(caminho)
    df_bytes = modulo_taxas.ler(caminho.read_bytes())
    esperado = pl.read_parquet(CAMINHO_PARQUET)

    assert df_arquivo.equals(esperado)
    assert df_bytes.equals(esperado)


def test_baixar_arquivo(monkeypatch):
    data_recebida = None
    conteudo = b"conteudo bruto"

    def obter_csv(data):
        nonlocal data_recebida
        data_recebida = data
        return conteudo

    monkeypatch.setattr(modulo_taxas, "_obter_csv", obter_csv)

    resultado = modulo_taxas.baixar_arquivo("06-02-2026")

    assert resultado == conteudo
    assert data_recebida == dt.date(2026, 2, 6)


def test_buscar_retorna_dados_completos_da_anbima(monkeypatch):
    monkeypatch.setattr(modulo_taxas, "_obter_csv", lambda _: CAMINHO_CSV.read_bytes())
    monkeypatch.setattr(
        modulo_taxas,
        "_montar_url_arquivo",
        lambda _: f"{modulo_taxas.ANBIMA_URL}/arquivo.txt",
    )

    resultado = modulo_taxas.buscar("06-02-2026")

    assert resultado.equals(pl.read_parquet(CAMINHO_PARQUET))


def test_taxas_aplica_regras_publicas_de_tpf(monkeypatch):
    df_completo = pl.read_parquet(CAMINHO_PARQUET)
    monkeypatch.setattr(
        modulo_tpf_taxas,
        "_obter_historico",
        lambda: df_completo,
    )

    resultado = yd.tpf.taxas("06-02-2026", titulo="PRE")

    assert yd.tpf.taxas is modulo_tpf_taxas.taxas
    assert resultado.columns == COLUNAS_PUBLICAS
    assert set(resultado["titulo"]) == {"LTN", "NTN-F"}


def test_taxas_busca_anbima_quando_cache_falha(monkeypatch):
    df_completo = pl.read_parquet(CAMINHO_PARQUET)

    def falhar_cache():
        raise requests.ConnectionError("cache indisponível")

    monkeypatch.setattr(modulo_tpf_taxas, "_obter_historico", falhar_cache)
    monkeypatch.setattr(modulo_taxas, "buscar", lambda _: df_completo)

    resultado = yd.tpf.taxas("06-02-2026", titulo="LFT")

    assert resultado.columns == COLUNAS_PUBLICAS
    assert resultado["titulo"].unique().to_list() == ["LFT"]


def test_taxas_historicas_aplica_filtros_publicos(monkeypatch):
    df_completo = pl.read_parquet(CAMINHO_PARQUET)
    monkeypatch.setattr(modulo_tpf_taxas, "_obter_historico", lambda: df_completo)

    resultado = yd.tpf.taxas_historicas(
        inicio="06-02-2026",
        fim="06-02-2026",
        titulo="PRE",
    )

    assert yd.tpf.taxas_historicas is modulo_tpf_taxas.taxas_historicas
    assert resultado.columns == COLUNAS_PUBLICAS
    assert set(resultado["titulo"]) == {"LTN", "NTN-F"}
    assert resultado["data_referencia"].unique().item() == dt.date(2026, 2, 6)


def test_taxas_historicas_rejeita_intervalo_invertido():
    with pytest.raises(ValueError, match="inicio deve ser menor ou igual a fim"):
        yd.tpf.taxas_historicas(inicio="07-02-2026", fim="06-02-2026")


@pytest.mark.parametrize(
    "funcao",
    [
        yd.tpf.taxas,
        yd.tpf.premios_pre,
        yd.tpf.curva_pre,
        yd.ltn.dados,
        yd.ltn.taxas_forward,
        yd.lft.dados,
        yd.ntnb.dados,
        yd.ntnc.dados,
        yd.ntnf.dados,
    ],
)
def test_feriado_retorna_dataframe_vazio(funcao):
    """30/05/2024 foi feriado (Corpus Christi): sem dados, sem erro."""
    assert funcao("30-05-2024").is_empty()


def test_taxas_vazio_mantem_colunas():
    assert yd.tpf.taxas("30-05-2024").columns == COLUNAS_PUBLICAS


@pytest.mark.parametrize(
    ("modulo", "colunas_calculadas", "colunas_finais"),
    [
        (yd.lft, ["prazo_medio"], ["taxa_di", "rentabilidade"]),
        (
            yd.ltn,
            ["duration", "prazo_medio", "dv01"],
            ["taxa_di", "premio", "rentabilidade"],
        ),
        (
            yd.ntnb,
            ["duration", "prazo_medio", "dv01"],
            ["taxa_di", "taxa_zero", "taxa_forward", "inflacao_implicita"],
        ),
        (yd.ntnc, ["duration", "prazo_medio", "dv01"], ["taxa_di"]),
        (
            yd.ntnf,
            ["prazo_medio", "duration", "dv01"],
            ["taxa_di", "taxa_zero", "premio", "premio_limpo", "rentabilidade"],
        ),
    ],
)
def test_dados_vazio_mantem_schema(
    monkeypatch, modulo, colunas_calculadas, colunas_finais
):
    def falhar_fetch():
        raise AssertionError("Uma consulta em feriado não deve buscar dados.")

    monkeypatch.setattr(modulo_tpf_taxas, "_obter_historico", falhar_fetch)

    resultado = modulo.dados("30-05-2024")
    esperado = {
        "data_referencia": pl.Date,
        "titulo": pl.String,
        "codigo_selic": pl.Int64,
        "data_base": pl.Date,
        "data_vencimento": pl.Date,
        "dias_uteis": pl.Int64,
        **dict.fromkeys(colunas_calculadas, pl.Float64),
        "pu": pl.Float64,
        "taxa_compra": pl.Float64,
        "taxa_venda": pl.Float64,
        "taxa_indicativa": pl.Float64,
        **dict.fromkeys(colunas_finais, pl.Float64),
    }

    assert resultado.is_empty()
    assert list(resultado.schema.items()) == list(esperado.items())
