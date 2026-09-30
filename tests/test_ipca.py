import datetime as dt
import importlib

import polars as pl
import pytest

import pyield as yd

historico = importlib.import_module("pyield.ipca.historico")


@pytest.mark.parametrize(
    "serie",
    [
        (yd.ipca.taxa_serie, 63, "0.16", "taxa", 0.0016),
        (yd.ipca.indice_serie, 2266, "7111.86", "indice", 7111.86),
    ],
)
@pytest.mark.parametrize("modo", ["intervalo", "ultimos", "fim_atual", "prioridade"])
def test_serie(monkeypatch, serie, modo):
    funcao, variavel, valor, coluna, esperado = serie
    urls = []

    def buscar(url):
        urls.append(url)
        return {"202501": valor}

    monkeypatch.setattr(historico, "_buscar_dados_api", buscar)
    monkeypatch.setattr(historico.relogio, "hoje", lambda: dt.date(2025, 3, 1))

    if modo == "intervalo":
        resultado = funcao("01-01-2025", "01-03-2025")
    elif modo == "ultimos":
        resultado = funcao(ultimos=3)
    elif modo == "fim_atual":
        resultado = funcao("01-01-2025")
    else:
        resultado = funcao("01-01-2025", "01-03-2025", ultimos=3)

    periodo = "-3" if modo in {"ultimos", "prioridade"} else "202501-202503"
    assert urls == [
        f"{historico._URL_BASE}{periodo}/variaveis/{variavel}{historico._SUFIXO_URL}"
    ]
    assert resultado.schema == {"periodo": pl.Int64, coluna: pl.Float64}
    assert resultado.to_dicts() == [{"periodo": 202501, coluna: esperado}]


@pytest.mark.parametrize("funcao", [yd.ipca.taxa_serie, yd.ipca.indice_serie])
@pytest.mark.parametrize(
    "parametros", [{}, {"fim": "01-03-2025"}, {"ultimos": 0}, {"ultimos": -1}]
)
def test_serie_rejeita_entrada_invalida(monkeypatch, funcao, parametros):
    def falhar_fetch(_url):
        raise AssertionError("A API não deveria ser chamada.")

    monkeypatch.setattr(historico, "_buscar_dados_api", falhar_fetch)

    mensagem = "maior que 0" if "ultimos" in parametros else "Informe 'inicio'"
    with pytest.raises(ValueError, match=mensagem):
        funcao(**parametros)


def test_api_publica():
    assert set(yd.ipca.__all__) == {
        "taxa",
        "taxa_serie",
        "indice",
        "indice_serie",
        "taxa_projetada",
    }
    assert yd.ipca.taxa_serie is historico.taxa_serie
    assert yd.ipca.indice_serie is historico.indice_serie
    for nome in ("taxas", "taxas_ultimas", "indices", "indices_ultimos"):
        assert not hasattr(yd.ipca, nome)
