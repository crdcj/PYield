import datetime as dt
import importlib
import math
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest

import pyield as yd

sgs = importlib.import_module("pyield.bc.sgs")


def _parametro_data(url: str, nome: str) -> dt.date:
    valor = parse_qs(urlparse(url).query)[nome][0]
    return dt.datetime.strptime(valor, "%d/%m/%Y").date()


def test_selic_meta_serie_anterior_ao_inicio_nao_chama_api(monkeypatch):
    def falhar_se_chamada(_url: str) -> pl.DataFrame:
        raise AssertionError("A API não deveria ser chamada.")

    monkeypatch.setattr(sgs, "_buscar_api", falhar_se_chamada)

    resultado = yd.selic.meta_serie("01-01-1995", "04-03-1999")

    assert resultado.is_empty()
    assert resultado.schema == {"data": pl.Date, "taxa": pl.Float64}
    assert math.isnan(yd.selic.meta("01-01-1995"))


def test_selic_meta_serie_ajusta_inicio_anterior(monkeypatch):
    urls = []

    def buscar_api(url: str) -> pl.DataFrame:
        urls.append(url)
        return pl.DataFrame(
            {"data": [dt.date(1999, 3, 5)], "valor": [45.0]},
            schema=sgs.ESQUEMA_BRUTO,
        )

    monkeypatch.setattr(sgs, "_buscar_api", buscar_api)

    resultado = yd.selic.meta_serie("01-01-1995", "06-03-1999")

    assert len(urls) == 1
    assert _parametro_data(urls[0], "dataInicial") == dt.date(1999, 3, 5)
    assert resultado.to_dicts() == [{"data": dt.date(1999, 3, 5), "taxa": 0.45}]


def test_selic_meta_serie_divide_intervalo_em_blocos_seguros(monkeypatch):
    urls = []

    def buscar_api(url: str) -> pl.DataFrame:
        urls.append(url)
        data = _parametro_data(url, "dataInicial")
        return pl.DataFrame(
            {"data": [data], "valor": [10.0]},
            schema=sgs.ESQUEMA_BRUTO,
        )

    monkeypatch.setattr(sgs, "_buscar_api", buscar_api)

    inicio = dt.date(1999, 3, 5)
    fim = dt.date(2019, 3, 5)
    resultado = yd.selic.meta_serie(inicio, fim)

    intervalos = sorted(
        (
            _parametro_data(url, "dataInicial"),
            _parametro_data(url, "dataFinal"),
        )
        for url in urls
    )
    numero_blocos_esperado = (
        (fim - inicio).days // (sgs._LIMITE_DIAS_SELIC_META + 1)
    ) + 1
    assert len(intervalos) == numero_blocos_esperado
    assert intervalos[0][0] == inicio
    assert intervalos[-1][1] == fim
    assert all(
        (fim_bloco - inicio_bloco).days <= sgs._LIMITE_DIAS_SELIC_META
        for inicio_bloco, fim_bloco in intervalos
    )
    assert all(
        inicio_seguinte == fim_anterior + dt.timedelta(days=1)
        for (_, fim_anterior), (inicio_seguinte, _) in zip(
            intervalos,
            intervalos[1:],
        )
    )
    assert resultado.height == len(intervalos)


def test_igpm_serie_consulta_competencias_e_converte_percentuais(monkeypatch):
    urls = []

    def chamar_api(url):
        urls.append(url)
        return [
            {"data": "01/02/2025", "valor": "-0.50"},
            {"data": "01/01/2025", "valor": "0.27"},
        ]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    resultado = yd.igpm.taxa_serie("15-01-2025", "20-02-2025")

    assert resultado.schema == {"periodo": pl.Int64, "taxa": pl.Float64}
    assert resultado.to_dicts() == [
        {"periodo": 202501, "taxa": 0.0027},
        {"periodo": 202502, "taxa": -0.005},
    ]
    assert len(urls) == 1
    assert urlparse(urls[0]).path == "/dados/serie/bcdata.sgs.189/dados"
    assert _parametro_data(urls[0], "dataInicial") == dt.date(2025, 1, 1)
    assert _parametro_data(urls[0], "dataFinal") == dt.date(2025, 2, 1)


def test_igpm_taxa_qualquer_dia_do_mes(monkeypatch):
    def chamar_api(url):
        assert _parametro_data(url, "dataInicial") == dt.date(2025, 1, 1)
        assert _parametro_data(url, "dataFinal") == dt.date(2025, 1, 1)
        return [{"data": "01/01/2025", "valor": "0.27"}]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    taxa_esperada = 0.0027
    assert yd.igpm.taxa(dt.date(2025, 1, 31)) == taxa_esperada


def test_igpm_ultimos_tem_prioridade(monkeypatch):
    def chamar_api(url):
        assert url.endswith("bcdata.sgs.189/dados/ultimos/1?formato=json")
        return [{"data": "01/01/2025", "valor": "0.27"}]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    assert yd.igpm.taxa_serie("invalida", "invalida", ultimos=1).to_dicts() == [
        {"periodo": 202501, "taxa": 0.0027}
    ]


def test_igpm_sem_dados(monkeypatch):
    monkeypatch.setattr(sgs, "_chamar_api", lambda _url: [])

    resultado = yd.igpm.taxa_serie("01-01-2025", "31-01-2025")

    assert resultado.is_empty()
    assert resultado.schema == {"periodo": pl.Int64, "taxa": pl.Float64}
    assert math.isnan(yd.igpm.taxa("15-01-2025"))
    assert math.isnan(yd.igpm.taxa(None))


@pytest.mark.parametrize("ultimos", [0, -1])
def test_igpm_rejeita_quantidade_nao_positiva(ultimos):
    with pytest.raises(ValueError, match="maior que 0"):
        yd.igpm.taxa_serie(ultimos=ultimos)


def test_igpm_exige_inicio_ou_ultimos():
    with pytest.raises(ValueError, match="inicio.*ultimos"):
        yd.igpm.taxa_serie()


def test_igpm_namespace_publico():
    assert "igpm" in yd.__all__
    assert yd.igpm.__all__ == ["taxa", "taxa_serie"]
    assert yd.igpm.taxa is sgs.igpm_taxa
    assert yd.igpm.taxa_serie is sgs.igpm_taxa_serie


def test_igpm_fim_padrao_usa_hoje(monkeypatch):
    hoje = dt.date(2025, 2, 20)
    monkeypatch.setattr(sgs.relogio, "hoje", lambda: hoje)

    def chamar_api(url):
        assert _parametro_data(url, "dataFinal") == hoje
        return [{"data": "01/02/2025", "valor": "0.27"}]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    assert yd.igpm.taxa_serie("15-02-2025").to_dicts() == [
        {"periodo": 202502, "taxa": 0.0027}
    ]


def test_igpm_intervalo_invertido_nao_chama_api(monkeypatch):
    def falhar_se_chamada(_url):
        raise AssertionError("A API não deveria ser chamada.")

    monkeypatch.setattr(sgs, "_chamar_api", falhar_se_chamada)

    resultado = yd.igpm.taxa_serie("01-02-2025", "31-01-2025")

    assert resultado.is_empty()
    assert resultado.schema == {"periodo": pl.Int64, "taxa": pl.Float64}
