import datetime as dt
import importlib
import math
from urllib.parse import parse_qs, urlparse

import polars as pl

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
