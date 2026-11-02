import importlib
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest

import pyield as yd

sgs = importlib.import_module("pyield.bc.sgs")
TAXA_DI_OVER = Decimal("0.1315000000")


@pytest.mark.parametrize("data", ["28-02-2025", "28/02/2025", "2025-02-28"])
def test_di_over_consulta_sgs_e_preserva_taxa_anualizada(monkeypatch, data):
    urls = []

    def chamar_api(url):
        urls.append(url)
        return [
            {"data": "27/02/2025", "valor": "13.15"},
            {"data": "28/02/2025", "valor": "13.15"},
        ]

    monkeypatch.setattr(sgs, "_chamar_api", chamar_api)

    assert yd.di_over(data) == TAXA_DI_OVER
    assert yd.di_over is sgs.di_over
    assert len(urls) == 1
    url = urlparse(urls[0])
    assert url.scheme == "https"
    assert url.path == "/dados/serie/bcdata.sgs.4389/dados"
    assert parse_qs(url.query) == {
        "formato": ["json"],
        "dataInicial": ["27/02/2025"],
        "dataFinal": ["28/02/2025"],
    }


def test_di_over_sem_dados(monkeypatch):
    monkeypatch.setattr(sgs, "_chamar_api", lambda _: [])

    assert yd.di_over("28/02/2025").is_nan()
