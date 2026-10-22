# ruff: noqa: PLR2004

import datetime as dt
import json
import ssl
from pathlib import Path

import polars as pl
import pytest
import requests

import pyield as yd
from pyield._internal import calendario_igpm as fgv  # noqa: PLC2701
from pyield._internal.cache import ttl_cache  # noqa: PLC2701
from pyield.ipca import _calendario as ibge  # noqa: PLC2701

_DADOS = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def caches_isolados(monkeypatch):
    for modulo in (ibge, fgv):
        monkeypatch.setattr(
            modulo,
            "_buscar_calendario",
            ttl_cache()(modulo._buscar_calendario.__wrapped__),
        )


def test_ipca_publico_com_respostas_reais(monkeypatch):
    paginas = [
        (_DADOS / f"ipca_calendario_pagina_{pagina}.json").read_bytes()
        for pagina in (1, 2)
    ]
    monkeypatch.setattr(ibge, "_buscar_calendario", lambda **_: paginas)

    resultado = yd.ipca.calendario(2026)

    assert resultado.height == 12
    assert resultado.schema == {"competencia": pl.Date, "data_divulgacao": pl.Date}
    assert resultado.row(-1) == (dt.date(2026, 12, 1), dt.date(2027, 1, 12))
    assert resultado["competencia"].is_sorted()
    assert yd.ipca.calendario(1900).schema == resultado.schema
    assert yd.ipca.calendario(1900).is_empty()


def test_igpm_publico_com_html_real(monkeypatch):
    monkeypatch.setattr(
        fgv,
        "_buscar_calendario",
        lambda *_, **__: (_DADOS / "igpm_calendario_2026.html").read_bytes(),
    )

    resultado = yd.igpm.calendario(2026)

    assert resultado.height == 12
    assert resultado.schema == {"competencia": pl.Date, "data_divulgacao": pl.Date}
    assert resultado.row(0) == (dt.date(2026, 1, 1), dt.date(2026, 1, 29))
    assert resultado.row(-1) == (dt.date(2026, 12, 1), dt.date(2026, 12, 29))
    assert resultado["competencia"].is_sorted()
    assert yd.igpm.calendario(1900).is_empty()


@pytest.mark.parametrize("calendario", [yd.ipca.calendario, yd.igpm.calendario])
@pytest.mark.parametrize("ano", [0, 10000, True, "2026"])
def test_ano_invalido(calendario, ano):
    with pytest.raises(ValueError, match="ano"):
        calendario(ano)


def test_paginacao_e_atualizacao_ipca(monkeypatch):
    chamadas = []

    class Resposta:
        def __init__(self, pagina):
            self.content = (
                _DADOS / f"ipca_calendario_pagina_{pagina}.json"
            ).read_bytes()

        def json(self):
            return json.loads(self.content)

        def raise_for_status(self):
            pass

    def buscar(url, *, params, timeout):
        chamadas.append(params["page"])
        assert url == ibge._URL
        assert timeout == 30
        return Resposta(params["page"])

    monkeypatch.setattr(ibge.requests, "get", buscar)
    primeira = yd.ipca.calendario(2026, atualizar=True)
    assert yd.ipca.calendario(2026).equals(primeira)
    assert chamadas == [1, 2]
    assert yd.ipca.calendario(2026, atualizar=True).equals(primeira)
    assert chamadas == [1, 2, 1, 2]


def test_paginacao_invalida(monkeypatch):
    class Resposta:
        content = b'{"items": [], "nextPage": 1}'

        def json(self):
            return json.loads(self.content)

        def raise_for_status(self):
            pass

    monkeypatch.setattr(ibge.requests, "get", lambda *_, **__: Resposta())
    with pytest.raises(ValueError, match="Paginação"):
        yd.ipca.calendario(2026, atualizar=True)


def test_ipca_conflitante_e_filtros(monkeypatch):
    item = {
        "produto_id": 9256,
        "tipo_id": 1,
        "ano_referencia_inicio": 2026,
        "mes_referencia_inicio": 9,
        "data_divulgacao": "09/10/2026 12:00:00",
    }
    itens = [item, dict(item, produto_id=3060), dict(item, tipo_id=2)]
    monkeypatch.setattr(
        ibge, "_buscar_calendario", lambda **_: [json.dumps({"items": itens}).encode()]
    )
    assert yd.ipca.calendario(2026).height == 1
    itens.append(dict(item, data_divulgacao="10/10/2026 12:00:00"))
    with pytest.raises(ValueError, match="conflitantes"):
        yd.ipca.calendario(2026)


def test_igpm_exclui_previas_e_detecta_conflitos(monkeypatch):
    html = b"""<ul>
    <li><b>29/09/2026</b><span>IGP-M e os componentes: IPA-M e IPC-M</span></li>
    <li>18/09/2026 IGP-M Segundo Decendio</li><li>07/10/2026 IGP-DI</li>
    </ul>"""
    monkeypatch.setattr(fgv, "_buscar_calendario", lambda *_, **__: html)
    assert yd.igpm.calendario(2026).rows() == [
        (dt.date(2026, 9, 1), dt.date(2026, 9, 29))
    ]
    html += b"<li>30/09/2026 IGP-M e os componentes: IPA-M e IPC-M</li>"
    with pytest.raises(ValueError, match="conflitantes"):
        yd.igpm.calendario(2026)


def test_igpm_fetch_filtros_cache_e_atualizacao(monkeypatch):
    chamadas = []

    class Resposta:
        content = (_DADOS / "igpm_calendario_2026.html").read_bytes()

        def raise_for_status(self):
            pass

    def buscar(self, url, *, params, timeout):
        chamadas.append(params)
        assert url == fgv._URL
        assert timeout == 30
        return Resposta()

    monkeypatch.setattr(fgv.requests.Session, "get", buscar)
    primeira = yd.igpm.calendario(2026)
    assert yd.igpm.calendario(2026).equals(primeira)
    assert chamadas == [
        {
            "field_indice_target_id": "128",
            "field_divulgacao_data_value": "2026-01-01",
            "field_divulgacao_data_value_1": "2026-12-31",
        }
    ]
    assert yd.igpm.calendario(2026, atualizar=True).equals(primeira)
    assert len(chamadas) == 2


def test_ipca_data_ausente_nao_vira_agenda_valida(monkeypatch):
    item = {
        "produto_id": 9256,
        "tipo_id": 1,
        "ano_referencia_inicio": 2026,
        "mes_referencia_inicio": 9,
        "data_divulgacao": None,
    }
    monkeypatch.setattr(
        ibge, "_buscar_calendario", lambda **_: [json.dumps({"items": [item]}).encode()]
    )
    with pytest.raises(ValueError, match="ausente"):
        yd.ipca.calendario(2026)


@pytest.mark.parametrize(
    ("modulo", "calendario"), [(ibge, yd.ipca.calendario), (fgv, yd.igpm.calendario)]
)
def test_falha_operacional_nao_vira_calendario_vazio(monkeypatch, modulo, calendario):
    def falhar(*_, **__):
        raise requests.Timeout("fonte indisponível")

    monkeypatch.setattr(modulo, "_buscar_calendario", falhar)
    with pytest.raises(requests.Timeout):
        calendario(2026)


def test_exports_canonicos():
    assert yd.ipca.calendario is ibge.calendario
    assert yd.igpm.calendario is fgv.calendario
    assert "calendario" in yd.ipca.__all__
    assert "calendario" in yd.igpm.__all__


def test_tls_fgv_preserva_verificacao_de_certificado():
    adaptador = fgv._AdaptadorFGV()
    contexto = adaptador.poolmanager.connection_pool_kw["ssl_context"]
    assert contexto.verify_mode == ssl.CERT_REQUIRED
    assert contexto.check_hostname
    assert "AES128-GCM-SHA256" in [cifra["name"] for cifra in contexto.get_ciphers()]
    adaptador.close()
