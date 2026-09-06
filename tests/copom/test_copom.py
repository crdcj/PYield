"""Calendário público com respostas brutas do BCB e cenários de borda."""

import datetime as dt
from pathlib import Path

import polars as pl
import pytest
import requests
from polars.testing import assert_frame_equal

import pyield as yd
import pyield.copom._calendario as impl  # noqa: PLC2701

NUMERO_REUNIAO = 280
ANO_AGENDA = 2027
REUNIOES_ANUAIS = 8

DATA = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def fontes(monkeypatch):
    monkeypatch.setattr(impl, "_buscar_atas", (DATA / "atas.json").read_bytes)
    monkeypatch.setattr(
        impl, "_buscar_calendario", (DATA / "calendario.ics").read_bytes
    )
    monkeypatch.setattr(impl.relogio, "hoje", lambda: dt.date(2026, 9, 6))


def test_calendario():
    cal = yd.copom.calendario()
    assert cal.schema == impl._SCHEMA
    assert cal["data_decisao"].is_sorted()
    assert cal["data_decisao"].n_unique() == cal.height
    assert (
        cal.filter(pl.col("data_decisao").dt.year() == ANO_AGENDA).height
        == REUNIOES_ANUAIS
    )
    atual = cal.filter(pl.col("nro_reuniao") == NUMERO_REUNIAO).row(0, named=True)
    assert atual["data_inicio"] == dt.date(2026, 8, 4)
    assert atual["data_decisao"] == dt.date(2026, 8, 5)
    assert atual["data_publicacao"] == dt.date(2026, 8, 11)
    assert atual["fonte"] == "Ata"
    assert atual["status"] == "Realizada"
    assert_frame_equal(
        cal.select("data_efetividade"),
        cal.select(data_efetividade=yd.du.deslocar_expr("data_decisao", 1)),
    )
    assert cal.filter(pl.col("data_inicio").is_null()).height > 0


@pytest.mark.parametrize("data", ["05-08-2026", "05/08/2026", "2026-08-05"])
def test_filtro_inclusivo(data):
    assert yd.copom.calendario(data, data)["nro_reuniao"].item() == NUMERO_REUNIAO
    assert yd.copom.proxima_reuniao(data)["nro_reuniao"].item() == NUMERO_REUNIAO


def test_proxima_e_vazio():
    assert yd.copom.proxima_reuniao()["data_decisao"].item() == dt.date(2026, 9, 16)
    vazio = yd.copom.proxima_reuniao("2099-01-01")
    assert vazio.is_empty()
    assert vazio.schema == impl._SCHEMA


def test_intervalo_invalido():
    with pytest.raises(ValueError, match="inicio"):
        yd.copom.calendario("2026-09-01", "2026-08-01")
    with pytest.raises(ValueError, match="."):
        yd.copom.calendario("data inválida")


def test_fontes_vazias(monkeypatch):
    monkeypatch.setattr(impl, "_buscar_atas", lambda: b'{"conteudo": []}')
    monkeypatch.setattr(
        impl, "_buscar_calendario", lambda: b"BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n"
    )
    cal = yd.copom.calendario()
    assert cal.is_empty()
    assert cal.schema == impl._SCHEMA


def test_sem_ata_preserva_reuniao_passada(monkeypatch):
    monkeypatch.setattr(impl, "_buscar_atas", lambda: b'{"conteudo": []}')
    cal = yd.copom.calendario("2026-08-05", "2026-08-05")
    assert cal["fonte"].item() == "Calendário"
    assert cal["status"].item() == "Realizada"
    assert cal["nro_reuniao"].item() is None


def _ics(datas):
    eventos = "".join(
        f"BEGIN:VEVENT\r\nDTSTART;VALUE=DATE:{data}\r\nEND:VEVENT\r\n" for data in datas
    )
    return f"BEGIN:VCALENDAR\r\n{eventos}END:VCALENDAR\r\n".encode()


def test_ics_duplicado_fora_de_ordem_e_quebra_de_linha(monkeypatch):
    conteudo = _ics(["20260916", "20260915", "20260916"])
    conteudo = conteudo.replace(b"20260915", b"2026\r\n 0915")
    monkeypatch.setattr(impl, "_buscar_calendario", lambda: conteudo)
    cal = yd.copom.calendario("2026-09-01")
    assert cal.height == 1
    assert cal["data_inicio"].item() == dt.date(2026, 9, 15)


def test_ics_incompleto_nao_combina_reunioes_distintas(monkeypatch):
    monkeypatch.setattr(
        impl, "_buscar_calendario", lambda: _ics(["20260916", "20261103"])
    )
    with pytest.raises(ValueError, match="dois dias consecutivos"):
        yd.copom.calendario()


@pytest.mark.parametrize(
    "conteudo",
    [b"<html>erro</html>", b"BEGIN:VCALENDAR\nBEGIN:VEVENT\nEND:VEVENT\nEND:VCALENDAR"],
)
def test_ics_invalido(monkeypatch, conteudo):
    monkeypatch.setattr(impl, "_buscar_calendario", lambda: conteudo)
    with pytest.raises(ValueError, match="ICS"):
        yd.copom.calendario()


def test_erro_operacional_propagado(monkeypatch):
    def falhar():
        raise requests.ConnectionError("indisponível")

    monkeypatch.setattr(impl, "_buscar_atas", falhar)
    with pytest.raises(requests.ConnectionError):
        yd.copom.calendario()


def test_fronteira_publica():
    assert yd.copom.calendario is impl.calendario
    assert yd.copom.proxima_reuniao is impl.proxima_reuniao
    assert not hasattr(yd.selic, "copom")


def test_cpm_consume_calendario_publico(monkeypatch):
    from pyield import cpm  # noqa: PLC0415

    monkeypatch.setattr(
        cpm.boletim,
        "buscar",
        lambda *args, **kwargs: pl.DataFrame({"codigo_negociacao": ["CPMU26C100000"]}),
    )
    monkeypatch.setattr(cpm, "_fetch_settlement_prices", lambda data: pl.DataFrame())
    resultado = yd.cpm.data("2026-09-01")
    assert resultado["data_fim_reuniao"].item() == dt.date(2026, 9, 16)
    assert resultado["data_expiracao"].item() == dt.date(2026, 9, 17)


def test_historico_contra_parquet_existente():
    esperado = (
        pl.read_parquet(DATA / "copom_calendar.parquet")
        .filter(pl.col("MeetingNumber").is_not_null())
        .select(
            nro_reuniao=pl.col("MeetingNumber").cast(pl.Int64),
            data_decisao=pl.col("EndDate"),
            data_efetividade=pl.col("ExpiryDate"),
        )
    )
    resultado = yd.copom.calendario(fim=esperado["data_decisao"].max())
    assert_frame_equal(resultado.select(esperado.columns), esperado)
