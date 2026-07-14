import datetime as dt
import math

import polars as pl
import pytest

from pyield import ntnb, ntnc
from pyield.tpf.vna import _download  # noqa: PLC2701
from pyield.tpf.vna import ntnb as vna_ntnb
from pyield.tpf.vna import ntnc as vna_ntnc

VNA_NTNB_DEZ_2025 = 4570.078408
VNA_NTNB_JAN_2026 = 4585.159356
VNA_NTNB_30_DEZ_2025 = 4577.369436
VNA_NTNB_JUN_2026 = 4731.856412
VNA_NTNC_2006_JUL_2000 = 1049.125124
VNA_NTNC_2006_AGO_2000 = 1065.620389
VNA_NTNC_2031_DEZ_2025 = 6450.107485
VNA_NTNC_2031_JAN_2026 = 6449.144194
VNA_NTNC_2031_16_DEZ_2025 = 6449.641358
VNA_NTNC_2031_JUN_2026 = 6693.537239


def test_api_publica_reexporta_implementacao_canonica() -> None:
    assert ntnb.vnas is vna_ntnb.vnas
    assert ntnb.vna is vna_ntnb.vna
    assert ntnb.vna_projetado is vna_ntnb.vna_projetado
    assert ntnc.vnas is vna_ntnc.vnas
    assert ntnc.vna is vna_ntnc.vna
    assert ntnc.vna_projetado is vna_ntnc.vna_projetado


def test_extrair_url_planilha() -> None:
    pagina = b"""
        <html><a href="https://example.com/?url=thot-arquivos.tesouro.gov.br/publicacao/1">
        Link incorreto</a>
        <html><a href="https://thot-arquivos.tesouro.gov.br/publicacao/53360">
        Download</a></html>
    """

    assert _download._extrair_url_planilha(pagina) == (
        "https://thot-arquivos.tesouro.gov.br/publicacao/53360"
    )


def test_processar_ntnb() -> None:
    df_bruto = pl.DataFrame(
        {
            "column_1": ["DATA", "2000-07-15 00:00:00", "2000-08-15 00:00:00"],
            "column_2": ["VNA", "1000", "1016.10286"],
        }
    )

    resultado = vna_ntnb._processar(df_bruto)

    esperado = pl.DataFrame(
        {
            "data": [dt.date(2000, 7, 15), dt.date(2000, 8, 15)],
            "vna": [1000.0, 1016.10286],
        },
        schema_overrides={"data": pl.Date},
    )
    assert resultado.equals(esperado)


def test_processar_ntnb_preserva_ultima_ocorrencia_da_data() -> None:
    df_bruto = pl.DataFrame(
        {
            "column_1": [
                "2000-07-15 00:00:00",
                "2000-07-15 00:00:00",
            ],
            "column_2": ["999", "1000"],
        }
    )

    resultado = vna_ntnb._processar(df_bruto)

    assert resultado.to_dicts() == [
        {"data": dt.date(2000, 7, 15), "vna": 1000.0}
    ]


def test_processar_ntnc_preserva_series_por_vencimento() -> None:
    df_bruto = pl.DataFrame(
        {
            "column_1": ["DATA", "2000-07-01 00:00:00"],
            "column_2": ["VENCIMENTOS", "1000"],
            "column_3": ["VENCIMENTOS", "1049.125124"],
        }
    )

    resultado = vna_ntnc._processar(df_bruto)

    assert resultado.to_dicts() == [
        {
            "data": dt.date(2000, 7, 1),
            "anos_vencimento": [2002, 2006],
            "vna": VNA_NTNC_2006_JUL_2000,
        },
        {
            "data": dt.date(2000, 7, 1),
            "anos_vencimento": [2005, 2008, 2011, 2017, 2021, 2031],
            "vna": 1000.0,
        },
    ]


def test_processar_ntnc_preserva_ultima_ocorrencia_da_serie_e_data() -> None:
    df_bruto = pl.DataFrame(
        {
            "column_1": [
                "2000-07-01 00:00:00",
                "2000-07-01 00:00:00",
            ],
            "column_2": ["999", "1000"],
            "column_3": ["1048", "1049.125124"],
        }
    )

    resultado = vna_ntnc._processar(df_bruto)

    assert resultado.to_dicts() == [
        {
            "data": dt.date(2000, 7, 1),
            "anos_vencimento": [2002, 2006],
            "vna": VNA_NTNC_2006_JUL_2000,
        },
        {
            "data": dt.date(2000, 7, 1),
            "anos_vencimento": [2005, 2008, 2011, 2017, 2021, 2031],
            "vna": 1000.0,
        },
    ]


def test_vna_ntnb_calcula_entre_valores_publicados(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        vna_ntnb,
        "vnas",
        lambda: pl.DataFrame(
            {
                "data": [dt.date(2025, 12, 15), dt.date(2026, 1, 15)],
                "vna": [VNA_NTNB_DEZ_2025, VNA_NTNB_JAN_2026],
            },
            schema_overrides={"data": pl.Date},
        ),
    )

    assert vna_ntnb.vna("15-12-2025") == VNA_NTNB_DEZ_2025
    assert vna_ntnb.vna("30-12-2025") == VNA_NTNB_30_DEZ_2025
    assert math.isnan(vna_ntnb.vna("14-12-2025"))
    assert math.isnan(vna_ntnb.vna("16-01-2026"))


def test_vna_ntnc_seleciona_serie_e_calcula_entre_valores_publicados(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        vna_ntnc,
        "vnas",
        lambda: pl.DataFrame(
            {
                "data": [
                    dt.date(2000, 7, 1),
                    dt.date(2000, 8, 1),
                    dt.date(2025, 12, 1),
                    dt.date(2026, 1, 1),
                ],
                "anos_vencimento": [
                    [2002, 2006],
                    [2002, 2006],
                    [2005, 2008, 2031],
                    [2005, 2008, 2031],
                ],
                "vna": [
                    VNA_NTNC_2006_JUL_2000,
                    VNA_NTNC_2006_AGO_2000,
                    VNA_NTNC_2031_DEZ_2025,
                    VNA_NTNC_2031_JAN_2026,
                ],
            },
            schema_overrides={"data": pl.Date},
        ),
    )

    assert vna_ntnc.vna("01-07-2000", "01-01-2006") == VNA_NTNC_2006_JUL_2000
    assert (
        vna_ntnc.vna("16-12-2025", "01-01-2031")
        == VNA_NTNC_2031_16_DEZ_2025
    )
    assert math.isnan(vna_ntnc.vna("01-07-2000", "01-01-2041"))


CASOS_VNA_PROJETADO = [
    (ntnb.vna_projetado, "15-06-2026", VNA_NTNB_JUN_2026, 0.45),
    (ntnc.vna_projetado, "01-06-2026", VNA_NTNC_2031_JUN_2026, 0.30),
]


@pytest.mark.parametrize(("funcao", "data", "vna_base", "inflacao"), CASOS_VNA_PROJETADO)
def test_vna_projetado_entradas_vazias(
    funcao, data, vna_base, inflacao
) -> None:
    assert math.isnan(funcao(None, vna_base, inflacao))


@pytest.mark.parametrize(("funcao", "data", "vna_base", "inflacao"), CASOS_VNA_PROJETADO)
def test_vna_projetado_valida_dominio(
    funcao, data, vna_base, inflacao
) -> None:
    with pytest.raises(ValueError, match="VNA-base"):
        funcao(data, 0, inflacao)
    with pytest.raises(ValueError, match="inflação"):
        funcao(data, vna_base, -100)
