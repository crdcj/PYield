import datetime as dt
import math
from decimal import Decimal

import polars as pl
import pytest

from pyield import ntnb, ntnc
from pyield.tpf.vna import _download, calcular_vna  # noqa: PLC2701
from pyield.tpf.vna import ntnb as vna_ntnb
from pyield.tpf.vna import ntnc as vna_ntnc

VNA_NTNB_DEZ_2025 = 4570.078408
VNA_NTNB_JAN_2026 = 4585.159356
VNA_NTNB_30_DEZ_2025 = 4577.369436
VNA_NTNB_JUN_2026 = 4731.856412
VNA_NTNB_JUL_2026 = 4739.424756
VNA_NTNB_AGO_2026 = 4742.744422
VNA_NTNB_12_AGO_2026 = 4742.423062
VNA_NTNB_13_AGO_2026 = 4742.530180
VNA_NTNB_14_AGO_2026 = 4742.637300
VNA_NTNC_2006_JUL_2000 = 1049.125124
VNA_NTNC_2006_AGO_2000 = 1065.620389
VNA_NTNC_2031_DEZ_2025 = 6450.107485
VNA_NTNC_2031_JAN_2026 = 6449.144194
VNA_NTNC_2031_16_DEZ_2025 = 6449.641358
VNA_NTNC_2031_JUN_2026 = 6693.537239
VNA_TESTE_BASE = 100.0
VNA_TESTE_FINAL = 121.0
VNA_TESTE_INTERMEDIARIO = 110.0
VNA_TESTE_FATOR_ALTERNATIVO = 1.44
VNA_TESTE_INTERMEDIARIO_ALTERNATIVO = 120.0


def test_calcular_vna_publico_retorna_ponto_exato() -> None:
    assert calcular_vna.__module__ == "pyield.tpf.vna.calculo"

    df = pl.DataFrame(
        {
            "data": [dt.date(2026, 1, 1), dt.date(2026, 1, 11)],
            "vna": [VNA_TESTE_BASE, VNA_TESTE_FINAL],
        },
        schema_overrides={"data": pl.Date},
    )

    resultado = calcular_vna(df, dt.date(2026, 1, 1))

    assert isinstance(resultado, float)
    assert resultado == VNA_TESTE_BASE


def test_calcular_vna_publico_calcula_prorata_e_fator_alternativo() -> None:
    df = pl.DataFrame(
        {
            "data": [dt.date(2026, 1, 1), dt.date(2026, 1, 11)],
            "vna": [VNA_TESTE_BASE, VNA_TESTE_FINAL],
        },
        schema_overrides={"data": pl.Date},
    )

    assert calcular_vna(df, dt.date(2026, 1, 6)) == VNA_TESTE_INTERMEDIARIO
    assert (
        calcular_vna(
            df,
            dt.date(2026, 1, 6),
            fator_variacao=VNA_TESTE_FATOR_ALTERNATIVO,
        )
        == VNA_TESTE_INTERMEDIARIO_ALTERNATIVO
    )


@pytest.mark.parametrize("data", [dt.date(2025, 12, 31), dt.date(2026, 1, 12)])
def test_calcular_vna_publico_fora_do_intervalo_retorna_float_nan(
    data: dt.date,
) -> None:
    df = pl.DataFrame(
        {
            "data": [dt.date(2026, 1, 1), dt.date(2026, 1, 11)],
            "vna": [VNA_TESTE_BASE, VNA_TESTE_FINAL],
        },
        schema_overrides={"data": pl.Date},
    )

    resultado = calcular_vna(df, data)

    assert isinstance(resultado, float)
    assert math.isnan(resultado)


def test_api_publica_reexporta_implementacao_canonica() -> None:
    assert not hasattr(ntnb, "calcular_vna")
    assert not hasattr(ntnc, "calcular_vna")
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


def test_ler_planilha_converte_explicitamente_para_dataframe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Planilha:
        @staticmethod
        def load_sheet(aba: str, *, header_row: int | None) -> pl.DataFrame:
            assert aba == "NTNB"
            assert header_row is None
            return pl.DataFrame({"a": ["DATA"], "b": ["VNA"]})

    monkeypatch.setattr(
        "pyield._internal.excel.fastexcel.read_excel", lambda _: Planilha()
    )

    resultado = _download.ler_planilha(b"conteudo", "NTNB")

    assert resultado.to_dict(as_series=False) == {
        "column_1": ["DATA"],
        "column_2": ["VNA"],
    }


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

    assert resultado.to_dicts() == [{"data": dt.date(2000, 7, 15), "vna": 1000.0}]


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
    monkeypatch.setattr(
        vna_ntnb._ipca,
        "indices",
        lambda inicio, fim: pl.DataFrame(
            {
                "periodo": [202511, 202512],
                "indice": [7378.94, 7403.29],
            }
        ),
    )

    assert vna_ntnb.vna("15-12-2025") == Decimal(str(VNA_NTNB_DEZ_2025))
    assert vna_ntnb.vna("30-12-2025") == Decimal(str(VNA_NTNB_30_DEZ_2025))
    assert vna_ntnb.vna("14-12-2025").is_nan()
    assert vna_ntnb.vna("16-01-2026").is_nan()


@pytest.mark.parametrize(
    ("data", "esperado"),
    [
        ("12-08-2026", VNA_NTNB_12_AGO_2026),
        ("13-08-2026", VNA_NTNB_13_AGO_2026),
        ("14-08-2026", VNA_NTNB_14_AGO_2026),
    ],
)
def test_vna_ntnb_usa_numeros_indice_com_precisao_normativa(
    monkeypatch: pytest.MonkeyPatch, data: str, esperado: float
) -> None:
    monkeypatch.setattr(
        vna_ntnb,
        "vnas",
        lambda: pl.DataFrame(
            {
                "data": [dt.date(2026, 7, 15), dt.date(2026, 8, 15)],
                "vna": [VNA_NTNB_JUL_2026, VNA_NTNB_AGO_2026],
            },
            schema_overrides={"data": pl.Date},
        ),
    )

    def indices(inicio, fim) -> pl.DataFrame:
        assert inicio == dt.date(2026, 6, 30)
        assert fim == dt.date(2026, 7, 31)
        return pl.DataFrame(
            {
                "periodo": [202606, 202607],
                "indice": [7652.37, 7657.73],
            }
        )

    monkeypatch.setattr(vna_ntnb._ipca, "indices", indices)

    assert vna_ntnb.vna(data) == Decimal(f"{esperado:.6f}")


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

    assert vna_ntnc.vna("01-07-2000", "01-01-2006") == Decimal(
        str(VNA_NTNC_2006_JUL_2000)
    )
    assert vna_ntnc.vna("16-12-2025", "01-01-2031") == Decimal(
        str(VNA_NTNC_2031_16_DEZ_2025)
    )
    assert vna_ntnc.vna("01-07-2000", "01-01-2041").is_nan()


CASOS_VNA_PROJETADO = [
    (ntnb.vna_projetado, "15-06-2026", VNA_NTNB_JUN_2026, 0.45),
    (ntnc.vna_projetado, "01-06-2026", VNA_NTNC_2031_JUN_2026, 0.30),
]


@pytest.mark.parametrize(
    ("funcao", "data", "vna_base", "inflacao"), CASOS_VNA_PROJETADO
)
def test_vna_projetado_entradas_vazias(funcao, data, vna_base, inflacao) -> None:
    assert math.isnan(funcao(None, vna_base, inflacao))


@pytest.mark.parametrize(
    ("funcao", "data", "vna_base", "inflacao"), CASOS_VNA_PROJETADO
)
def test_vna_projetado_valida_dominio(funcao, data, vna_base, inflacao) -> None:
    with pytest.raises(ValueError, match="VNA-base"):
        funcao(data, 0, inflacao)
    with pytest.raises(ValueError, match="inflação"):
        funcao(data, vna_base, -100)
