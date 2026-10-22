import datetime as dt
import math
from decimal import Decimal

import polars as pl
import pytest

from pyield import vna
from pyield.vna import _download, calcular_vna  # noqa: PLC2701
from pyield.vna import _ntnb as vna_ntnb  # noqa: PLC2701
from pyield.vna import _ntnc as vna_ntnc  # noqa: PLC2701

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
VNA_TESTE_BASE = Decimal("100.000000")
VNA_TESTE_FINAL = Decimal("121.000000")
VNA_TESTE_INTERMEDIARIO = Decimal("110.000000")
VNA_TESTE_FATOR_ALTERNATIVO = Decimal("1.44")
VNA_TESTE_INTERMEDIARIO_ALTERNATIVO = Decimal("120.000000")
TIPO_VNA = pl.Decimal(precision=18, scale=6)


def _df_teste() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "data": [dt.date(2026, 1, 1), dt.date(2026, 1, 11)],
            "vna": [VNA_TESTE_BASE, VNA_TESTE_FINAL],
        },
        schema_overrides={"data": pl.Date, "vna": TIPO_VNA},
    )


def test_calcular_vna_publico_retorna_ponto_exato() -> None:
    assert calcular_vna.__module__ == "pyield.vna._calculo"

    resultado = calcular_vna(_df_teste(), dt.date(2026, 1, 1))

    assert isinstance(resultado, Decimal)
    assert resultado == VNA_TESTE_BASE


def test_calcular_vna_aceita_vna_float() -> None:
    df = _df_teste().with_columns(vna=pl.col("vna").cast(pl.Float64))

    resultado = calcular_vna(df, dt.date(2026, 1, 6))

    assert isinstance(resultado, Decimal)
    assert resultado == VNA_TESTE_INTERMEDIARIO


def test_calcular_vna_publico_calcula_prorata_e_fator_alternativo() -> None:
    df = _df_teste()

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
def test_calcular_vna_publico_fora_do_intervalo_retorna_decimal_nan(
    data: dt.date,
) -> None:
    resultado = calcular_vna(_df_teste(), data)

    assert isinstance(resultado, Decimal)
    assert resultado.is_nan()


@pytest.mark.parametrize(
    ("titulo", "data", "inicio", "fim"),
    [
        ("NTN-B", "14-07-2026", dt.date(2026, 6, 15), dt.date(2026, 7, 15)),
        ("NTN-B", "15/07/2026", dt.date(2026, 7, 15), dt.date(2026, 8, 15)),
        ("NTN-B", "2026-12-31", dt.date(2026, 12, 15), dt.date(2027, 1, 15)),
        ("NTN-B", dt.date(2027, 1, 1), dt.date(2026, 12, 15), dt.date(2027, 1, 15)),
        ("NTN-B", "29-02-2024", dt.date(2024, 2, 15), dt.date(2024, 3, 15)),
        ("NTN-C", "01-07-2026", dt.date(2026, 7, 1), dt.date(2026, 8, 1)),
        ("NTN-C", "31/07/2026", dt.date(2026, 7, 1), dt.date(2026, 8, 1)),
        ("NTN-C", "2026-12-31", dt.date(2026, 12, 1), dt.date(2027, 1, 1)),
        ("NTN-C", "29-02-2024", dt.date(2024, 2, 1), dt.date(2024, 3, 1)),
        ("NTN-C", "28-02-2025", dt.date(2025, 2, 1), dt.date(2025, 3, 1)),
    ],
)
def test_vigencia_publica(titulo, data, inicio, fim):
    assert vna.vigencia(titulo, data) == (inicio, fim)


@pytest.mark.parametrize("titulo", ["NTN-B", "NTN-C"])
@pytest.mark.parametrize("data", [None, "", "31-02-2026"])
def test_vigencia_rejeita_data_invalida(titulo, data):
    with pytest.raises(ValueError, match="data|Data"):
        vna.vigencia(titulo, data)


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
            "vna": [Decimal("1000.000000"), Decimal("1016.102860")],
        },
        schema_overrides={
            "data": pl.Date,
            "vna": pl.Decimal(precision=18, scale=6),
        },
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
        {"data": dt.date(2000, 7, 15), "vna": Decimal("1000.000000")}
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
            "vna": Decimal("1049.125124"),
        },
        {
            "data": dt.date(2000, 7, 1),
            "anos_vencimento": [2005, 2008, 2011, 2017, 2021, 2031],
            "vna": Decimal("1000.000000"),
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
            "vna": Decimal("1049.125124"),
        },
        {
            "data": dt.date(2000, 7, 1),
            "anos_vencimento": [2005, 2008, 2011, 2017, 2021, 2031],
            "vna": Decimal("1000.000000"),
        },
    ]


def test_processar_ntnc_preserva_vna_em_texto_brasileiro() -> None:
    df_bruto = pl.DataFrame(
        {
            "column_1": ["2018-01-01 00:00:00"],
            "column_2": ["3.580,282456"],
            "column_3": ["3.580,282456"],
        }
    )

    resultado = vna_ntnc._processar(df_bruto)

    assert resultado.to_dicts() == [
        {
            "data": dt.date(2018, 1, 1),
            "anos_vencimento": [2002, 2006],
            "vna": Decimal("3580.282456"),
        },
        {
            "data": dt.date(2018, 1, 1),
            "anos_vencimento": [2005, 2008, 2011, 2017, 2021, 2031],
            "vna": Decimal("3580.282456"),
        },
    ]


def test_processar_ntnb_preserva_vna_em_texto_brasileiro() -> None:
    df_bruto = pl.DataFrame(
        {"column_1": ["2018-01-15 00:00:00"], "column_2": ["\xa03.059,128527"]}
    )

    resultado = vna_ntnb._processar(df_bruto)

    assert resultado.to_dicts() == [
        {"data": dt.date(2018, 1, 15), "vna": Decimal("3059.128527")}
    ]


@pytest.mark.parametrize("modulo", [vna_ntnb, vna_ntnc])
def test_processar_rejeita_vna_ilegivel_em_data_valida(modulo) -> None:
    df_bruto = pl.DataFrame(
        {
            "column_1": ["DATA", "2018-01-01 00:00:00"],
            "column_2": ["VALORES NOMINAIS", "3580#282456"],
            "column_3": ["VALORES NOMINAIS", "0"],
        }
    )

    with pytest.raises(pl.exceptions.InvalidOperationError):
        modulo._processar(df_bruto)


def test_vna_ntnb_calcula_entre_valores_publicados(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        vna_ntnb,
        "vnas",
        lambda *, atualizar=False: pl.DataFrame(
            {
                "data": [dt.date(2025, 12, 15), dt.date(2026, 1, 15)],
                "vna": [
                    Decimal(str(VNA_NTNB_DEZ_2025)),
                    Decimal(str(VNA_NTNB_JAN_2026)),
                ],
            },
            schema_overrides={
                "data": pl.Date,
                "vna": pl.Decimal(precision=18, scale=6),
            },
        ),
    )
    monkeypatch.setattr(
        vna_ntnb._ipca,
        "indice_serie",
        lambda inicio, fim: pl.DataFrame(
            {
                "periodo": [202511, 202512],
                "indice": [7378.94, 7403.29],
            }
        ),
    )

    assert vna.valor("NTN-B", "15-12-2025") == Decimal(str(VNA_NTNB_DEZ_2025))
    assert vna.valor("NTN-B", "30-12-2025") == Decimal(str(VNA_NTNB_30_DEZ_2025))
    assert vna.valor("NTN-B", "14-12-2025").is_nan()
    assert vna.valor("NTN-B", "16-01-2026").is_nan()


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
        lambda *, atualizar=False: pl.DataFrame(
            {
                "data": [dt.date(2026, 7, 15), dt.date(2026, 8, 15)],
                "vna": [
                    Decimal(str(VNA_NTNB_JUL_2026)),
                    Decimal(str(VNA_NTNB_AGO_2026)),
                ],
            },
            schema_overrides={
                "data": pl.Date,
                "vna": pl.Decimal(precision=18, scale=6),
            },
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

    monkeypatch.setattr(vna_ntnb._ipca, "indice_serie", indices)

    assert vna.valor("NTN-B", data) == Decimal(f"{esperado:.6f}")


def test_vna_ntnc_seleciona_serie_e_calcula_entre_valores_publicados(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        vna_ntnc,
        "vnas",
        lambda *, atualizar=False: pl.DataFrame(
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
                    Decimal(str(VNA_NTNC_2006_JUL_2000)),
                    Decimal(str(VNA_NTNC_2006_AGO_2000)),
                    Decimal(str(VNA_NTNC_2031_DEZ_2025)),
                    Decimal(str(VNA_NTNC_2031_JAN_2026)),
                ],
            },
            schema_overrides={
                "data": pl.Date,
                "vna": pl.Decimal(precision=18, scale=6),
            },
        ),
    )

    assert vna.valor("NTN-C", "01-07-2000", "01-01-2006") == Decimal(
        str(VNA_NTNC_2006_JUL_2000)
    )
    assert vna.valor("NTN-C", "16-12-2025", "01-01-2031") == Decimal(
        str(VNA_NTNC_2031_16_DEZ_2025)
    )
    assert vna.valor("NTN-C", "01-07-2000", "01-01-2041").is_nan()


CASOS_VNA_PROJETADO = [
    ("NTN-B", "15-06-2026", VNA_NTNB_JUN_2026, 0.45),
    ("NTN-C", "01-06-2026", VNA_NTNC_2031_JUN_2026, 0.30),
]


@pytest.mark.parametrize(
    ("titulo", "data", "vna_base", "inflacao"), CASOS_VNA_PROJETADO
)
def test_vna_projetado_entradas_vazias(titulo, data, vna_base, inflacao) -> None:
    assert math.isnan(vna.projetado(titulo, None, vna_base, inflacao))


@pytest.mark.parametrize(
    ("titulo", "data", "vna_base", "inflacao"), CASOS_VNA_PROJETADO
)
def test_vna_projetado_valida_dominio(titulo, data, vna_base, inflacao) -> None:
    with pytest.raises(ValueError, match="VNA-base"):
        vna.projetado(titulo, data, 0, inflacao)
    with pytest.raises(ValueError, match="inflação"):
        vna.projetado(titulo, data, vna_base, -100)
