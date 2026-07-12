"""Testes de precificação usando o conceito de máscara."""

import datetime as dt
import math

import pytest

from pyield import lft, ltn, ntnb, ntnb1, ntnbp, ntnc, ntnf

DATA_OPERACAO = dt.date(2026, 7, 10)
DATA_LIQUIDACAO = dt.date(2026, 7, 13)
REFERENCIAS_NTNB = [
    ("2026-08-15", 0.1167),
    ("2027-05-15", 0.0844),
    ("2028-08-15", 0.0853),
    ("2029-05-15", 0.0832),
    ("2030-08-15", 0.0832),
    ("2031-05-15", 0.0822),
    ("2032-08-15", 0.0816),
    ("2033-05-15", 0.0809),
    ("2035-05-15", 0.0799),
    ("2037-05-15", 0.0787),
    ("2040-08-15", 0.0771),
    ("2045-05-15", 0.0753),
    ("2050-08-15", 0.0748),
    ("2055-05-15", 0.0741),
    ("2060-08-15", 0.0740),
]

VNAS = {
    "LFT": (19_405.300490, 19_415.561740),
    "LTN": (1_000.0, 1_000.0),
    "NTN-B Princ": (4_738.164713, 4_738.922274),
    "NTN-B": (4_738.164713, 4_738.922274),
    "NTN-B1 Educa+": (4_738.164713, 4_738.922274),
    "NTN-B1 Renda+": (4_738.164713, 4_738.922274),
    "NTN-F": (1_000.0, 1_000.0),
    "NTN-C": (6_646.341898, 6_641.760509),
}

# família, vencimento, taxa compra, taxa venda, PU compra D1, PU venda D0,
# PU venda D1
MASCARA_TD = [
    ("LFT", "2027-03-01", 0.000070, 0.000170, 19414.70, 19403.22, 19413.50),
    ("LFT", "2028-03-01", 0.000173, 0.000273, 19410.08, 19396.64, 19406.92),
    ("LFT", "2029-03-01", 0.000400, 0.000500, 19395.29, 19379.93, 19390.22),
    ("LFT", "2031-03-01", 0.000740, 0.000840, 19349.60, 19330.43, 19340.71),
    ("LTN", "2027-01-01", 0.1368, 0.1380, 941.24, 940.29, 940.78),
    ("LTN", "2028-01-01", 0.1381, 0.1393, 827.01, 825.30, 825.73),
    ("LTN", "2029-01-01", 0.1404, 0.1416, 724.55, 722.31, 722.69),
    ("LTN", "2031-01-01", 0.1427, 0.1439, 553.03, 550.17, 550.46),
    ("LTN", "2032-01-01", 0.1434, 0.1446, 482.36, 479.36, 479.61),
    ("NTN-B Princ", "2026-08-15", 0.1163, 0.1175, 4687.47, 4684.16, 4686.97),
    ("NTN-B Princ", "2029-05-15", 0.0827, 0.0839, 3789.57, 3775.97, 3777.78),
    ("NTN-B Princ", "2032-08-15", 0.0809, 0.0821, 2957.70, 2936.49, 2937.88),
    ("NTN-B Princ", "2035-05-15", 0.0789, 0.0801, 2432.46, 2407.71, 2408.83),
    ("NTN-B Princ", "2040-08-15", 0.0753, 0.0765, 1713.02, 1685.69, 1686.45),
    ("NTN-B Princ", "2045-05-15", 0.0727, 0.0739, 1273.31, 1246.38, 1246.93),
    ("NTN-B Princ", "2050-08-15", 0.0721, 0.0733, 894.77, 870.73, 871.11),
    ("NTN-B", "2026-08-15", 0.1163, 0.1175, 4826.05, 4822.64, 4825.54),
    ("NTN-B", "2030-08-15", 0.0828, 0.0840, 4505.26, 4485.40, 4487.55),
    ("NTN-B", "2032-08-15", 0.0812, 0.0824, 4400.60, 4374.25, 4376.32),
    ("NTN-B", "2035-05-15", 0.0795, 0.0807, 4234.12, 4200.50, 4202.47),
    ("NTN-B", "2037-05-15", 0.0783, 0.0795, 4189.20, 4151.10, 4153.02),
    ("NTN-B", "2040-08-15", 0.0767, 0.0779, 4210.40, 4166.19, 4168.10),
    ("NTN-B", "2045-05-15", 0.0749, 0.0761, 4111.42, 4060.40, 4062.23),
    ("NTN-B", "2050-08-15", 0.0744, 0.0756, 4129.89, 4074.23, 4076.06),
    ("NTN-B", "2055-05-15", 0.0737, 0.0749, 4048.64, 3989.82, 3991.60),
    ("NTN-B", "2060-08-15", 0.0736, 0.0748, 4088.62, 4027.88, 4029.68),
    ("NTN-B1 Educa+", "2030-12-15", 0.0849, 0.0861, 3584.20, 3574.28, 3576.02),
    ("NTN-B1 Educa+", "2031-12-15", 0.0842, 0.0854, 3759.75, 3746.39, 3748.21),
    ("NTN-B1 Educa+", "2032-12-15", 0.0836, 0.0848, 3476.64, 3460.48, 3462.15),
    ("NTN-B1 Educa+", "2033-12-15", 0.0828, 0.0840, 3221.32, 3202.80, 3204.35),
    ("NTN-B1 Educa+", "2034-12-15", 0.0820, 0.0832, 2988.53, 2968.08, 2969.49),
    ("NTN-B1 Educa+", "2035-12-15", 0.0812, 0.0824, 2776.81, 2754.75, 2756.05),
    ("NTN-B1 Educa+", "2036-12-15", 0.0804, 0.0816, 2584.10, 2560.72, 2561.93),
    ("NTN-B1 Educa+", "2037-12-15", 0.0796, 0.0808, 2408.51, 2384.08, 2385.19),
    ("NTN-B1 Educa+", "2038-12-15", 0.0789, 0.0801, 2246.57, 2221.31, 2222.35),
    ("NTN-B1 Educa+", "2039-12-15", 0.0782, 0.0794, 2098.17, 2072.29, 2073.25),
    ("NTN-B1 Educa+", "2040-12-15", 0.0775, 0.0787, 1961.65, 1935.29, 1936.19),
    ("NTN-B1 Educa+", "2041-12-15", 0.0768, 0.0780, 1836.32, 1809.63, 1810.46),
    ("NTN-B1 Educa+", "2042-12-15", 0.0761, 0.0773, 1721.26, 1694.34, 1695.12),
    ("NTN-B1 Educa+", "2043-12-15", 0.0755, 0.0767, 1613.14, 1586.15, 1586.87),
    ("NTN-B1 Educa+", "2044-12-15", 0.0749, 0.0761, 1513.63, 1486.63, 1487.31),
    ("NTN-B1 Educa+", "2045-12-15", 0.0743, 0.0755, 1421.96, 1395.03, 1395.65),
    ("NTN-B1 Educa+", "2046-12-15", 0.0739, 0.0751, 1333.01, 1306.31, 1306.89),
    ("NTN-B1 Educa+", "2047-12-15", 0.0735, 0.0747, 1250.65, 1224.24, 1224.78),
    ("NTN-B1 Educa+", "2048-12-15", 0.0732, 0.0744, 1172.11, 1146.08, 1146.59),
    ("NTN-B1 Renda+", "2049-12-15", 0.0767, 0.0779, 1925.05, 1900.70, 1901.57),
    ("NTN-B1 Renda+", "2054-12-15", 0.0746, 0.0758, 1375.72, 1350.70, 1351.30),
    ("NTN-B1 Renda+", "2059-12-15", 0.0730, 0.0742, 992.75, 969.22, 969.65),
    ("NTN-B1 Renda+", "2064-12-15", 0.0721, 0.0733, 715.04, 694.19, 694.50),
    ("NTN-B1 Renda+", "2069-12-15", 0.0716, 0.0728, 513.29, 495.55, 495.76),
    ("NTN-B1 Renda+", "2074-12-15", 0.0714, 0.0726, 366.45, 351.81, 351.97),
    ("NTN-B1 Renda+", "2079-12-15", 0.0714, 0.0726, 260.09, 248.32, 248.42),
    ("NTN-B1 Renda+", "2084-12-15", 0.0714, 0.0726, 184.62, 175.28, 175.36),
    ("NTN-F", "2027-01-01", 0.1371, 0.1383, 987.06, 986.06, 986.57),
    ("NTN-F", "2029-01-01", 0.1396, 0.1408, 928.23, 925.58, 926.07),
    ("NTN-F", "2031-01-01", 0.1432, 0.1444, 873.00, 869.22, 869.69),
    ("NTN-F", "2033-01-01", 0.1439, 0.1451, 832.46, 827.90, 828.34),
    ("NTN-F", "2035-01-01", 0.1439, 0.1451, 803.86, 798.75, 799.18),
    ("NTN-F", "2037-01-01", 0.1438, 0.1450, 781.98, 776.51, 776.92),
    ("NTN-C", "2031-01-01", 0.0823, 0.0835, 7560.13, 7532.57, 7529.77),
]


def _calcular_pu(
    familia: str,
    data_liquidacao: dt.date,
    vencimento: dt.date,
    taxa: float,
    vna: float,
) -> float:
    """Calcula o PU pela API pública correspondente à família."""
    if familia == "LFT":
        return lft.pu(vna, lft.cotacao(data_liquidacao, vencimento, taxa))
    if familia == "LTN":
        return ltn.pu(data_liquidacao, vencimento, taxa)
    if familia == "NTN-B":
        return ntnb.pu(vna, ntnb.cotacao(data_liquidacao, vencimento, taxa))
    if familia.startswith("NTN-B1"):
        nome = (
            ntnb1.NomeComercial.EDUCA_MAIS
            if "Educa+" in familia
            else ntnb1.NomeComercial.RENDA_MAIS
        )
        return ntnb1.pu(
            vna,
            ntnb1.cotacao(data_liquidacao, vencimento, taxa, nome),
        )
    if familia == "NTN-F":
        return ntnf.pu(data_liquidacao, vencimento, taxa)
    if familia == "NTN-C":
        return ntnc.pu(vna, ntnc.cotacao(data_liquidacao, vencimento, taxa))
    raise ValueError(f"Família não suportada: {familia}")


def _truncar_centavos(valor: float) -> float:
    """Aplica o truncamento final usado na máscara de PU."""
    return math.trunc(valor * 100) / 100


def _gerar_casos() -> list:
    """Desdobra cada título nos cenários de compra e venda D0/D1."""
    casos = []
    for (
        familia,
        vencimento,
        taxa_compra,
        taxa_venda,
        pu_compra,
        pu_d0,
        pu_d1,
    ) in MASCARA_TD:
        vna_d0, vna_d1 = VNAS[familia]
        cenarios = [
            ("compra_d1", DATA_LIQUIDACAO, taxa_compra, vna_d1, pu_compra),
            ("venda_d0", DATA_OPERACAO, taxa_venda, vna_d0, pu_d0),
            ("venda_d1", DATA_LIQUIDACAO, taxa_venda, vna_d1, pu_d1),
        ]
        for lado, data, taxa, vna, esperado in cenarios:
            casos.append(
                pytest.param(
                    (
                        familia,
                        lado,
                        data,
                        vencimento,
                        taxa,
                        vna,
                        esperado,
                    ),
                    id=f"{familia}-{vencimento}-{lado}",
                )
            )
    return casos


@pytest.fixture(scope="module")
def curva_zero_td():
    vencimentos, taxas = zip(*REFERENCIAS_NTNB, strict=True)
    return ntnbp.taxas_zero(
        DATA_LIQUIDACAO,
        vencimentos,
        taxas,
        incluir_vertices=True,
    )


@pytest.mark.parametrize(
    "caso",
    _gerar_casos(),
)
def test_pu_reproduz_mascara_td(caso, curva_zero_td):
    familia, lado, data, vencimento, taxa, vna, pu_esperado = caso
    if familia == "NTN-B Princ":
        ajuste_taxa = -0.0004 if lado == "compra_d1" else 0.0008
        taxa_mercado = ntnbp.taxa(
            DATA_LIQUIDACAO,
            vencimento,
            curva_zero_td,
        )
        cotacao = ntnbp.cotacao(data, vencimento, taxa_mercado + ajuste_taxa)
        pu_calculado = ntnbp.pu(vna, cotacao)
    else:
        pu_calculado = _calcular_pu(familia, data, vencimento, taxa, vna)
    assert _truncar_centavos(pu_calculado) == pytest.approx(
        pu_esperado,
        abs=1e-9,
    )
