"""Conferência da formação do VNA da LFT pelo cálculo público.

Capturas brutas de 22/10/2026 em data/lft_selic.zip, preservadas byte a byte:
- SGS 11: https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados
  formato=json; intervalos 01/07/2000–31/12/2004, 01/01/2005–31/12/2009,
  01/01/2010–31/12/2019 e 01/01/2020–21/10/2026.
- VNA: https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/
  seguido de YYYYMMDDAPC238. Cada nome de arquivo registra a data consultada.

Apenas o fetch do relatório é substituído; a referência é obtida por vna.valor.
O modelo de acumulação fica explícito neste teste; calcular_base recebe o
fator acumulado, sem consultar fontes nem acumular as taxas automaticamente.
"""

import datetime as dt
import json
import re
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal, localcontext
from pathlib import Path
from zipfile import ZipFile

import pytest

import pyield as yd
from pyield.vna import _lft  # noqa: PLC2701

DADOS = Path(__file__).parent / "data" / "lft_selic.zip"
with ZipFile(DADOS) as pacote:
    ARQUIVOS = sorted(nome for nome in pacote.namelist() if nome.endswith("APC238.txt"))
TOTAL_TAXAS = 6607


@pytest.fixture(scope="module")
def dados():
    with ZipFile(DADOS) as pacote:
        yield pacote


@pytest.fixture(scope="module")
def acumulacao(dados):
    taxas = {
        dt.datetime.strptime(linha["data"], "%d/%m/%Y").date(): Decimal(linha["valor"])
        / 100
        for arquivo in dados.namelist()
        if arquivo.startswith("sgs11") and arquivo.endswith(".json")
        for linha in json.loads(dados.read(arquivo))
    }
    assert len(taxas) == TOTAL_TAXAS
    fatores = {}
    with localcontext(prec=28):
        fator = Decimal(1)
        vna_encadeado = Decimal(1000)
        for data, taxa in sorted(taxas.items()):
            fator *= 1 + taxa
            fatores[data] = fator
            vna_encadeado = (vna_encadeado * (1 + taxa)).quantize(
                Decimal("1e-6"), rounding=ROUND_DOWN
            )
    return taxas, fatores, vna_encadeado


@pytest.fixture(autouse=True)
def relatorios_locais(monkeypatch, dados):
    def baixar(data, **kwargs):
        return dados.read(f"{data:%Y%m%d}APC238.txt").decode("latin1")

    monkeypatch.setattr(_lft, "_baixar_texto", baixar)


def _vna(fator, casas_fator=16):
    with localcontext(prec=28):
        acumulado = fator.quantize(Decimal(1).scaleb(-casas_fator), rounding=ROUND_HALF_UP)
        return (Decimal(1000) * acumulado).quantize(Decimal("1e-6"), rounding=ROUND_DOWN)


@pytest.mark.parametrize("arquivo", ARQUIVOS, ids=lambda nome: nome.removesuffix(".txt"))
def test_produtorio_desde_a_origem_reproduz_vna_publicado(arquivo, acumulacao, dados):
    taxas, fatores, _ = acumulacao
    data_arquivo = dt.datetime.strptime(arquivo[:8], "%Y%m%d").date()
    texto = dados.read(arquivo).decode("latin1")
    taxa_relatorio = re.search(r"APURADOS NO SELIC\.+\s+([\d,]+)\s+%", texto)
    assert taxa_relatorio is not None
    assert taxas[data_arquivo] == Decimal(taxa_relatorio[1].replace(",", ".")) / 100

    calculado = yd.vna.calcular_base("LFT", fatores[data_arquivo])

    assert calculado == _vna(fatores[data_arquivo])
    assert calculado == yd.vna.valor("LFT", data_arquivo)


def test_reproduz_exemplo_da_metodologia_stn_com_data_final_exclusiva(acumulacao):
    _, fatores, _ = acumulacao
    # O exemplo do manual é o VNA para 20/05/2008: acumula até 19/05/2008.
    assert _vna(fatores[dt.date(2008, 5, 19)]) == Decimal("3449.694215")
    assert yd.vna.calcular_base("LFT", fatores[dt.date(2008, 5, 19)]) == Decimal(
        "3449.694215"
    )


def test_calculo_lft_aceita_float_e_independe_do_contexto_decimal():
    with localcontext(prec=9):
        calculado = yd.vna.calcular_base("LFT", 14.903011480282196)

    assert calculado == Decimal("14903.011480")


def test_calculo_lft_arredonda_fator_antes_de_truncar_vna():
    # O A16 ultrapassa a sexta casa do VNA; um T16 retornaria 1000.000000.
    calculado = yd.vna.calcular_base("LFT", Decimal("1.00000000099999996"))

    assert calculado == Decimal("1000.000001")


def test_calculo_lft_fator_unitario_retorna_base_original():
    assert yd.vna.calcular_base("LFT", 1.0) == Decimal("1000.000000")


def test_arredondar_fator_em_oito_casas_diverge_do_publicado(acumulacao):
    _, fatores, _ = acumulacao
    data = dt.date(2026, 10, 21)

    com_oito_casas = _vna(fatores[data], casas_fator=8)

    assert com_oito_casas == Decimal("20139.601020")
    assert yd.vna.valor("LFT", data) == Decimal("20139.601018")


def test_truncar_vna_diariamente_acumula_perda(acumulacao):
    _, _, encadeado = acumulacao

    assert encadeado == Decimal("20139.583411")
    assert yd.vna.valor("LFT", "21-10-2026") == Decimal("20139.601018")


@pytest.mark.parametrize(
    ("arquivo", "vigencia"),
    [("20240531APC238.txt", "03/06/2024"), ("20260918APC238.txt", "21/09/2026")],
)
def test_relatorio_informa_vigencia_no_proximo_dia_util(arquivo, vigencia, dados):
    texto = dados.read(arquivo).decode("latin1")
    cabecalho = re.search(r"PARA O DIA:\s*(\d{2}/\d{2}/\d{4})", texto)

    assert cabecalho is not None
    assert cabecalho[1] == vigencia
