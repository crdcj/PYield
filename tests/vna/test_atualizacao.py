from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace
from zipfile import ZipFile

import pytest
import requests

import pyield as yd
from pyield._internal.cache import ttl_cache  # noqa: PLC2701
from pyield.vna import _download, _lft, _ntnb, _ntnc  # noqa: PLC2701
from tests.vna.test_lft import TEXTO_BCB

TOTAL_REQUISICOES = 4


def _planilha(aba: str, valor: int) -> bytes:
    """Cria uma planilha sintética para exercitar o leitor Excel real."""
    arquivo = BytesIO()
    with ZipFile(arquivo, "w") as zip_file:
        zip_file.writestr(
            "_rels/.rels",
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
            'Target="xl/workbook.xml"/></Relationships>',
        )
        zip_file.writestr(
            "[Content_Types].xml",
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            "</Types>",
        )
        zip_file.writestr(
            "xl/workbook.xml",
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f'<sheets><sheet name="{aba}" sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        zip_file.writestr(
            "xl/_rels/workbook.xml.rels",
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            'Target="worksheets/sheet1.xml"/></Relationships>',
        )
        zip_file.writestr(
            "xl/worksheets/sheet1.xml",
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<sheetData><row r="1">'
            '<c r="A1" t="inlineStr"><is><t>2025-12-15</t></is></c>'
            f'<c r="B1"><v>{valor}</v></c><c r="C1"><v>1</v></c>'
            "</row></sheetData></worksheet>",
        )
    return arquivo.getvalue()


@pytest.mark.parametrize(("titulo", "modulo"), [("NTN-B", _ntnb), ("NTN-C", _ntnc)])
@pytest.mark.parametrize("operacao", ["valor", "historico", "ultimo"])
def test_atualizacao_mensal_renova_cache(monkeypatch, titulo, modulo, operacao):
    downloads = []

    def buscar(url):
        downloads.append(url)
        if url == modulo._URL_PUBLICACAO:
            return (
                b'<a href="https://thot-arquivos.tesouro.gov.br/publicacao/1">VNA</a>'
            )
        return _planilha("NTNB" if titulo == "NTN-B" else "NTN-C", len(downloads))

    # Isola o cache; somente o transporte é substituído no fluxo de consulta.
    monkeypatch.setattr(
        _download, "baixar_planilha", ttl_cache()(_download.baixar_planilha.__wrapped__)
    )
    monkeypatch.setattr(_download, "_buscar_conteudo", buscar)
    vencimento = "01-01-2031" if titulo == "NTN-C" else None

    def consultar(**kwargs):
        funcao = getattr(yd.vna, operacao)
        if operacao == "valor":
            return funcao(titulo, "15-12-2025", vencimento, **kwargs)
        return funcao(titulo, vencimento, **kwargs).item(0, "vna")

    assert consultar() == Decimal(2)
    assert consultar() == Decimal(2)
    assert consultar(atualizar=True) == Decimal(4)
    assert consultar() == Decimal(4)
    assert len(downloads) == TOTAL_REQUISICOES  # página e planilha em cada download


def test_lft_atualiza_apenas_data_consultada_e_propaga_erro(monkeypatch):
    chamadas = []
    falhar = False

    def buscar(url, **kwargs):
        chamadas.append(url)
        if falhar:
            raise requests.HTTPError("falha de atualização")
        texto = TEXTO_BCB.replace("14903,011480", f"{len(chamadas)},000000")
        return SimpleNamespace(text=texto, raise_for_status=lambda: None)

    # Remove o retry no teste para verificar o erro sem esperas.
    monkeypatch.setattr(
        _lft, "_baixar_texto", ttl_cache()(_lft._baixar_texto.__wrapped__.__wrapped__)
    )
    monkeypatch.setattr(_lft.requests, "get", buscar)
    assert yd.vna.valor("LFT", "31-05-2024") == Decimal(1)
    assert yd.vna.valor("LFT", "29-05-2024") == Decimal(2)
    assert yd.vna.valor("LFT", "31-05-2024", atualizar=True) == Decimal(3)
    assert yd.vna.valor("LFT", "31-05-2024") == Decimal(3)
    assert yd.vna.valor("LFT", "29-05-2024") == Decimal(2)
    falhar = True
    with pytest.raises(requests.HTTPError, match="falha"):
        yd.vna.valor("LFT", "31-05-2024", atualizar=True)
    assert yd.vna.valor("LFT", "31-05-2024") == Decimal(3)
    assert len(chamadas) == TOTAL_REQUISICOES
