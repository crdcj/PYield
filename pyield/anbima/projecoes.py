"""Projeções de IPCA e IGP-M do Grupo Consultivo Macroeconômico da ANBIMA."""

import datetime as dt
import re
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Literal

import requests
from lxml import html

from pyield import relogio
from pyield._internal.retry import requisitar

_URL = (
    "https://www.anbima.com.br/lumis/portal/controller/html/"
    "ServiceInterfaceInstanceControllerHtml.jsp"
)
_PARAMETROS = {
    "lumPageId": "4028B8816B19A532016B19C2B8A20C4F",
    "lumRenderII": "4028B8816B19A532016B19C351DF0CF7",
    # Sem lumReferer, o portal responde HTTP 200 com corpo vazio.
    "lumReferer": (
        "https://www.anbima.com.br/pt_br/informar/estatisticas/"
        "precos-e-indices/projecao-de-inflacao-gp-m.htm"
    ),
}
_TITULO_TABELA = re.compile(r"PROJEÇÕES PARA O MÊS(?! POSTERIOR)")
_CABECALHO = ["Mês de Coleta", "Data", "Projeção (%)", "Data de Validade*"]
_COMPETENCIA = re.compile(r"([A-ZÇ]+) DE (\d{4})")
_MESES = (
    "JANEIRO",
    "FEVEREIRO",
    "MARÇO",
    "ABRIL",
    "MAIO",
    "JUNHO",
    "JULHO",
    "AGOSTO",
    "SETEMBRO",
    "OUTUBRO",
    "NOVEMBRO",
    "DEZEMBRO",
)
_SEM_PROJECAO = "-"

type _Indice = Literal["IPCA", "IGP-M"]


@dataclass(frozen=True)
class ProjecaoIndicador:
    competencia: dt.date  # Primeiro dia do mês de referência da projeção
    valor_projetado: Decimal  # Valor projetado em unidade decimal
    data_coleta: dt.date  # Dia da coleta das projeções pela ANBIMA
    data_validade: dt.date  # Início da vigência para atualização do VNA


def _buscar_projecoes() -> bytes:
    try:
        resposta = requisitar(requests.get, _URL, params=_PARAMETROS, timeout_leitura=5)
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Erro ao acessar as projeções da ANBIMA: {e}") from e
    return resposta.content


def _texto(elemento: html.HtmlElement) -> str:
    return " ".join(elemento.text_content().split())


def _parsear_projecoes(
    conteudo: bytes, indice: _Indice
) -> tuple[str, list[tuple[str, str, str]]]:
    """Extrai o mês de referência e as linhas (data, projeção, validade)."""
    if not conteudo.strip():
        raise ValueError("A ANBIMA retornou as projeções sem conteúdo.")
    documento = html.fromstring(conteudo.decode("utf-8"))
    abas = {
        _texto(aba): aba.get("href", "").removeprefix("#")
        for aba in documento.xpath("//ul[contains(@class, 'nav-tabs')]//a")
    }
    paineis = documento.xpath("//div[@id=$id]", id=abas.get(indice, ""))
    if not paineis:
        raise ValueError(f"Aba de projeções do {indice} não encontrada.")
    for tabela in paineis[0].iter("table"):
        titulos = [_texto(th) for th in tabela.xpath(".//th")]
        if titulos and _TITULO_TABELA.match(titulos[0]):
            break
    else:
        raise ValueError(f"Tabela de projeções do {indice} não encontrada.")

    cabecalho, *registros = (
        [_texto(td) for td in tr.xpath("./td")] for tr in tabela.xpath("./tbody/tr")
    )
    if cabecalho != _CABECALHO:
        raise ValueError(f"Colunas inesperadas nas projeções do {indice}.")
    return titulos[-1], [
        (data, projecao, validade) for _, data, projecao, validade in registros
    ]


def _processar_projecoes(
    mes_referencia: str, linhas: list[tuple[str, str, str]]
) -> list[ProjecaoIndicador]:
    mes = _COMPETENCIA.fullmatch(mes_referencia)
    if mes is None or mes[1] not in _MESES:
        raise ValueError(f"Mês de referência inválido: {mes_referencia!r}.")
    competencia = dt.date(int(mes[2]), _MESES.index(mes[1]) + 1, 1)
    return [
        ProjecaoIndicador(
            competencia=competencia,
            valor_projetado=(Decimal(projecao.replace(",", ".")) / 100).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_EVEN
            ),
            data_coleta=dt.datetime.strptime(data, "%d/%m/%y").date(),
            data_validade=dt.datetime.strptime(validade, "%d/%m/%y").date(),
        )
        for data, projecao, validade in linhas
        # Coletas agendadas aparecem antes da divulgação, sem valor.
        if projecao != _SEM_PROJECAO
    ]


def _projecao_vigente(indice: _Indice) -> ProjecaoIndicador | None:
    hoje = relogio.hoje()
    projecoes = _processar_projecoes(*_parsear_projecoes(_buscar_projecoes(), indice))
    vigentes = [p for p in projecoes if p.data_validade <= hoje]
    return max(vigentes, key=lambda p: (p.data_validade, p.data_coleta), default=None)


def ipca_taxa_projetada() -> ProjecaoIndicador | None:
    """Obtém a projeção do IPCA vigente hoje, segundo a ANBIMA.

    A função consulta a página de projeções do Grupo Consultivo Permanente
    Macroeconômico da ANBIMA e seleciona, entre as projeções do mês corrente,
    a de maior data de validade que não seja posterior à data de hoje no
    Brasil.

    Returns:
        ProjecaoIndicador | None: Objeto contendo:
            - competencia (dt.date): Primeiro dia do mês de referência da
              projeção (ex.: ``dt.date(2026, 10, 1)``).
            - valor_projetado (Decimal): Valor projetado do IPCA em unidade
              decimal, com quatro casas decimais (0.0045 representa 0,45%).
            - data_coleta (dt.date): Dia em que a ANBIMA coletou as
              projeções.
            - data_validade (dt.date): Data a partir da qual a projeção vale
              para a atualização do Valor Nominal dos títulos indexados ao
              IPCA.

        Retorna ``None`` quando nenhuma projeção do mês corrente está
        vigente, o que ocorre entre a divulgação do IPCA fechado e a data de
        validade da primeira projeção do mês seguinte. Nesse intervalo, a
        atualização do VNA usa o índice oficial.

    Raises:
        ConnectionError: Se houver erro de conexão com o site da ANBIMA.
        ValueError: Se a página não tiver a estrutura esperada.

    Notes:
        Fonte: ANBIMA, página "Projeções IPCA e IGP-M"
        (https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm),
        lida do conteúdo que o portal carrega dinamicamente.

        A coleta ocorre no dia da divulgação do IPCA-15 e do IPCA fechado;
        o resultado é publicado por volta das 17h30 e passa a valer na data
        de validade informada pela ANBIMA. Uma projeção recém-divulgada só é
        retornada a partir da sua data de validade.

        O quadro de indicadores da ANBIMA (``indicadores.xls``) não é usado
        porque não informa a validade e pode permanecer com a projeção
        anterior depois que a nova já está vigente.

        O valor é convertido do texto da página para Decimal, dividido por
        100 e arredondado em quatro casas decimais, com empate para o par.

    Examples:
        >>> projecao = yd.ipca.taxa_projetada()
        >>> projecao is None or projecao.data_validade <= yd.hoje()
        True
    """
    return _projecao_vigente("IPCA")


def igpm_taxa_projetada() -> ProjecaoIndicador | None:
    """Obtém a projeção do IGP-M vigente hoje, segundo a ANBIMA.

    A função consulta a página de projeções do Grupo Consultivo Permanente
    Macroeconômico da ANBIMA e seleciona, entre as projeções do mês corrente,
    a de maior data de validade que não seja posterior à data de hoje no
    Brasil.

    Returns:
        ProjecaoIndicador | None: Objeto contendo:
            - competencia (dt.date): Primeiro dia do mês de referência da
              projeção (ex.: ``dt.date(2026, 11, 1)``).
            - valor_projetado (Decimal): Valor projetado do IGP-M em unidade
              decimal, com quatro casas decimais (0.0078 representa 0,78%).
            - data_coleta (dt.date): Dia em que a ANBIMA coletou as
              projeções.
            - data_validade (dt.date): Data a partir da qual a projeção vale
              para a atualização do Valor Nominal dos títulos indexados ao
              IGP-M.

        Retorna ``None`` quando nenhuma projeção do mês corrente está
        vigente, o que ocorre entre a divulgação do IGP-M fechado e a data de
        validade da primeira projeção do mês seguinte. Nesse intervalo, a
        atualização do VNA usa o índice oficial.

    Raises:
        ConnectionError: Se houver erro de conexão com o site da ANBIMA.
        ValueError: Se a página não tiver a estrutura esperada.

    Notes:
        Fonte: ANBIMA, página "Projeções IPCA e IGP-M"
        (https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm),
        lida do conteúdo que o portal carrega dinamicamente.

        A coleta ocorre no dia da divulgação de cada prévia e do IGP-M
        fechado; o resultado é publicado por volta das 17h30 e passa a valer
        na data de validade informada pela ANBIMA. Uma projeção
        recém-divulgada só é retornada a partir da sua data de validade.

        O valor é convertido do texto da página para Decimal, dividido por
        100 e arredondado em quatro casas decimais, com empate para o par.

    Examples:
        >>> projecao = yd.igpm.taxa_projetada()
        >>> projecao is None or projecao.data_validade <= yd.hoje()
        True
    """
    return _projecao_vigente("IGP-M")
