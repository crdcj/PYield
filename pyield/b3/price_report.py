import datetime as dt
import io
import zipfile

import polars as pl
import requests
from lxml import etree

import pyield._internal.converters as cv
from pyield._internal.cache import ttl_cache
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty
from pyield.b3._contracts import normalizar_codigos_contrato
from pyield.b3._validar_pregao import data_negociacao_valida

# --- Constantes de Processamento XML ---
NAMESPACE_B3 = "urn:bvmf.217.01.xsd"
NAMESPACES = {"ns": NAMESPACE_B3}
# ZIP válido do price report ~2KB; 1KB detecta arquivos "sem dados"
MIN_TAMANHO_ZIP_BYTES = 1024
XPATH_TODOS_TICKERS = "//ns:TckrSymb"
XPATH_DATA_NEGOCIACAO = ".//ns:TradDt/ns:Dt"
XPATH_ATRIBUTOS_INSTRUMENTO = ".//ns:FinInstrmAttrbts"
XPATH_DETALHES_NEGOCIO = ".//ns:TradDtls"

# --- Mapeamento de Colunas ---

# Estrutura: (id_pdf, nome_xml, tipo_polars)
# Esta camada base preserva os nomes originais do XML da B3.
# https://www.b3.com.br/data/files/16/70/29/9C/6219D710C8F297D7AC094EA8/Catalogo_precos_v1.3.pdf
COLUNAS_PRICE_REPORT: list[tuple[str, str, type[pl.DataType]]] = [
    ("1.00", "TradDt", pl.Date),
    ("2.01", "TckrSymb", pl.String),
    ("3.01.01", "Id", pl.String),
    ("3.01.02.01", "Prtry", pl.String),
    ("3.02.01", "MktIdrCd", pl.String),
    ("4.01", "DaysToSttlm", pl.Int64),
    ("4.02", "TradQty", pl.Int64),
    ("5.01", "MktDataStrmId", pl.String),
    ("5.02", "NtlFinVol", pl.Float64),
    ("5.03", "IntlFinVol", pl.Float64),
    ("5.04", "OpnIntrst", pl.Int64),
    ("5.05", "FinInstrmQty", pl.Int64),
    ("5.06", "BestBidPric", pl.Float64),
    ("5.07", "BestAskPric", pl.Float64),
    ("5.08", "FrstPric", pl.Float64),
    ("5.09", "MinPric", pl.Float64),
    ("5.10", "MaxPric", pl.Float64),
    ("5.11", "TradAvrgPric", pl.Float64),
    ("5.12", "LastPric", pl.Float64),
    ("5.13", "RglrTxsQty", pl.Int64),
    ("5.14", "NonRglrTxsQty", pl.Int64),
    ("5.15", "RglrTraddCtrcts", pl.Int64),
    ("5.16", "NonRglrTraddCtrcts", pl.Int64),
    ("5.17", "NtlRglrVol", pl.Float64),
    ("5.18", "NtlNonRglrVol", pl.Float64),
    ("5.19", "IntlRglrVol", pl.Float64),
    ("5.20", "IntlNonRglrVol", pl.Float64),
    ("5.21", "AdjstdQt", pl.Float64),
    ("5.22", "AdjstdQtTax", pl.Float64),
    ("5.23", "AdjstdQtStin", pl.String),
    ("5.24", "PrvsAdjstdQt", pl.Float64),
    ("5.25", "PrvsAdjstdQtTax", pl.Float64),
    ("5.26", "PrvsAdjstdQtStin", pl.String),
    ("5.27", "OscnPctg", pl.Float64),
    ("5.28", "VartnPts", pl.Float64),
    ("5.29", "EqvtVal", pl.Float64),
    ("5.30", "AdjstdValCtrct", pl.Float64),
    ("5.31", "MaxTradLmt", pl.Float64),
    ("5.32", "MinTradLmt", pl.Float64),
]

# Schema completo: nome_xml → tipo_polars. Garante ordem e tipagem constante.
SCHEMA_PRICE_REPORT = {nome: tipo for _, nome, tipo in COLUNAS_PRICE_REPORT}


@ttl_cache()
@retry_padrao
def _baixar_zip_url(data: dt.date, relatorio_completo: bool) -> bytes:
    data_str = data.strftime("%y%m%d")
    if relatorio_completo:
        url = f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{data_str}.zip"
    else:
        url = (
            f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRD{data_str}.zip"
        )

    resposta = requests.get(url, timeout=(5, 30))
    resposta.raise_for_status()

    if len(resposta.content) < MIN_TAMANHO_ZIP_BYTES:
        return bytes()
    return resposta.content


def price_report_extract(conteudo_zip: bytes) -> bytes:
    """Extrai o XML válido do ZIP aninhado do Price Report da B3.

    O ZIP da B3 contém um ZIP interno, que por sua vez contém um ou
    mais XMLs. Esta função extrai o último XML (mais recente).

    Args:
        conteudo_zip: Conteúdo do ZIP externo em bytes.

    Returns:
        Conteúdo do XML extraído em bytes.

    Raises:
        ValueError: Se o ZIP estiver vazio ou não contiver XML.
    """
    with zipfile.ZipFile(io.BytesIO(conteudo_zip), "r") as zip_externo:
        nomes = zip_externo.namelist()
        if not nomes:
            raise ValueError("ZIP externo está vazio")

        conteudo_interno = zip_externo.read(nomes[0])
        with zipfile.ZipFile(io.BytesIO(conteudo_interno), "r") as zip_interno:
            nomes_xml = sorted(n for n in zip_interno.namelist() if n.endswith(".xml"))
            if not nomes_xml:
                raise ValueError("Nenhum XML encontrado no ZIP interno")
            return zip_interno.read(nomes_xml[-1])


def _extrair_dados_contrato(elemento_ticker: etree._Element) -> dict | None:
    if elemento_ticker.text is None:
        return None
    pai = elemento_ticker.getparent()
    if pai is None:
        return None
    registro_pregao = pai.getparent()
    if registro_pregao is None:
        return None
    elemento_data = registro_pregao.find(XPATH_DATA_NEGOCIACAO, NAMESPACES)
    if elemento_data is None:
        return None

    dados_ticker = {"TradDt": elemento_data.text, "TckrSymb": elemento_ticker.text}
    atributos_instr = registro_pregao.find(XPATH_ATRIBUTOS_INSTRUMENTO, NAMESPACES)
    if atributos_instr is None:
        return None

    for attr in atributos_instr:
        nome_tag = etree.QName(attr).localname
        dados_ticker[nome_tag] = attr.text

    detalhes_negocio = registro_pregao.find(XPATH_DETALHES_NEGOCIO, NAMESPACES)
    if detalhes_negocio is not None:
        for detalhe in detalhes_negocio:
            nome_tag = etree.QName(detalhe).localname
            dados_ticker[nome_tag] = detalhe.text

    return dados_ticker


def _parsear_xml_registros(xml_bytes: bytes) -> list[dict]:
    analisador = etree.XMLParser(
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        recover=True,
        resolve_entities=False,
        no_network=True,
        load_dtd=False,
    )
    arvore = etree.parse(io.BytesIO(xml_bytes), parser=analisador)
    resultado_xpath = arvore.xpath(XPATH_TODOS_TICKERS, namespaces=NAMESPACES)
    elementos: list[etree._Element] = resultado_xpath  # type: ignore[assignment]
    if not elementos:
        return []

    registros = []
    for elemento in elementos:
        dados_contrato = _extrair_dados_contrato(elemento)
        if dados_contrato is not None:
            registros.append(dados_contrato)
    return registros


def _converter_para_df(registros: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(registros)
    # Casting usa os nomes originais do XML, que são constantes
    tipos_coluna = {k: v for k, v in SCHEMA_PRICE_REPORT.items() if k in df.columns}
    df = df.cast(tipos_coluna, strict=False)  # type: ignore
    # Adiciona colunas ausentes com null e garante ordem/schema constante
    colunas_faltantes = {
        nome: pl.lit(None).cast(tipo)
        for nome, tipo in SCHEMA_PRICE_REPORT.items()
        if nome not in df.columns
    }
    if colunas_faltantes:
        df = df.with_columns(**colunas_faltantes)
    return df.select(SCHEMA_PRICE_REPORT.keys())


def _processar_xml_extraido(xml_bytes: bytes) -> pl.DataFrame:
    registros = _parsear_xml_registros(xml_bytes)
    if not registros:
        return pl.DataFrame()
    return _converter_para_df(registros).sort("TckrSymb")


def _filtrar_df(
    df: pl.DataFrame,
    prefixos: list[str],
    comprimento_ticker: int | None = None,
) -> pl.DataFrame:
    if comprimento_ticker:
        df = df.filter(pl.col("TckrSymb").str.len_chars() == comprimento_ticker)

    if prefixos:
        filtro = pl.any_horizontal(
            pl.col("TckrSymb").str.starts_with(p) for p in prefixos
        )
        df = df.filter(filtro)
    return df.sort("TckrSymb")


def _obter_df_price_report(data: dt.date, relatorio_completo: bool) -> pl.DataFrame:
    dados_zip = _baixar_zip_url(data, relatorio_completo)
    if not dados_zip:
        return pl.DataFrame()
    xml_bytes = price_report_extract(dados_zip)
    return _processar_xml_extraido(xml_bytes)


def price_report_fetch(
    date: DateLike,
    ticker_prefix: str | list[str] | None = None,
    ticker_length: int | None = None,
    full_report: bool = False,
) -> pl.DataFrame:
    """Busca e processa o price report da B3 no site oficial.

    Faz o download do ZIP com XML, extrai os dados do contrato e devolve um
    DataFrame Polars com os dados brutos do XML e colunas no padrão
    original da B3 (nomes em inglês das tags XML).

    O DataFrame retornado **não** contém colunas calculadas
    (dias_uteis, dias_corridos, dv01, taxa_forward)
    nem normalização semântica por classe de ativo. O enriquecimento é responsabilidade do
    módulo consumidor (ex.: ``futures.historical``).

    Nota:
        O dataset cacheado ``pr`` (arquivo ``b3_pr.parquet``) pode conter um
        subconjunto de colunas focado em futuros. Esta função, porém, opera no
        schema bruto do XML da B3 para a data/relatório consultados.

    Args:
        date: Data de negociação no formato 'DD-MM-YYYY', 'DD/MM/YYYY',
            'YYYY-MM-DD' ou objeto datetime.date.
        ticker_prefix: Prefixo do ticker B3 (ex.: 'DI1', 'DOL',
            'CPM') ou lista de prefixos (ex.: ['DI1', 'DAP']).
            Usado como filtro starts-with no XML. Se None (padrão),
            retorna todos os ativos sem filtro.
        ticker_length: Comprimento exato do ticker para filtrar registros.
            Se None (padrão), retorna todos os tickers que casam com o
            prefixo (ex.: 6 para futuros, 13 para opções).
        full_report: Se False (padrão), usa o simplified price report (SPR),
            arquivo leve (~2 KB) com apenas preços de ajuste. Se True, usa o
            price report completo (PR, ~2 MB) com todos os dados de negociação.

    Returns:
        pl.DataFrame: DataFrame com colunas tipadas no padrão original do XML,
        ordenado por ticker (`TckrSymb`). Inclui todos os registros do XML (sem filtro de
        vencimento). Retorna DataFrame vazio para data inválida ou resposta
        vazia.

    Output Columns:
        * TradDt (Date): data de negociação.
        * TckrSymb (String): código de negociação na B3.
        * Id (String): identificador do instrumento.
        * Prtry (String): tipo do identificador proprietário.
        * MktIdrCd (String): código do mercado.
        * DaysToSttlm (Int64): dias para liquidação.
        * TradQty (Int64): número de negócios.
        * MktDataStrmId (String): identificador do fluxo de dados.
        * NtlFinVol (Float64): volume financeiro nacional bruto.
        * IntlFinVol (Float64): volume financeiro internacional.
        * OpnIntrst (Int64): contratos em aberto.
        * FinInstrmQty (Int64): quantidade negociada de instrumentos financeiros.
                * BestBidPric (Float64): ultima melhor oferta de compra no snapshot
                    diario; pode ser nulo.
                * BestAskPric (Float64): ultima melhor oferta de venda no snapshot
                    diario; pode ser nulo.
        * FrstPric (Float64): preço de abertura.
        * MinPric (Float64): preço mínimo negociado.
        * MaxPric (Float64): preço máximo negociado.
        * TradAvrgPric (Float64): preço médio negociado.
        * LastPric (Float64): preço de fechamento.
        * RglrTxsQty (Int64): número de negócios regulares.
        * NonRglrTxsQty (Int64): número de negócios não regulares.
        * RglrTraddCtrcts (Int64): contratos regulares negociados.
        * NonRglrTraddCtrcts (Int64): contratos não regulares negociados.
        * NtlRglrVol (Float64): volume financeiro regular nacional.
        * NtlNonRglrVol (Float64): volume não regular nacional.
        * IntlRglrVol (Float64): volume regular internacional.
        * IntlNonRglrVol (Float64): volume não regular internacional.
        * AdjstdQt (Float64): preço/cotação de ajuste.
        * AdjstdQtTax (Float64): taxa de ajuste.
        * AdjstdQtStin (String): indicador de cotação ajustada.
        * PrvsAdjstdQt (Float64): preço/cotação de ajuste do dia anterior.
        * PrvsAdjstdQtTax (Float64): taxa de ajuste do dia anterior.
        * PrvsAdjstdQtStin (String): indicador de ajuste anterior.
        * OscnPctg (Float64): percentual de oscilação.
        * VartnPts (Float64): variação em pontos.
        * EqvtVal (Float64): valor equivalente.
        * AdjstdValCtrct (Float64): valor do contrato ajustado.
        * MaxTradLmt (Float64): limite máximo de variação.
        * MinTradLmt (Float64): limite mínimo de variação.

    Raises:
        requests.HTTPError: Se a requisição HTTP ao endpoint falhar.
        zipfile.BadZipFile: Se o ZIP estiver corrompido após re-download.
        etree.XMLSyntaxError: Se o XML recebido estiver malformado.

    Examples:
        >>> import pyield as yd
        >>> df = yd.b3.price_report_fetch("26-04-2024", "DI1")

        >>> # Múltiplos contratos de uma vez
        >>> df = yd.b3.price_report_fetch("26-04-2024", ["DI1", "DAP"])

        >>> # Feriado ou fim de semana (retorna DataFrame vazio)
        >>> df = yd.b3.price_report_fetch("25-12-2023", "DI1")  # Véspera de Natal
        >>> df.is_empty()
        True
    """
    if any_is_empty(date):
        return pl.DataFrame()

    date = cv.converter_datas(date)
    # Validação centralizada (evita chamadas desnecessárias às APIs B3)
    if not data_negociacao_valida(date):
        return pl.DataFrame()

    df = _obter_df_price_report(date, full_report)
    if df.is_empty() or ticker_prefix is None:
        return df

    prefixos = normalizar_codigos_contrato(ticker_prefix)
    if not prefixos:
        return pl.DataFrame()
    return _filtrar_df(df, prefixos, ticker_length)


def price_report_read(
    xml_bytes: bytes,
    ticker_prefix: str | list[str] | None = None,
    ticker_length: int | None = None,
) -> pl.DataFrame:
    """Lê e processa o price report da B3 a partir do conteúdo XML bruto.

    Mesma saída de :func:`price_report_fetch`, mas recebe o XML já
    descomprimido em vez de baixar da rede.

    Args:
        xml_bytes: Conteúdo do XML em bytes (já descomprimido).
        ticker_prefix: Prefixo do ticker B3 (ex.: 'DI1', 'DOL',
            'CPM') ou lista de prefixos (ex.: ['DI1', 'DAP']).
            Se None (padrão), retorna todos os ativos sem filtro.
        ticker_length: Comprimento exato do ticker para filtrar registros.
            Se None (padrão), retorna todos os tickers que casam com o
            prefixo.

    Returns:
        pl.DataFrame: DataFrame com as mesmas colunas documentadas em
        :func:`price_report_fetch`.
    """
    if any_is_empty(xml_bytes):
        return pl.DataFrame()

    df = _processar_xml_extraido(xml_bytes)
    if df.is_empty() or ticker_prefix is None:
        return df

    prefixos = normalizar_codigos_contrato(ticker_prefix)
    if not prefixos:
        return pl.DataFrame()
    return _filtrar_df(df, prefixos, ticker_length)
