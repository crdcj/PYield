import datetime as dt
import io
import logging
import zipfile
from typing import Literal

import polars as pl
import requests
from lxml import etree
from lxml.etree import _Element

import pyield._internal.converters as cv
from pyield._internal.retry import DadoIndisponivelError, retry_padrao
from pyield._internal.types import DateLike, any_is_empty
from pyield.b3.validar_pregao import data_negociacao_valida

registro = logging.getLogger(__name__)

# --- Configuração de Contratos ---
# Contratos de taxa (sufixo "Rate" em colunas como "OpenRate", "CloseRate")
# Contratos de preço usam sufixo "Price" (como "OpenPrice", "ClosePrice")
CONTRATOS_TAXA = {"DI1", "DAP", "DDI", "FRC", "FRO"}

# --- Constantes de Processamento XML ---
NAMESPACE_B3 = "urn:bvmf.217.01.xsd"
NAMESPACES = {"ns": NAMESPACE_B3}
# ZIP válido do price report ~2KB; 1KB detecta arquivos "sem dados"
MIN_TAMANHO_ZIP_BYTES = 1024
# Comprimentos de ticker B3:
# - 6 chars: futuros padrão AAAnYY (ex.: DI1F26)
# - 13 chars: opções sobre futuros AAAnYYTssssss (ex.: CPMF25C100750)
TAMANHO_TICKER_FUTURO = 6
TAMANHO_TICKER_OPCAO = 13
MODELO_XPATH_TICKER = '//ns:TckrSymb[starts-with(text(), "{codigo_ativo}")]'
XPATH_DATA_NEGOCIACAO = ".//ns:TradDt/ns:Dt"
XPATH_ATRIBUTOS_INSTRUMENTO = ".//ns:FinInstrmAttrbts"
XPATH_DETALHES_NEGOCIO = ".//ns:TradDtls"

# --- Mapeamento de Colunas ---

# Estrutura: (id_pdf, nome_original_xml, nome_novo, tipo_polars)
# As colunas comentadas ficam fora do ETL; ao descomentar, entram no fluxo.
# https://www.b3.com.br/data/files/16/70/29/9C/6219D710C8F297D7AC094EA8/Catalogo_precos_v1.3.pdf
COLUNAS_PRICE_REPORT: list[tuple[str, str, str, type[pl.DataType]]] = [
    ("1.00", "TradDt", "TradeDate", pl.Date),
    ("2.01", "TckrSymb", "TickerSymbol", pl.String),
    ("3.01.01", "Id", "InstrumentId", pl.String),
    ("3.01.02.01", "Prtry", "IdentifierType", pl.String),
    ("3.02.01", "MktIdrCd", "MarketIdentifierCode", pl.String),
    ("4.01", "DaysToSttlm", "DaysToSettlement", pl.String),
    ("4.02", "TradQty", "TradeCount", pl.Int64),
    ("5.01", "MktDataStrmId", "MarketDataStreamId", pl.String),
    ("5.02", "NtlFinVol", "FinancialVolume", pl.Float64),
    ("5.03", "IntlFinVol", "InternationalFinancialVolume", pl.Float64),
    ("5.04", "OpnIntrst", "OpenContracts", pl.Int64),
    ("5.05", "FinInstrmQty", "TradeVolume", pl.Int64),
    ("5.06", "BestBidPric", "BestBid", pl.Float64),
    ("5.07", "BestAskPric", "BestAsk", pl.Float64),
    ("5.08", "FrstPric", "Open", pl.Float64),
    ("5.09", "MinPric", "Min", pl.Float64),
    ("5.10", "MaxPric", "Max", pl.Float64),
    ("5.11", "TradAvrgPric", "Avg", pl.Float64),
    ("5.12", "LastPric", "Close", pl.Float64),
    ("5.13", "RglrTxsQty", "RegularTradeCount", pl.Int64),
    ("5.14", "NonRglrTxsQty", "NonRegularTradeCount", pl.Int64),
    ("5.15", "RglrTraddCtrcts", "RegularTradedContracts", pl.Int64),
    ("5.16", "NonRglrTraddCtrcts", "NonRegularTradedContracts", pl.Int64),
    ("5.17", "NtlRglrVol", "NationalRegularVolume", pl.Float64),
    ("5.18", "NtlNonRglrVol", "NationalNonRegularVolume", pl.Float64),
    ("5.19", "IntlRglrVol", "InternationalRegularVolume", pl.Float64),
    ("5.20", "IntlNonRglrVol", "InternationalNonRegularVolume", pl.Float64),
    ("5.21", "AdjstdQt", "SettlementPrice", pl.Float64),
    ("5.22", "AdjstdQtTax", "SettlementRate", pl.Float64),
    ("5.23", "AdjstdQtStin", "AdjustedQuotationIndicator", pl.String),
    ("5.24", "PrvsAdjstdQt", "PreviousAdjustedQuotation", pl.Float64),
    ("5.25", "PrvsAdjstdQtTax", "PreviousAdjustedRate", pl.Float64),
    ("5.26", "PrvsAdjstdQtStin", "PreviousAdjustedIndicator", pl.String),
    ("5.27", "OscnPctg", "OscillationPercentage", pl.Float64),
    ("5.28", "VartnPts", "VariationPoints", pl.Float64),
    ("5.29", "EqvtVal", "EquivalentValue", pl.Float64),
    ("5.30", "AdjstdValCtrct", "AdjustedValueContract", pl.Float64),
    ("5.31", "MaxTradLmt", "MaxLimit", pl.Float64),
    ("5.32", "MinTradLmt", "MinLimit", pl.Float64),
]

# Colunas cujo nome final recebe sufixo dinâmico (Rate/Price).
COLUNAS_XML_COM_SUFIXO = {
    "MinTradLmt",
    "MaxTradLmt",
    "BestAskPric",
    "BestBidPric",
    "FrstPric",
    "MinPric",
    "TradAvrgPric",
    "MaxPric",
    "LastPric",
}

# Mapa de tipos para cast inicial usando os nomes originais do XML.
TIPOS_XML = {nome_original: tipo for _, nome_original, _, tipo in COLUNAS_PRICE_REPORT}


def _mapa_renomeacao_colunas(codigo_contrato: str) -> dict[str, str]:
    """
    Constrói o dicionário de renomeação dinamicamente baseado no contrato.
    Retorna: {XML_Name: New_Name}
    """
    # 1. Determina o sufixo (Rate ou Price)
    sufixo = "Rate" if codigo_contrato in CONTRATOS_TAXA else "Price"

    mapa_renomeacao = {}
    for _, nome_original, nome_novo, _ in COLUNAS_PRICE_REPORT:
        if nome_original in COLUNAS_XML_COM_SUFIXO:
            mapa_renomeacao[nome_original] = f"{nome_novo}{sufixo}"
        else:
            mapa_renomeacao[nome_original] = nome_novo

    return mapa_renomeacao


@retry_padrao
def _baixar_zip_url(data: dt.date, tipo_fonte: str) -> bytes:
    data_str = data.strftime("%y%m%d")
    if tipo_fonte == "PR":
        url = f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{data_str}.zip"
    elif tipo_fonte == "SPR":
        url = (
            f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRD{data_str}.zip"
        )
    else:
        raise ValueError("Tipo de fonte inválido. Deve ser 'PR' ou 'SPR'.")

    resposta = requests.get(url, timeout=(5, 30))
    resposta.raise_for_status()

    if len(resposta.content) < MIN_TAMANHO_ZIP_BYTES:
        data_str_formatada = data.strftime("%Y-%m-%d")
        raise DadoIndisponivelError(f"Sem dados disponíveis para {data_str_formatada}.")
    return resposta.content


def _extrair_xml_zip_aninhado(conteudo_zip: bytes) -> bytes:
    buffer_zip = io.BytesIO(conteudo_zip)
    with zipfile.ZipFile(buffer_zip, "r") as zip_externo:
        arquivos_externos = zip_externo.namelist()
        if not arquivos_externos:
            raise ValueError("ZIP externo está vazio")
        nome_arquivo_externo = arquivos_externos[0]
        conteudo_arquivo_externo = zip_externo.read(nome_arquivo_externo)
    arquivo_externo = io.BytesIO(conteudo_arquivo_externo)

    with zipfile.ZipFile(arquivo_externo, "r") as zip_interno:
        nomes_arquivos = zip_interno.namelist()
        xml_nomes = [nome for nome in nomes_arquivos if nome.endswith(".xml")]
        if not xml_nomes:
            raise ValueError("Nenhum XML encontrado no ZIP aninhado")
        xml_nomes.sort()
        conteudo_xml = zip_interno.read(xml_nomes[-1])
    return conteudo_xml


def _ticker_valido_para_contrato(ticker: str, codigo_contrato: str) -> bool:
    if codigo_contrato == "CPM":
        return len(ticker) == TAMANHO_TICKER_OPCAO
    return len(ticker) == TAMANHO_TICKER_FUTURO


def _extrair_dados_contrato(
    elemento_ticker: _Element, codigo_contrato: str
) -> dict | None:
    if elemento_ticker.text is None or not _ticker_valido_para_contrato(
        elemento_ticker.text, codigo_contrato
    ):
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


def _parsear_xml_registros(xml_bytes: bytes, codigo_ativo: str) -> list[dict]:
    analisador = etree.XMLParser(
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        recover=True,
        resolve_entities=False,
        no_network=True,
        load_dtd=False,
    )
    arquivo_xml = io.BytesIO(xml_bytes)
    arvore = etree.parse(arquivo_xml, parser=analisador)
    caminho_xpath = MODELO_XPATH_TICKER.format(codigo_ativo=codigo_ativo)
    elementos_ticker = arvore.xpath(caminho_xpath, namespaces=NAMESPACES)

    if not elementos_ticker or not isinstance(elementos_ticker, list):
        return []

    registros = []
    for elemento in elementos_ticker:
        if not isinstance(elemento, etree._Element):
            continue
        dados_contrato = _extrair_dados_contrato(elemento, codigo_ativo)
        if dados_contrato is not None:
            registros.append(dados_contrato)
    return registros


def _converter_para_df(registros: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(registros)
    # Casting usa os nomes originais do XML, que são constantes
    tipos_coluna = {k: v for k, v in TIPOS_XML.items() if k in df.columns}
    return df.cast(tipos_coluna, strict=False)  # type: ignore


def _processar_xml_extraido(xml_bytes: bytes, codigo_contrato: str) -> pl.DataFrame:
    registros = _parsear_xml_registros(xml_bytes, codigo_contrato)
    if not registros:
        return pl.DataFrame()

    df = _converter_para_df(registros)
    mapa_renomeacao = _mapa_renomeacao_colunas(codigo_contrato)
    df = df.rename(mapa_renomeacao, strict=False)
    return df.sort("TickerSymbol")


def _processar_zip(dados_zip: bytes, codigo_contrato: str) -> pl.DataFrame:
    if not dados_zip:
        registro.warning("ZIP XML vazio.")
        return pl.DataFrame()

    xml_bytes = _extrair_xml_zip_aninhado(dados_zip)
    return _processar_xml_extraido(xml_bytes, codigo_contrato)


def fetch_price_report(
    date: DateLike, contract_code: str, source_type: Literal["PR", "SPR"] = "SPR"
) -> pl.DataFrame:
    """Busca e processa o price report da B3 no site oficial.

    Faz o download do ZIP com XML, extrai os dados do contrato e devolve um
    DataFrame Polars com os dados brutos do XML, tipados e com colunas
    renomeadas para nomes padronizados.

    O sufixo das colunas OHLC (Rate vs Price) é definido pelo contrato:
    - Contratos de taxa (DI1, DAP, DDI, FRC, FRO): "OpenRate", "CloseRate"
    - Contratos de preço (DOL, WDO, IND, WIN, etc.): "OpenPrice", "ClosePrice"

    O DataFrame retornado **não** contém colunas calculadas (BDaysToExp,
    DaysToExp, DV01, ForwardRate) nem normalização de taxas. O enriquecimento
    é responsabilidade do módulo consumidor (ex.: ``futures.historical``).

    Args:
        date: Data de negociação no formato 'DD-MM-YYYY', 'DD/MM/YYYY',
            'YYYY-MM-DD' ou objeto datetime.date.
        contract_code: Código B3 (ex.: 'DI1', 'DOL', 'DAP', 'FRC', 'DDI',
            'WDO', 'IND', 'WIN'). Os 3 primeiros caracteres são usados no XML.
        source_type: Tipo de arquivo. 'SPR' (default) para settlement price
            report e 'PR' para price report regular.

    Returns:
        pl.DataFrame: DataFrame com colunas tipadas e renomeadas, ordenado por
        TickerSymbol. Inclui todos os registros do XML (sem filtro de
        vencimento). Retorna DataFrame vazio para data inválida, resposta
        vazia ou falhas de parsing recuperáveis.

    Raises:
        ValueError: Se source_type for inválido.
        DadoIndisponivelError: Se a data for válida, mas o endpoint não fornecer
            arquivo para a data consultada.
        requests.HTTPError: Se a requisição HTTP ao endpoint falhar.

    Examples:
        >>> import pyield as yd
        >>> df = yd.b3.fetch_price_report("26-04-2024", "DI1")
        >>> df.is_empty() or {"TradeDate", "TickerSymbol", "CloseRate"}.issubset(
        ...     set(df.columns)
        ... )
        True

        >>> # Feriado ou fim de semana (retorna DataFrame vazio)
        >>> df = yd.b3.fetch_price_report("25-12-2023", "DI1")  # Véspera de Natal
        >>> df.is_empty()
        True
    """
    msg_vazia = f"Sem dados para {contract_code} em {date}. Retornando DataFrame vazio."
    if any_is_empty(date):
        registro.warning(msg_vazia)
        return pl.DataFrame()

    date = cv.converter_datas(date)
    # Validação centralizada (evita chamadas desnecessárias às APIs B3)
    if not data_negociacao_valida(date):
        registro.warning(f"{date} não é uma data válida. Retornando DataFrame vazio.")
        return pl.DataFrame()

    try:
        dados_zip = _baixar_zip_url(date, source_type)

        if not dados_zip:
            registro.warning(msg_vazia)
            return pl.DataFrame()

        df = _processar_zip(dados_zip, contract_code)

        if df.is_empty():
            registro.warning(msg_vazia)

        return df

    except (ValueError, DadoIndisponivelError, requests.HTTPError):
        raise
    except (zipfile.BadZipFile, etree.XMLSyntaxError):
        registro.warning(
            f"Falha ao parsear o price report de {contract_code} em {date}."
        )
        return pl.DataFrame()
    except Exception:
        registro.exception(
            f"ERRO CRÍTICO: Falha ao processar {contract_code} {source_type} em {date}"
        )
        raise


def read_price_report(
    xml_bytes: bytes,
    contract_code: str,
) -> pl.DataFrame:
    """Lê e processa o price report da B3 a partir do conteúdo XML bruto.

    Retorna os dados brutos do XML (tipados e renomeados), sem enriquecimento.

    Args:
        xml_bytes: Conteúdo do XML em bytes (já descomprimido).
        contract_code: Código B3 do contrato.

    Returns:
        pl.DataFrame: DataFrame com colunas tipadas e renomeadas, ordenado por
        TickerSymbol.
    """
    if not xml_bytes:
        registro.warning("XML vazio.")
        return pl.DataFrame()

    return _processar_xml_extraido(xml_bytes, contract_code)
