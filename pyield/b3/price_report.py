import datetime as dt
import io
import logging
import zipfile
from pathlib import Path
from typing import Literal

import polars as pl
import polars.selectors as cs
import requests
from lxml import etree
from lxml.etree import _Element

import pyield.b3.common as cm
import pyield.converters as cv
from pyield import bday
from pyield.fwd import forwards
from pyield.retry import DataNotAvailableError, default_retry
from pyield.types import DateLike, any_is_empty

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
# Formato de ticker B3: AAAnYY (ex.: DI1F26 = DI1, jan/2026)
TAMANHO_TICKER = 6
TICKER_XPATH_TEMPLATE = '//ns:TckrSymb[starts-with(text(), "{asset_code}")]'
TRADE_DATE_XPATH = ".//ns:TradDt/ns:Dt"
FIN_INSTRM_ATTRBTS_XPATH = ".//ns:FinInstrmAttrbts"

# --- Mapeamento de Colunas ---

# 1. Colunas fixas: nome de destino sempre o mesmo
# Formato: {XML_Name: (New_Name, DataType)}
MAPEAMENTO_BASE = {
    "TradDt": ("TradeDate", pl.Date),
    "TckrSymb": ("TickerSymbol", pl.String),
    "OpnIntrst": ("OpenContracts", pl.Int64),
    "RglrTxsQty": ("TradeCount", pl.Int64),
    "FinInstrmQty": ("TradeVolume", pl.Int64),
    "NtlFinVol": ("FinancialVolume", pl.Float64),
    "AdjstdQt": ("SettlementPrice", pl.Float64),  # Settlement price (PU - Unit Price)
    "AdjstdQtTax": ("SettlementRate", pl.Float64),  # DI1, DAP, ...
    "RglrTraddCtrcts": ("RegularTradedContracts", pl.Int64),
    "NtlRglrVol": ("NationalRegularVolume", pl.Float64),
    "IntlRglrVol": ("InternationalRegularVolume", pl.Float64),
    "OscnPctg": ("OscillationPercentage", pl.Float64),
    "VartnPts": ("VariationPoints", pl.Float64),
    "AdjstdValCtrct": ("AdjustedValueContract", pl.Float64),
    "MktDataStrmId": ("MarketDataStreamId", pl.String),
    "IntlFinVol": ("InternationalFinancialVolume", pl.Float64),
    "AdjstdQtStin": ("AdjustedQuotationIndicator", pl.String),
    "PrvsAdjstdQt": ("PreviousAdjustedQuotation", pl.Float64),
    "PrvsAdjstdQtTax": ("PreviousAdjustedRate", pl.Float64),
    "PrvsAdjstdQtStin": ("PreviousAdjustedIndicator", pl.String),
}

# 2. Colunas variáveis: nome de destino depende do sufixo (Rate ou Price)
# Formato: {XML_Name: (Prefix, DataType)}
MAPEAMENTO_VARIAVEL = {
    "MinTradLmt": ("MinLimit", pl.Float64),
    "MaxTradLmt": ("MaxLimit", pl.Float64),
    "BestAskPric": ("BestAsk", pl.Float64),
    "BestBidPric": ("BestBid", pl.Float64),
    "FrstPric": ("Open", pl.Float64),
    "MinPric": ("Min", pl.Float64),
    "TradAvrgPric": ("Avg", pl.Float64),
    "MaxPric": ("Max", pl.Float64),
    "LastPric": ("Close", pl.Float64),
}

# Agrega todos os tipos para cast inicial (usando nomes originais do XML)
TIPOS_XML = {k: v[1] for k, v in MAPEAMENTO_BASE.items()}
TIPOS_XML.update({k: v[1] for k, v in MAPEAMENTO_VARIAVEL.items()})

COLUNAS_SAIDA = [
    "TradeDate",
    "TickerSymbol",
    "ExpirationDate",
    "BDaysToExp",
    "DaysToExp",
    "OpenContracts",
    "TradeCount",
    "TradeVolume",
    "FinancialVolume",
    "DV01",
    "SettlementPrice",
    # Columns that can be Rate or Price depending on contract type
    "MinLimitRate",
    "MinLimitPrice",
    "MaxLimitRate",
    "MaxLimitPrice",
    "BestBidRate",
    "BestBidPrice",
    "BestAskRate",
    "BestAskPrice",
    "OpenRate",
    "OpenPrice",
    "MinRate",
    "MinPrice",
    "AvgRate",
    "AvgPrice",
    "MaxRate",
    "MaxPrice",
    "CloseRate",
    "ClosePrice",
    "SettlementRate",
    "ForwardRate",
    # Other fields normally not used for analysis but included for completeness
    "MarketDataStreamId",
    "AdjustedQuotationIndicator",
    "RegularTradedContracts",
    "NationalRegularVolume",
    "InternationalRegularVolume",
    "InternationalFinancialVolume",
    "PreviousAdjustedQuotation",
    "PreviousAdjustedRate",
    "PreviousAdjustedIndicator",
    "OscillationPercentage",
    "VariationPoints",
    "AdjustedValueContract",
]


def _mapa_renomeacao_colunas(contract_code: str) -> dict[str, str]:
    """
    Constrói o dicionário de renomeação dinamicamente baseado no contrato.
    Retorna: {XML_Name: New_Name}
    """
    # 1. Determina o sufixo (Rate ou Price)
    sufixo = "Rate" if contract_code in CONTRATOS_TAXA else "Price"

    # 2. Mapeamento Base (Fixo)
    mapa_renomeacao = {k: v[0] for k, v in MAPEAMENTO_BASE.items()}

    # 3. Mapeamento Variável (Com Sufixo)
    # Ex: FrstPric -> OpenRate (se DI1) ou OpenPrice (se DOL)
    for xml_col, (prefix, _) in MAPEAMENTO_VARIAVEL.items():
        mapa_renomeacao[xml_col] = f"{prefix}{sufixo}"

    return mapa_renomeacao


def _ler_zip_arquivo(file_path: Path) -> bytes:
    if not isinstance(file_path, Path):
        raise ValueError("É necessário informar um caminho de arquivo.")
    if not file_path.exists():
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {file_path}.")
    return file_path.read_bytes()


@default_retry
def _baixar_zip_url(date: dt.date, source_type: str) -> bytes:
    data_str = date.strftime("%y%m%d")
    if source_type == "PR":
        url = f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{data_str}.zip"
    elif source_type == "SPR":
        url = (
            f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRD{data_str}.zip"
        )
    else:
        raise ValueError("Tipo de fonte inválido. Deve ser 'PR' ou 'SPR'.")

    resposta = requests.get(url, timeout=(5, 30))
    resposta.raise_for_status()

    if len(resposta.content) < MIN_TAMANHO_ZIP_BYTES:
        data_str_formatada = date.strftime("%Y-%m-%d")
        raise DataNotAvailableError(
            f"Sem dados disponíveis para {data_str_formatada}."
        )
    return resposta.content


def _extrair_xml_zip_aninhado(conteudo_zip: bytes) -> bytes:
    zip_file = io.BytesIO(conteudo_zip)
    with zipfile.ZipFile(zip_file, "r") as zip_externo:
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


def _extrair_dados_contrato(ticker: _Element) -> dict | None:
    if ticker.text is None or len(ticker.text) != TAMANHO_TICKER:
        return None
    parent = ticker.getparent()
    if parent is None:
        return None
    price_report = parent.getparent()
    if price_report is None:
        return None
    date_elem = price_report.find(TRADE_DATE_XPATH, NAMESPACES)
    if date_elem is None:
        return None

    dados_ticker = {"TradDt": date_elem.text, "TckrSymb": ticker.text}
    atributos_instr = price_report.find(FIN_INSTRM_ATTRBTS_XPATH, NAMESPACES)
    if atributos_instr is None:
        return None

    for attr in atributos_instr:
        tag_name = etree.QName(attr).localname
        dados_ticker[tag_name] = attr.text

    return dados_ticker


def _parsear_xml_registros(xml_bytes: bytes, asset_code: str) -> list[dict]:
    parser = etree.XMLParser(
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        recover=True,
        resolve_entities=False,
        no_network=True,
        load_dtd=False,
    )
    arquivo_xml = io.BytesIO(xml_bytes)
    tree = etree.parse(arquivo_xml, parser=parser)
    path = TICKER_XPATH_TEMPLATE.format(asset_code=asset_code)
    tickers = tree.xpath(path, namespaces=NAMESPACES)

    if not tickers or not isinstance(tickers, list):
        return []

    registros = []
    for ticker in tickers:
        if not isinstance(ticker, etree._Element):
            continue
        dados_contrato = _extrair_dados_contrato(ticker)
        if dados_contrato is not None:
            registros.append(dados_contrato)
    return registros


def _converter_para_df(registros: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(registros)
    # Casting usa os nomes originais do XML, que são constantes
    tipos_coluna = {k: v for k, v in TIPOS_XML.items() if k in df.columns}
    return df.cast(tipos_coluna, strict=False)  # type: ignore


def _processar_df(df: pl.DataFrame, contract_code: str) -> pl.DataFrame:
    # 1. Adiciona métricas baseadas em datas
    df = df.with_columns(
        BDaysToExp=bday.count_expr("TradeDate", "ExpirationDate"),
        DaysToExp=(pl.col("ExpirationDate") - pl.col("TradeDate")).dt.total_days(),
    )

    # 2. Normaliza taxas (divide por 100)
    # Seleciona apenas colunas contendo "Rate". Como renomeamos corretamente
    # (OpenPrice para DOL vs OpenRate para DI1), cs.contains("Rate") ignora
    # colunas de preço e afeta apenas colunas de taxa.
    df = df.with_columns(cs.contains("Rate").truediv(100).round(5))

    # 3. Colunas derivadas específicas do contrato
    if contract_code == "DI1":
        # DV01 requer SettlementRate e SettlementPrice
        if "SettlementRate" in df.columns and "SettlementPrice" in df.columns:
            anos_base = pl.col("BDaysToExp") / 252
            duracao_mod = anos_base / (1 + pl.col("SettlementRate"))
            df = df.with_columns(DV01=0.0001 * duracao_mod * pl.col("SettlementPrice"))

    if contract_code in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        forward_rates = forwards(bdays=df["BDaysToExp"], rates=df["SettlementRate"])
        df = df.with_columns(ForwardRate=forward_rates)

    coluna_ordem = [col for col in COLUNAS_SAIDA if col in df.columns]
    return df.select(coluna_ordem).filter(pl.col("DaysToExp") > 0)


def _processar_zip(
    zip_data: bytes, contract_code: str, source_type: str = "SPR"
) -> pl.DataFrame:
    if not zip_data:
        registro.warning("ZIP XML vazio.")
        return pl.DataFrame()

    xml_bytes = _extrair_xml_zip_aninhado(zip_data)
    registros = _parsear_xml_registros(xml_bytes, contract_code)

    if not registros:
        return pl.DataFrame()

    df = _converter_para_df(registros)

    # Aplica renomeação dinâmica baseada no tipo de contrato
    # 1. Gera o mapa correto de renomeação (sufixo Rate ou Price)
    mapa_renomeacao = _mapa_renomeacao_colunas(contract_code)
    df = df.rename(mapa_renomeacao, strict=False)
    df = cm.add_expiration_date(df, contract_code, "TickerSymbol")
    df = _processar_df(df, contract_code)

    return df.sort("ExpirationDate")


def fetch_price_report(
    date: DateLike, contract_code: str, source_type: Literal["PR", "SPR"] = "SPR"
) -> pl.DataFrame:
    """Busca e processa o price report da B3 no site oficial.

    Faz o download do ZIP com XML, extrai os dados do contrato e devolve um
    DataFrame Polars com colunas padronizadas e métricas calculadas.

    A função decide o sufixo das colunas (Rate vs Price) conforme o contrato:
    - Contratos de taxa (DI1, DAP, DDI, FRC, FRO): "OpenRate", "CloseRate"
    - Contratos de preço (DOL, WDO, IND, WIN, etc.): "OpenPrice", "ClosePrice"

    Métricas calculadas:
    - BDaysToExp: Dias úteis até o vencimento
    - DaysToExp: Dias corridos até o vencimento
    - DV01: Valor de 1 bp (para DI1)
    - ForwardRate: Taxa forward (para DI1, DAP)

    Args:
        date: Data de negociação no formato 'DD-MM-YYYY', 'DD/MM/YYYY',
            'YYYY-MM-DD' ou objeto datetime.date.
        contract_code: Código B3 (ex.: 'DI1', 'DOL', 'DAP', 'FRC', 'DDI',
            'WDO', 'IND', 'WIN'). Os 3 primeiros caracteres são usados no XML.
        source_type: Tipo de arquivo. 'SPR' (default) para settlement price
            report e 'PR' para price report regular.

    Returns:
        pl.DataFrame: DataFrame com colunas ordenadas conforme COLUNAS_SAIDA,
        filtrado para excluir contratos vencidos (DaysToExp <= 0). Retorna
        DataFrame vazio se não houver dados ou se a data for inválida.

    Raises:
        ValueError: Se source_type for inválido.
        DataNotAvailableError: Se a data for válida mas não houver dados.
        requests.HTTPError: Se a requisição HTTP falhar.

    Examples:
        >>> import pyield as yd
        >>> df = yd.b3.fetch_price_report("26-04-2024", "DI1")
        >>> df.columns[:5]
        ['TradeDate', 'TickerSymbol', 'ExpirationDate', 'BDaysToExp', 'DaysToExp']
        >>> df.shape[0] > 0
        True

        >>> # Feriado ou fim de semana (retorna DataFrame vazio)
        >>> df = yd.b3.fetch_price_report("25-12-2023", "DI1")  # Véspera de Natal
        >>> df.is_empty()
        True
    """
    msg_vazia = (
        f"Sem dados para {contract_code} em {date}. Retornando DataFrame vazio."
    )
    if any_is_empty(date):
        registro.warning(msg_vazia)
        return pl.DataFrame()

    date = cv.convert_dates(date)
    # Validação centralizada (evita chamadas desnecessárias às APIs B3)
    if not cm.is_trade_date_valid(date):
        registro.warning(f"{date} não é uma data válida. Retornando DataFrame vazio.")
        return pl.DataFrame()

    try:
        dados_zip = _baixar_zip_url(date, source_type)

        if not dados_zip:
            registro.warning(msg_vazia)
            return pl.DataFrame()

        df = _processar_zip(dados_zip, contract_code, source_type)

        if df.is_empty():
            registro.warning(msg_vazia)

        return df

    except (ValueError, DataNotAvailableError, requests.HTTPError):
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
        return pl.DataFrame()


def read_price_report(
    file_path: Path,
    contract_code: str,
    source_type: Literal["PR", "SPR"] | None = None,
) -> pl.DataFrame:
    """Lê e processa o price report da B3 a partir de um ZIP local.

    Args:
        file_path: Caminho do arquivo ZIP local.
        contract_code: Código B3 do contrato.
        source_type: 'SPR' ou 'PR'. Se None, infere pelo prefixo do arquivo.

    Returns:
        pl.DataFrame: DataFrame processado com colunas padronizadas.
    """
    if source_type is None:
        filename = file_path.name
        source_type = "SPR" if filename.startswith("SPRD") else "PR"

    dados_zip = _ler_zip_arquivo(file_path)
    df = _processar_zip(dados_zip, contract_code, source_type)
    return df
