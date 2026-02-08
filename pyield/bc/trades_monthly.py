"""
Módulo para buscar dados mensais de negociações secundárias da Dívida Pública
Federal (TPF) registradas no sistema Selic do Banco Central do Brasil (BCB).
Os dados são baixados em ZIP, extraídos e carregados em um DataFrame Polars.
Exemplo do formato dos dados (3 primeiras linhas):
DATA MOV  ; SIGLA; CODIGO; CODIGO ISIN ; EMISSAO   ; VENCIMENTO; NUM DE OPER; QUANT NEGOCIADA; VALOR NEGOCIADO; PU MIN        ; PU MED        ; PU MAX        ; PU LASTRO     ; VALOR PAR     ; TAXA MIN; TAXA MED; TAXA MAX; NUM OPER COM CORRETAGEM; QUANT NEG COM CORRETAGEM
02/09/2024; LFT  ; 210100; BRSTNCLF1RC4; 26/10/2018; 01/03/2025;          48;          100221;                ; 15288,00898200; 15292,57098100; 15302,77742100; 15285,54813387; 15288,23830700; -0,1897 ; -0,0565 ; 0,0032  ;                      20;                    16155
02/09/2024; LFT  ; 210100; BRSTNCLF1RD2; 08/03/2019; 01/09/2025;         101;          230120;                ; 15288,23830700; 15294,25937800; 15311,01778200; 15279,49187722; 15288,23830700; -0,1498 ; -0,0395 ; 0,0000  ;                      21;                    19059
02/09/2024; LFT  ; 210100; BRSTNCLF1RE0; 06/09/2019; 01/03/2026;          88;          512642;                ; 15286,63304100; 15288,20025100; 15292,77891300; 15268,60295396; 15288,23830700; -0,0198 ; 0,0002  ; 0,0071  ;                      27;                   121742
...
"""  # noqa: E501

import datetime as dt
import io
import logging
import zipfile as zf

import polars as pl
import requests

from pyield.converters import convert_dates
from pyield.retry import default_retry
from pyield.types import DateLike, any_is_empty

registro = logging.getLogger(__name__)

URL_BASE = "https://www4.bcb.gov.br/pom/demab/negociacoes/download"

MAPA_COLUNAS = {
    "DATA MOV": ("SettlementDate", pl.String),
    "SIGLA": ("BondType", pl.String),
    "CODIGO": ("SelicCode", pl.Int64),
    "CODIGO ISIN": ("ISIN", pl.String),
    "EMISSAO": ("IssueDate", pl.String),
    "VENCIMENTO": ("MaturityDate", pl.String),
    "NUM DE OPER": ("Trades", pl.Int64),
    "QUANT NEGOCIADA": ("Quantity", pl.Int64),
    "VALOR NEGOCIADO": ("Value", pl.Float64),
    "PU MIN": ("MinPrice", pl.Float64),
    "PU MED": ("AvgPrice", pl.Float64),
    "PU MAX": ("MaxPrice", pl.Float64),
    "PU LASTRO": ("UnderlyingPrice", pl.Float64),
    "VALOR PAR": ("ParValue", pl.Float64),
    "TAXA MIN": ("MinRate", pl.Float64),
    "TAXA MED": ("AvgRate", pl.Float64),
    "TAXA MAX": ("MaxRate", pl.Float64),
    "NUM OPER COM CORRETAGEM": ("BrokerageTrades", pl.Int64),
    "QUANT NEG COM CORRETAGEM": ("BrokerageQuantity", pl.Int64),
}

ESQUEMA_CSV = {col: dtype for col, (_, dtype) in MAPA_COLUNAS.items()}
MAPEAMENTO_COLUNAS = {col: alias for col, (alias, _) in MAPA_COLUNAS.items()}

ORDEM_COLUNAS_FINAL = [
    "SettlementDate",
    "BondType",
    "SelicCode",
    "ISIN",
    "IssueDate",
    "MaturityDate",
    "Trades",
    "Quantity",
    "Value",
    "MinPrice",
    "AvgPrice",
    "MaxPrice",
    "UnderlyingPrice",
    "ParValue",
    "MinRate",
    "AvgRate",
    "MaxRate",
    "BrokerageTrades",
    "BrokerageQuantity",
]

CHAVES_ORDENACAO = ["SettlementDate", "BondType", "MaturityDate"]


def _montar_url(data_alvo: dt.date, extragroup: bool) -> str:
    """Monta a URL de download do arquivo ZIP de negociações mensais.

    URL com todos os arquivos disponíveis:
    https://www4.bcb.gov.br/pom/demab/negociacoes/apresentacao.asp?frame=1

    Formato do arquivo com todas as operações: NegTYYYYMM.ZIP
    Formato do arquivo apenas extragrupo: NegEYYYYMM.ZIP
    """
    ano_mes = data_alvo.strftime("%Y%m")
    sufixo_operacao = "E" if extragroup else "T"
    return f"{URL_BASE}/Neg{sufixo_operacao}{ano_mes}.ZIP"


@default_retry
def _baixar_zip(url_arquivo: str) -> bytes:
    """Baixa o conteúdo ZIP e retorna os bytes."""
    resposta = requests.get(url_arquivo, timeout=10)
    resposta.raise_for_status()
    return resposta.content


def _descompactar_zip(conteudo_zip: bytes) -> bytes:
    """Descompacta o ZIP e retorna o conteúdo do primeiro arquivo."""
    with zf.ZipFile(io.BytesIO(conteudo_zip), "r") as arquivo_zip:
        return arquivo_zip.read(arquivo_zip.namelist()[0])


def _ler_df_zip(conteudo_csv: bytes) -> pl.DataFrame:
    """Lê o CSV em bytes e retorna DataFrame Polars."""
    return pl.read_csv(
        conteudo_csv,
        decimal_comma=True,
        encoding="latin1",
        separator=";",
        schema_overrides=ESQUEMA_CSV,
    )


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Processa tipos, calcula valor e ordena."""
    colunas_data = ["SettlementDate", "IssueDate", "MaturityDate"]
    return (
        df.rename(MAPEAMENTO_COLUNAS)
        .with_columns(
            pl.col(colunas_data).str.to_date(format="%d/%m/%Y", strict=False),
            Value=(pl.col("Quantity") * pl.col("AvgPrice")).round(2),
        )
        .sort(by=CHAVES_ORDENACAO)
    )


def _ordenar_selecionar_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Reordena colunas e ordena linhas para saída consistente e determinística."""
    colunas_selecionadas = [col for col in ORDEM_COLUNAS_FINAL if col in df.columns]
    return df.select(colunas_selecionadas).sort(by=CHAVES_ORDENACAO)


def tpf_monthly_trades(target_date: DateLike, extragroup: bool = False) -> pl.DataFrame:
    """Consulta negociações mensais no mercado secundário de Títulos Públicos Federais (TPF)
    registradas no sistema Selic do Banco Central do Brasil (BCB).

    Baixa os dados mensais de negociação do site do BCB para o mês correspondente
    à data fornecida. Os dados são baixados como arquivo ZIP, extraídos e carregados
    em um DataFrame Polars. Contém todas as negociações do mês, separadas por
    data de liquidação (SettlementDate).

    Args:
        target_date: Data de referência. Apenas ano e mês são utilizados para
            baixar o arquivo correspondente.
        extragroup: Se True, busca apenas negociações extragrupo (entre grupos
            econômicos distintos). Se False, busca todas. Default é False.
            Negociações extragrupo são aquelas em que o conglomerado da contraparte
            cedente difere do conglomerado da contraparte cessionária, ou quando ao
            menos uma das contrapartes não pertence a um conglomerado. No caso de
            fundos, considera-se o conglomerado do administrador.

    Returns:
        DataFrame com dados de negociação do mês especificado. Em caso de erro
        retorna DataFrame vazio e registra log da exceção.

    Output Columns:
        * SettlementDate (Date): data de liquidação da negociação.
        * BondType (str): sigla do título (ex: LFT, LTN, NTN-B, NTN-F).
        * SelicCode (Int64): código único no sistema Selic.
        * ISIN (str): código ISIN (International Securities Identification Number).
        * IssueDate (Date): data de emissão do título.
        * MaturityDate (Date): data de vencimento do título.
        * Trades (Int64): número de operações realizadas.
        * Quantity (Int64): quantidade negociada.
        * Value (Float64): valor financeiro negociado (Quantity * AvgPrice).
        * MinPrice (Float64): preço unitário mínimo.
        * AvgPrice (Float64): preço unitário médio.
        * MaxPrice (Float64): preço unitário máximo.
        * UnderlyingPrice (Float64): PU lastro.
        * ParValue (Float64): valor par.
        * MinRate (Float64): taxa mínima.
        * AvgRate (Float64): taxa média.
        * MaxRate (Float64): taxa máxima.
        * BrokerageTrades (Int64): número de operações com corretagem.
        * BrokerageQuantity (Int64): quantidade negociada com corretagem.

    Notes:
        - Dados ordenados por: SettlementDate, BondType, MaturityDate.

    Examples:
        >>> from pyield import bc
        >>> # Busca todas as negociações de jan/2025
        >>> bc.tpf_monthly_trades("07-01-2025", extragroup=True)
        shape: (1_019, 19)
        ┌────────────────┬──────────┬───────────┬──────────────┬───┬─────────┬─────────┬─────────────────┬───────────────────┐
        │ SettlementDate ┆ BondType ┆ SelicCode ┆ ISIN         ┆ … ┆ AvgRate ┆ MaxRate ┆ BrokerageTrades ┆ BrokerageQuantity │
        │ ---            ┆ ---      ┆ ---       ┆ ---          ┆   ┆ ---     ┆ ---     ┆ ---             ┆ ---               │
        │ date           ┆ str      ┆ i64       ┆ str          ┆   ┆ f64     ┆ f64     ┆ i64             ┆ i64               │
        ╞════════════════╪══════════╪═══════════╪══════════════╪═══╪═════════╪═════════╪═════════════════╪═══════════════════╡
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RC4 ┆ … ┆ 0.0132  ┆ 0.0906  ┆ 2               ┆ 9581              │
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RD2 ┆ … ┆ 0.0561  ┆ 0.101   ┆ 11              ┆ 42823             │
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RE0 ┆ … ┆ 0.0191  ┆ 0.0405  ┆ 19              ┆ 33330             │
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RF7 ┆ … ┆ 0.0304  ┆ 0.05    ┆ 10              ┆ 14583             │
        │ 2025-01-02     ┆ LFT      ┆ 210100    ┆ BRSTNCLF1RG5 ┆ … ┆ 0.0697  ┆ 0.0935  ┆ 12              ┆ 51776             │
        │ …              ┆ …        ┆ …         ┆ …            ┆ … ┆ …       ┆ …       ┆ …               ┆ …                 │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF1P8 ┆ … ┆ null    ┆ null    ┆ 0               ┆ 0                 │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF1Q6 ┆ … ┆ null    ┆ null    ┆ 0               ┆ 0                 │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF204 ┆ … ┆ null    ┆ null    ┆ 12              ┆ 570000            │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF212 ┆ … ┆ null    ┆ null    ┆ 0               ┆ 0                 │
        │ 2025-01-31     ┆ NTN-F    ┆ 950199    ┆ BRSTNCNTF238 ┆ … ┆ null    ┆ null    ┆ 4               ┆ 115000            │
        └────────────────┴──────────┴───────────┴──────────────┴───┴─────────┴─────────┴─────────────────┴───────────────────┘

    """  # noqa: E501
    if any_is_empty(target_date):
        registro.warning("Nenhuma data informada. Retornando DataFrame vazio.")
        return pl.DataFrame()
    try:
        data_alvo = convert_dates(target_date)
        url = _montar_url(data_alvo, extragroup)
        registro.debug(f"Consultando BCB: {url}")
        conteudo_zip = _baixar_zip(url)
        arquivo_extraido = _descompactar_zip(conteudo_zip)
        df = _ler_df_zip(arquivo_extraido)
        df = _processar_df(df)
        df = _ordenar_selecionar_colunas(df)
    except Exception as e:
        registro.exception(f"Erro ao coletar dados mensais do BCB: {e}")
        return pl.DataFrame()

    registro.info(f"Dados processados de {url}. Registros: {len(df)}.")
    return df
