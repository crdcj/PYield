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
"""

import datetime as dt
import io
import zipfile as zf

import polars as pl
import polars.selectors as ps
import requests

from pyield._internal.br_numbers import float_br
from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty

URL_BASE = "https://www4.bcb.gov.br/pom/demab/negociacoes/download"

CHAVES_ORDENACAO = ["SettlementDate", "BondType", "MaturityDate"]


def _montar_url(data_alvo: dt.date, extragroup: bool) -> str:
    ano_mes = data_alvo.strftime("%Y%m")
    sufixo = "E" if extragroup else "T"
    return f"{URL_BASE}/Neg{sufixo}{ano_mes}.ZIP"


@ttl_cache()
@retry_padrao
def _baixar_zip(url_arquivo: str) -> bytes:
    """Baixa o conteúdo ZIP e retorna os bytes."""
    resposta = requests.get(url_arquivo, timeout=10)
    resposta.raise_for_status()
    return resposta.content


def _descompactar_zip(conteudo_zip: bytes) -> bytes:
    """Descompacta o ZIP e retorna o conteúdo do primeiro arquivo."""
    with zf.ZipFile(io.BytesIO(conteudo_zip), "r") as arquivo_zip:
        return arquivo_zip.read(arquivo_zip.namelist()[0])


def _parsear_df(conteudo_csv: bytes) -> pl.DataFrame:
    """Lê o CSV extraído do ZIP em DataFrame com todas as colunas como string."""
    return pl.read_csv(
        conteudo_csv,
        encoding="latin1",
        separator=";",
        infer_schema=False,
        null_values="",
    )


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(ps.string().str.strip_chars())
        .with_columns(
            Quantity=pl.col("QUANT NEGOCIADA").cast(pl.Int64),
            AvgPrice=float_br("PU MED"),
        )
        .select(
            SettlementDate=pl.col("DATA MOV").str.to_date("%d/%m/%Y", strict=False),
            BondType=pl.col("SIGLA"),
            SelicCode=pl.col("CODIGO").cast(pl.Int64),
            ISIN=pl.col("CODIGO ISIN"),
            IssueDate=pl.col("EMISSAO").str.to_date("%d/%m/%Y", strict=False),
            MaturityDate=pl.col("VENCIMENTO").str.to_date("%d/%m/%Y", strict=False),
            Trades=pl.col("NUM DE OPER").cast(pl.Int64),
            Quantity=pl.col("Quantity"),
            Value=(pl.col("Quantity") * pl.col("AvgPrice")).round(2),
            MinPrice=float_br("PU MIN"),
            AvgPrice=pl.col("AvgPrice"),
            MaxPrice=float_br("PU MAX"),
            UnderlyingPrice=float_br("PU LASTRO"),
            ParValue=float_br("VALOR PAR"),
            MinRate=float_br("TAXA MIN"),
            AvgRate=float_br("TAXA MED"),
            MaxRate=float_br("TAXA MAX"),
            BrokerageTrades=pl.col("NUM OPER COM CORRETAGEM").cast(pl.Int64),
            BrokerageQuantity=pl.col("QUANT NEG COM CORRETAGEM").cast(pl.Int64),
        )
        .sort(CHAVES_ORDENACAO)
    )


def tpf_monthly_trades(target_date: DateLike, extragroup: bool = False) -> pl.DataFrame:
    """Consulta negociações mensais no mercado secundário de TPF
    registradas no sistema Selic do BCB.

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
        - SettlementDate (Date): data de liquidação da negociação.
        - BondType (str): sigla do título (ex: LFT, LTN, NTN-B, NTN-F).
        - SelicCode (Int64): código único no sistema Selic.
        - ISIN (str): código ISIN (International Securities Identification Number).
        - IssueDate (Date): data de emissão do título.
        - MaturityDate (Date): data de vencimento do título.
        - Trades (Int64): número total de operações realizadas.
        - Quantity (Int64): quantidade total negociada.
        - Value (Float64): valor financeiro negociado (Quantity * AvgPrice).
        - MinPrice (Float64): preço unitário mínimo.
        - AvgPrice (Float64): preço unitário médio.
        - MaxPrice (Float64): preço unitário máximo.
        - UnderlyingPrice (Float64): PU lastro.
        - ParValue (Float64): valor nominal atualizado (VNA) do título.
        - MinRate (Float64): taxa mínima.
        - AvgRate (Float64): taxa média.
        - MaxRate (Float64): taxa máxima.
        - BrokerageTrades (Int64): subconjunto de Trades com corretagem.
        - BrokerageQuantity (Int64): subconjunto de Quantity com corretagem.

    Notes:
        - Dados ordenados por: SettlementDate, BondType, MaturityDate.

    Examples:
        >>> from pyield import bc
        >>> df = bc.tpf_monthly_trades("07-01-2025", extragroup=True)

    """
    if any_is_empty(target_date):
        return pl.DataFrame()
    data_alvo = converter_datas(target_date)
    url = _montar_url(data_alvo, extragroup)
    conteudo_zip = _baixar_zip(url)
    arquivo_extraido = _descompactar_zip(conteudo_zip)
    df = _parsear_df(arquivo_extraido)
    return _processar_df(df)
