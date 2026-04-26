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
from pyield.relogio import hoje

URL_BASE = "https://www4.bcb.gov.br/pom/demab/negociacoes/download"

CHAVES_ORDENACAO = ["data_liquidacao", "titulo", "data_vencimento"]


def _montar_url(data_alvo: dt.date, extragrupo: bool) -> str:
    ano_mes = data_alvo.strftime("%Y%m")
    sufixo = "E" if extragrupo else "T"
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
            quantidade=pl.col("QUANT NEGOCIADA").cast(pl.Int64),
            pu_medio=float_br("PU MED"),
        )
        .select(
            data_liquidacao=pl.col("DATA MOV").str.to_date("%d/%m/%Y", strict=False),
            titulo=pl.col("SIGLA"),
            codigo_selic=pl.col("CODIGO").cast(pl.Int64),
            isin=pl.col("CODIGO ISIN"),
            data_emissao=pl.col("EMISSAO").str.to_date("%d/%m/%Y", strict=False),
            data_vencimento=pl.col("VENCIMENTO").str.to_date("%d/%m/%Y", strict=False),
            operacoes=pl.col("NUM DE OPER").cast(pl.Int64),
            quantidade=pl.col("quantidade"),
            financeiro=(pl.col("quantidade") * pl.col("pu_medio")).round(2),
            pu_minimo=float_br("PU MIN"),
            pu_medio=pl.col("pu_medio"),
            pu_maximo=float_br("PU MAX"),
            pu_lastro=float_br("PU LASTRO"),
            valor_par=float_br("VALOR PAR"),
            taxa_minima=float_br("TAXA MIN"),
            taxa_media=float_br("TAXA MED"),
            taxa_maxima=float_br("TAXA MAX"),
            operacoes_corretagem=pl.col("NUM OPER COM CORRETAGEM").cast(pl.Int64),
            quantidade_corretagem=pl.col("QUANT NEG COM CORRETAGEM").cast(pl.Int64),
        )
        .sort(CHAVES_ORDENACAO)
    )


def secundario_mensal(
    data: DateLike,
    extragrupo: bool = False,
) -> pl.DataFrame:
    """Busca dados mensais do mercado secundário de TPFs.

    Fonte: Banco Central do Brasil, sistema SELIC. Baixa o arquivo mensal de
    negociações secundárias para o mês correspondente à data informada.

    Args:
        data: Data de referência.
        extragrupo: Se verdadeiro, busca apenas negociações extragrupo.

    Returns:
        DataFrame Polars com dados mensais do mercado secundário.

    Output Columns:
        * data_liquidacao (Date): data de liquidação da negociação.
        * titulo (String): sigla do título público.
        * codigo_selic (Int64): código único no sistema SELIC.
        * isin (String): código ISIN.
        * data_emissao (Date): data de emissão do título.
        * data_vencimento (Date): data de vencimento do título.
        * operacoes (Int64): número total de operações.
        * quantidade (Int64): quantidade total negociada.
        * financeiro (Float64): valor financeiro negociado.
        * pu_minimo (Float64): preço unitário mínimo.
        * pu_medio (Float64): preço unitário médio.
        * pu_maximo (Float64): preço unitário máximo.
        * pu_lastro (Float64): preço unitário de lastro.
        * valor_par (Float64): valor par do título.
        * taxa_minima (Float64): taxa mínima.
        * taxa_media (Float64): taxa média.
        * taxa_maxima (Float64): taxa máxima.
        * operacoes_corretagem (Int64): operações com corretagem.
        * quantidade_corretagem (Int64): quantidade com corretagem.

    Examples:
        >>> df = yd.tpf.secundario_mensal("07-01-2025", extragrupo=True)
    """
    if any_is_empty(data):
        return pl.DataFrame()
    data_alvo = converter_datas(data)
    if (data_alvo.year, data_alvo.month) > (hoje().year, hoje().month):
        return pl.DataFrame()
    url = _montar_url(data_alvo, extragrupo)
    conteudo_zip = _baixar_zip(url)
    arquivo_extraido = _descompactar_zip(conteudo_zip)
    df = _parsear_df(arquivo_extraido)
    return _processar_df(df)
