"""Dados mensais do mercado secundário de TPFs no sistema Selic do BCB."""

import datetime as dt
import io
import os
import zipfile as zf
from pathlib import Path

import polars as pl
import polars.selectors as ps
import requests

from pyield import relogio
from pyield._internal.br_numbers import float_br
from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty

URL_BASE_MENSAL = "https://www4.bcb.gov.br/pom/demab/negociacoes/download"
CHAVES_ORDENACAO = ["data_liquidacao", "titulo", "data_vencimento"]
COLUNAS_MINIMAS_CSV = 2

type CaminhoArquivo = str | os.PathLike[str]


def _tipo_arquivo(extragrupo: bool) -> str:
    return "E" if extragrupo else "T"


def _data_mensal(data: DateLike) -> dt.date:
    data_alvo = converter_datas(data)
    if not isinstance(data_alvo, dt.date):
        msg = "data deve ser escalar para consultas mensais do secundário de TPFs"
        raise ValueError(msg)
    return data_alvo


def _nome_arquivo_mensal(data: DateLike, extragrupo: bool = False) -> str:
    """Retorna o nome do ZIP mensal no BCB (ex.: ``NegT202606.ZIP``)."""
    data_alvo = _data_mensal(data)
    return f"Neg{_tipo_arquivo(extragrupo)}{data_alvo:%Y%m}.ZIP"


@ttl_cache()
@retry_padrao
def _baixar_url_zip(url_arquivo: str) -> bytes:
    resposta = requests.get(url_arquivo, allow_redirects=True, timeout=60)
    resposta.raise_for_status()
    return resposta.content


def baixar_zip(data: DateLike, extragrupo: bool = False) -> bytes:
    """Baixa o ZIP bruto mensal de negociações secundárias de TPFs.

    Fonte: Banco Central do Brasil, sistema SELIC. A fonte publica um arquivo
    por mês; por isso, apenas o ano e o mês de ``data`` são usados.

    A função valida a estrutura mínima do ZIP antes de retornar os bytes, para
    evitar que pipelines de ingestão salvem arquivo vazio, corrompido ou sem
    CSV plausível. Use :func:`ler` para processar os bytes ou o arquivo salvo.

    Args:
        data: Data de referência. Apenas ano e mês definem o arquivo.
        extragrupo: Se verdadeiro, baixa o arquivo extragrupo.

    Returns:
        Bytes validados do arquivo ZIP mensal publicado pelo BCB.

    Raises:
        requests.HTTPError: Se o arquivo não estiver disponível no BCB ou a
            resposta HTTP indicar erro.
        ValueError: Se o conteúdo baixado não for um ZIP bruto plausível.

    Examples:
        >>> conteudo = yd.tpf.secundario.baixar_zip("07-01-2025")  # doctest: +SKIP
    """
    arquivo = _nome_arquivo_mensal(data, extragrupo)
    conteudo_zip = _baixar_url_zip(f"{URL_BASE_MENSAL}/{arquivo}")
    _validar_zip(conteudo_zip, arquivo)
    return conteudo_zip


def _extrair_csv_zip(conteudo_zip: bytes) -> bytes:
    try:
        arquivo_zip = zf.ZipFile(io.BytesIO(conteudo_zip), "r")
    except zf.BadZipFile as exc:
        msg = "ZIP inválido ou ilegível"
        raise ValueError(msg) from exc

    with arquivo_zip:
        nomes = arquivo_zip.namelist()
        if not nomes:
            raise ValueError("ZIP vazio")

        arquivo_corrompido = arquivo_zip.testzip()
        if arquivo_corrompido is not None:
            msg = f"ZIP contém arquivo corrompido: {arquivo_corrompido}"
            raise ValueError(msg)

        return arquivo_zip.read(nomes[0])


def _validar_zip(conteudo_zip: bytes, nome: str | None = None) -> None:
    rotulo = f"{nome}: " if nome else ""
    try:
        conteudo_csv = _extrair_csv_zip(conteudo_zip)
    except ValueError as exc:
        msg = f"{rotulo}{exc}"
        raise ValueError(msg) from exc

    df_amostra = pl.read_csv(
        conteudo_csv,
        encoding="latin1",
        separator=";",
        infer_schema=False,
        null_values="",
        n_rows=1,
    )

    if len(df_amostra.columns) < COLUNAS_MINIMAS_CSV:
        msg = f"{rotulo}CSV não parece estar separado por ponto e vírgula"
        raise ValueError(msg)


def _parsear_csv_mensal(conteudo_csv: bytes) -> pl.DataFrame:
    return pl.read_csv(
        conteudo_csv,
        encoding="latin1",
        separator=";",
        infer_schema=False,
        null_values="",
    )


def _processar_df_mensal(df: pl.DataFrame) -> pl.DataFrame:
    operacoes_corretagem = (
        pl.col("NUM OPER COM CORRETAGEM").cast(pl.Int64)
        if "NUM OPER COM CORRETAGEM" in df.columns
        else pl.lit(None, dtype=pl.Int64)
    )
    quantidade_corretagem = (
        pl.col("QUANT NEG COM CORRETAGEM").cast(pl.Int64)
        if "QUANT NEG COM CORRETAGEM" in df.columns
        else pl.lit(None, dtype=pl.Int64)
    )

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
            pu_minimo=float_br("PU MIN"),
            pu_medio=pl.col("pu_medio"),
            pu_maximo=float_br("PU MAX"),
            pu_lastro=float_br("PU LASTRO"),
            valor_par=float_br("VALOR PAR"),
            taxa_minima=float_br("TAXA MIN"),
            taxa_media=float_br("TAXA MED"),
            taxa_maxima=float_br("TAXA MAX"),
            operacoes_corretagem=operacoes_corretagem,
            quantidade_corretagem=quantidade_corretagem,
        )
        .sort(CHAVES_ORDENACAO)
        .with_columns(
            financeiro=(pl.col("quantidade") * pl.col("pu_medio")).round(2),
        )
    )


def ler(fonte: bytes | CaminhoArquivo) -> pl.DataFrame:
    """Lê o ZIP mensal bruto do secundário de TPFs.

    Fonte: Banco Central do Brasil, sistema SELIC. Mesma saída de
    :func:`mensal`, mas recebe o ZIP bruto em vez de baixá-lo. Útil para
    processar arquivos salvos com :func:`baixar_zip`.

    Args:
        fonte: ZIP bruto em bytes ou caminho para o arquivo ZIP no disco.

    Returns:
        DataFrame Polars com as colunas documentadas em :func:`mensal`.

    Raises:
        ValueError: Se o conteúdo não for um ZIP válido.

    Examples:
        >>> conteudo = yd.tpf.secundario.baixar_zip("07-01-2025")  # doctest: +SKIP
        >>> df = yd.tpf.secundario.ler(conteudo)  # doctest: +SKIP
        >>> df = yd.tpf.secundario.ler("NegT202501.ZIP")  # doctest: +SKIP
    """
    conteudo_zip = fonte if isinstance(fonte, bytes) else Path(fonte).read_bytes()
    return _processar_df_mensal(_parsear_csv_mensal(_extrair_csv_zip(conteudo_zip)))


def mensal(data: DateLike, extragrupo: bool = False) -> pl.DataFrame:
    """Busca dados mensais do mercado secundário de TPFs.

    Fonte: Banco Central do Brasil, sistema SELIC. Baixa e valida o ZIP mensal
    de negociações secundárias e retorna os dados processados.
    Apenas o ano e o mês de ``data`` são usados para identificar o arquivo.

    Args:
        data: Data de referência. Apenas ano e mês definem o arquivo.
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
        * financeiro (Float64): valor financeiro negociado
            (``quantidade * pu_medio``).

    Notes:
        O schema é estável para concatenação entre meses. Em layouts antigos
        da fonte que não trazem corretagem, ``operacoes_corretagem`` e
        ``quantidade_corretagem`` são retornadas como nulas.

    Examples:
        >>> df = yd.tpf.secundario.mensal("07-01-2025", extragrupo=True)
    """
    if any_is_empty(data):
        return pl.DataFrame()

    data_alvo = _data_mensal(data)
    hoje = relogio.hoje()
    if (data_alvo.year, data_alvo.month) > (hoje.year, hoje.month):
        return pl.DataFrame()

    return ler(baixar_zip(data_alvo, extragrupo))
