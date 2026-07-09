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


def nome_arquivo_mensal(data: DateLike, extragrupo: bool = False) -> str:
    """Retorna o nome do arquivo ZIP mensal do secundário no BCB/SELIC.

    Args:
        data: Data de referência. Apenas ano e mês definem o arquivo.
        extragrupo: Se verdadeiro, retorna o nome do arquivo extragrupo.

    Returns:
        Nome do arquivo ZIP mensal publicado pelo BCB.

    Examples:
        >>> yd.tpf.secundario.nome_arquivo_mensal("07-06-2026")
        'NegT202606.ZIP'
        >>> yd.tpf.secundario.nome_arquivo_mensal("07-01-2025", extragrupo=True)
        'NegE202501.ZIP'
    """
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
    evitar que pipelines de ingestão salvem bronze vazio, corrompido ou sem CSV
    plausível.

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
    arquivo = nome_arquivo_mensal(data, extragrupo)
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
    )


def zip_para_silver(conteudo_zip: bytes) -> pl.DataFrame:
    """Converte o ZIP mensal bruto do secundário de TPFs em silver.

    Fonte: Banco Central do Brasil, sistema SELIC. Esta função representa a
    etapa bronze -> silver: extrai o CSV interno, limpa os campos, converte
    tipos e retorna o schema Polars canônico usado pela PYield. Ela não faz
    enriquecimento para camada ouro.

    Args:
        conteudo_zip: Bytes do arquivo ZIP bruto mensal.

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

    Notes:
        O schema silver é estável para concatenação entre meses. Em layouts
        antigos da fonte que não trazem corretagem, ``operacoes_corretagem`` e
        ``quantidade_corretagem`` são retornadas como nulas.

    Examples:
        >>> conteudo = yd.tpf.secundario.baixar_zip("07-01-2025")  # doctest: +SKIP
        >>> df = yd.tpf.secundario.zip_para_silver(conteudo)  # doctest: +SKIP
    """
    conteudo_csv = _extrair_csv_zip(conteudo_zip)
    return _processar_df_mensal(_parsear_csv_mensal(conteudo_csv))


def ler_zip(caminho: CaminhoArquivo) -> pl.DataFrame:
    """Lê um ZIP mensal local do secundário de TPFs e converte para silver.

    Fonte: Banco Central do Brasil, sistema SELIC. Esta função é um atalho para
    pipelines que salvam o bronze bruto e depois processam o arquivo local com o
    mesmo schema de ``zip_para_silver``.

    Args:
        caminho: Caminho do arquivo ZIP bruto.

    Returns:
        DataFrame Polars com dados mensais do mercado secundário.

    Examples:
        >>> df = yd.tpf.secundario.ler_zip("NegT202501.ZIP")  # doctest: +SKIP
    """
    return zip_para_silver(Path(caminho).read_bytes())


def mensal(data: DateLike, extragrupo: bool = False) -> pl.DataFrame:
    """Busca dados mensais do mercado secundário de TPFs.

    Fonte: Banco Central do Brasil, sistema SELIC. Baixa o ZIP mensal de
    negociações secundárias, valida o bronze bruto e retorna o DataFrame ouro.
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
        * financeiro (Float64): valor financeiro negociado.

    Notes:
        Esta é a camada ouro mensal: retorna o schema de ``zip_para_silver``
        acrescido de ``financeiro = quantidade * pu_medio``.

    Examples:
        >>> df = yd.tpf.secundario.mensal("07-01-2025", extragrupo=True)
    """
    if any_is_empty(data):
        return pl.DataFrame()

    data_alvo = _data_mensal(data)
    hoje = relogio.hoje()
    if (data_alvo.year, data_alvo.month) > (hoje.year, hoje.month):
        return pl.DataFrame()

    return zip_para_silver(baixar_zip(data_alvo, extragrupo)).with_columns(
        financeiro=(pl.col("quantidade") * pl.col("pu_medio")).round(2),
    )
