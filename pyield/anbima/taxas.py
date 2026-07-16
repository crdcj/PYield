"""Taxas de Títulos Públicos Federais (TPF) da ANBIMA.

Fonte:
    https://www.anbima.com.br/pt_br/informar/taxas-de-titulos-publicos.htm

Exemplo de URL:
    https://www.anbima.com.br/informacoes/merc-sec/arqs/ms240614.txt

Exemplo de dado bruto (CSV separado por @, encoding latin1):
    ANBIMA - Associação Brasileira das Entidades dos Mercados Financeiro e de Capitais

    Titulo@Data Referencia@Codigo SELIC@Data Base/Emissao@Data Vencimento@Tx. Compra@Tx. Venda@Tx. Indicativas@PU@Desvio padrao@Interv. Ind. Inf. (D0)@Interv. Ind. Sup. (D0)@Interv. Ind. Inf. (D+1)@Interv. Ind. Sup. (D+1)@Criterio
    LTN@20250924@100000@20230707@20251001@14,9483@14,9263@14,9375@997,241543@0,00433039162894@14,7341@15,2612@14,7316@15,2689@Calculado
    LTN@20250924@100000@20200206@20260101@14,7741@14,7485@14,7616@963,001853@0,00729826731971@14,7008@14,9986@14,7021@14,9975@Calculado
"""  # noqa: E501

import datetime as dt
import io
import logging
import os
import socket
import zipfile as zf
from pathlib import Path

import polars as pl
import requests

from pyield import du
from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.converters import converter_datas, data_referencia_valida
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike

type _CaminhoArquivo = str | os.PathLike[str]

ANBIMA_URL = "https://www.anbima.com.br/informacoes/merc-sec/arqs"
ANBIMA_RTM_HOSTNAME = "www.anbima.associados.rtm"
ANBIMA_RTM_URL = f"http://{ANBIMA_RTM_HOSTNAME}/merc_sec/arqs"

# Antes de 13/05/2014 o arquivo era zipado e o endpoint terminava com ".exe"
DATA_MUDANCA_FORMATO = dt.date(2014, 5, 13)

DIAS_RETENCAO_PUBLICA = 5

logger = logging.getLogger(__name__)


def _montar_nome_arquivo(data: dt.date) -> str:
    data_url = data.strftime("%y%m%d")
    if data < DATA_MUDANCA_FORMATO:
        nome_arquivo = f"ms{data_url}.exe"
    else:
        nome_arquivo = f"ms{data_url}.txt"
    return nome_arquivo


def _montar_url_arquivo(data: dt.date) -> str:
    ultimo_dia_util = du.ultimo_dia_util()
    qtd_dias_uteis = du.contar(data, ultimo_dia_util)
    if qtd_dias_uteis > DIAS_RETENCAO_PUBLICA:
        # Para datas com mais de 5 dias úteis, apenas os dados da RTM estão disponíveis
        logger.info("Tentando buscar dados RTM para %s", data.strftime("%d/%m/%Y"))
        url_arquivo = f"{ANBIMA_RTM_URL}/{_montar_nome_arquivo(data)}"
    else:
        url_arquivo = f"{ANBIMA_URL}/{_montar_nome_arquivo(data)}"
    return url_arquivo


@retry_padrao
def _obter_csv(data: dt.date) -> bytes:
    url_arquivo = _montar_url_arquivo(data)
    resposta = requests.get(url_arquivo, timeout=10)
    resposta.raise_for_status()
    return resposta.content


def baixar_arquivo(data: DateLike) -> bytes:
    """Baixa o arquivo bruto de taxas de TPF publicado pela ANBIMA.

    Args:
        data: Data de referência do arquivo.

    Returns:
        Bytes do arquivo publicado pela ANBIMA, sem decodificação ou parsing.

    Raises:
        requests.HTTPError: Se o arquivo não estiver disponível ou a resposta
            HTTP indicar erro.
        ValueError: Se ``data`` não for uma data escalar válida.
    """
    if isinstance(data, str) and not data.strip():
        msg = "data deve ser escalar para baixar um arquivo da ANBIMA"
        raise ValueError(msg)
    data_arquivo = converter_datas(data)
    return _obter_csv(data_arquivo)


def _parsear_df(csv_bytes: bytes) -> pl.DataFrame:
    """Converte bytes brutos do CSV da ANBIMA em DataFrame (tudo string)."""
    return pl.read_csv(
        source=csv_bytes,
        skip_lines=2,
        separator="@",
        null_values=["--"],
        infer_schema=False,
        encoding="latin1",
    )


def ler(fonte: bytes | _CaminhoArquivo) -> pl.DataFrame:
    """Lê taxas de TPF da ANBIMA a partir de bytes ou arquivo local.

    Fonte: arquivo de taxas de títulos públicos da ANBIMA.

    Arquivos atuais ``.txt`` são lidos diretamente. Arquivos históricos
    ``.exe`` são tratados como ZIPs e o arquivo interno é lido.

    Args:
        fonte: Bytes do arquivo bruto ou caminho do arquivo salvo localmente.

    Returns:
        DataFrame Polars com todas as colunas processadas da fonte.

    Output Columns:
        * titulo (String): tipo do título público.
        * data_referencia (Date): data de referência dos dados.
        * codigo_selic (Int64): código do título no SELIC.
        * data_base (Date): data base ou de emissão do título.
        * data_vencimento (Date): data de vencimento do título.
        * taxa_compra (Float64): taxa de compra em D0.
        * taxa_venda (Float64): taxa de venda em D0.
        * taxa_indicativa (Float64): taxa indicativa em D0.
        * pu (Float64): preço unitário para liquidação em D0.
        * desvio_padrao (Float64): desvio padrão das taxas observadas.
        * taxa_intervalo_inf_d0 (Float64): limite inferior indicativo em D0.
        * taxa_intervalo_sup_d0 (Float64): limite superior indicativo em D0.
        * taxa_intervalo_inf_d1 (Float64): limite inferior indicativo em D+1.
        * taxa_intervalo_sup_d1 (Float64): limite superior indicativo em D+1.
        * criterio (String): critério usado pela ANBIMA para o título.
    """
    conteudo = fonte if isinstance(fonte, bytes) else Path(fonte).read_bytes()
    if zf.is_zipfile(io.BytesIO(conteudo)):
        with zf.ZipFile(io.BytesIO(conteudo)) as arquivo_zip:
            conteudo = arquivo_zip.read(arquivo_zip.namelist()[0])
    return _processar_df(_parsear_df(conteudo))


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Renomeia, converte tipos e define a ordem das colunas."""
    return df.select(
        titulo=pl.col("Titulo"),
        data_referencia=pl.col("Data Referencia").str.to_date("%Y%m%d"),
        codigo_selic=pl.col("Codigo SELIC").cast(pl.Int64),
        data_base=pl.col("Data Base/Emissao").str.to_date("%Y%m%d"),
        data_vencimento=pl.col("Data Vencimento").str.to_date("%Y%m%d"),
        taxa_compra=taxa_br("Tx. Compra"),
        taxa_venda=taxa_br("Tx. Venda"),
        taxa_indicativa=taxa_br("Tx. Indicativas"),
        pu=float_br("PU"),
        desvio_padrao=float_br("Desvio padrao"),
        taxa_intervalo_inf_d0=taxa_br("Interv. Ind. Inf. (D0)"),
        taxa_intervalo_sup_d0=taxa_br("Interv. Ind. Sup. (D0)"),
        taxa_intervalo_inf_d1=taxa_br("Interv. Ind. Inf. (D+1)"),
        taxa_intervalo_sup_d1=taxa_br("Interv. Ind. Sup. (D+1)"),
        criterio=pl.col("Criterio"),
    )


def buscar(data: DateLike) -> pl.DataFrame:
    """Busca e processa taxas de TPF diretamente na ANBIMA.

    Fonte: arquivo de taxas de títulos públicos da ANBIMA.

    Args:
        data: Data de referência do arquivo.

    Returns:
        DataFrame com todas as colunas processadas da fonte. Retorna DataFrame
        vazio para datas válidas sem dados disponíveis.

    Output Columns:
        * titulo (String): tipo do título público.
        * data_referencia (Date): data de referência dos dados.
        * codigo_selic (Int64): código do título no SELIC.
        * data_base (Date): data base ou de emissão do título.
        * data_vencimento (Date): data de vencimento do título.
        * taxa_compra (Float64): taxa de compra em D0.
        * taxa_venda (Float64): taxa de venda em D0.
        * taxa_indicativa (Float64): taxa indicativa em D0.
        * pu (Float64): preço unitário para liquidação em D0.
        * desvio_padrao (Float64): desvio padrão das taxas observadas.
        * taxa_intervalo_inf_d0 (Float64): limite inferior indicativo em D0.
        * taxa_intervalo_sup_d0 (Float64): limite superior indicativo em D0.
        * taxa_intervalo_inf_d1 (Float64): limite inferior indicativo em D+1.
        * taxa_intervalo_sup_d1 (Float64): limite superior indicativo em D+1.
        * criterio (String): critério usado pela ANBIMA para o título.

    Raises:
        ValueError: Se ``data`` não for uma data escalar válida.
    """
    data = converter_datas(data)
    if not data_referencia_valida(data):
        return pl.DataFrame()

    url_arquivo = _montar_url_arquivo(data)

    # Fail-fast: se a URL é RTM e o host não resolve, não adianta tentar
    if ANBIMA_RTM_URL in url_arquivo:
        try:
            socket.gethostbyname(ANBIMA_RTM_HOSTNAME)
        except socket.gaierror:
            data_str = data.strftime("%d/%m/%Y")
            logger.warning(
                f"Não foi possível resolver o host da RTM para {data_str}. "
                "Dados históricos exigem acesso à rede RTM."
            )
            return pl.DataFrame()

    csv_bytes = _obter_csv(data)
    if not csv_bytes.strip():
        return pl.DataFrame()

    return ler(csv_bytes)
