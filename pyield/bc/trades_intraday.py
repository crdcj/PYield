"""
Busca dados intradiários de negociações secundárias da dívida pública federal.
https://www.bcb.gov.br/htms/selic/selicprecos.asp?frame=1
"""

import datetime as dt
import io
import logging

import polars as pl
import polars.selectors as cs
import requests

from pyield import bday, clock
from pyield.retry import retry_padrao

HORA_INICIO_TEMPO_REAL = dt.time(9, 0, 0)
HORA_FIM_TEMPO_REAL = dt.time(22, 0, 0)
URL_BASE_TEMPO_REAL = (
    "https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/"
)

registro = logging.getLogger(__name__)

MAPA_COLUNAS = {
    "//1": ("RowType", None),
    "código título": ("SelicCode", pl.Int64),
    "data vencimento": ("MaturityDate", None),
    "sigla": ("BondType", None),
    "mercado à vista pu último": ("LastPrice", pl.Float64),
    "tx último": ("LastRate", pl.Float64),
    "pu mínimo": ("MinPrice", pl.Float64),
    "tx mínimo": ("MinRate", pl.Float64),
    "pu médio": ("AvgPrice", pl.Float64),
    "tx médio": ("AvgRate", pl.Float64),
    "pu máximo": ("MaxPrice", pl.Float64),
    "tx máximo": ("MaxRate", pl.Float64),
    "totais liquidados operações": ("Trades", pl.Int64),
    "corretagem liquidados operações": ("BrokeredTrades", pl.Int64),
    "títulos": ("Quantity", pl.Int64),
    "corretagem títulos": ("BrokeredQuantity", pl.Int64),
    "financeiro": ("Value", pl.Float64),
    "mercado a termo pu último": ("FwdLastPrice", pl.Float64),
    "tx último_duplicated_0": ("FwdLastRate", pl.Float64),
    "pu mínimo_duplicated_0": ("FwdMinPrice", pl.Float64),
    "tx mínimo_duplicated_0": ("FwdMinRate", pl.Float64),
    "pu médio_duplicated_0": ("FwdAvgPrice", pl.Float64),
    "tx médio_duplicated_0": ("FwdAvgRate", pl.Float64),
    "pu máximo_duplicated_0": ("FwdMaxPrice", pl.Float64),
    "tx máximo_duplicated_0": ("FwdMaxRate", pl.Float64),
    "totais contratados operações": ("FwdTrades", pl.Int64),
    "corretagem contratados operações": ("FwdBrokeredTrades", pl.Int64),
    "títulos_duplicated_0": ("FwdQuantity", pl.Int64),
    "corretagem títulos_duplicated_0": ("FwdBrokeredQuantity", pl.Int64),
    "financeiro_duplicated_0": ("FwdValue", pl.Float64),
}

MAPEAMENTO_COL_API = {col: alias for col, (alias, _) in MAPA_COLUNAS.items()}
ESQUEMA_DADOS = {
    alias: dtype for _, (alias, dtype) in MAPA_COLUNAS.items() if dtype is not None
}

ORDEM_COLUNAS_FINAL = [
    "CollectedAt",
    "SettlementDate",
    "BondType",
    "SelicCode",
    "MaturityDate",
    "MinPrice",
    "AvgPrice",
    "MaxPrice",
    "LastPrice",
    "MinRate",
    "AvgRate",
    "MaxRate",
    "LastRate",
    "Trades",
    "Quantity",
    "Value",
    "BrokeredTrades",
    "BrokeredQuantity",
    "FwdMinPrice",
    "FwdAvgPrice",
    "FwdLastPrice",
    "FwdMaxPrice",
    "FwdLastRate",
    "FwdMinRate",
    "FwdAvgRate",
    "FwdMaxRate",
    "FwdTrades",
    "FwdQuantity",
    "FwdValue",
    "FwdBrokeredTrades",
    "FwdBrokeredQuantity",
]


@retry_padrao
def _buscar_csv() -> str:
    """
    Exemplo de URL do CSV com dados intradiários:
        https://www3.bcb.gov.br/novoselic/rest/precosNegociacao/pub/download/estatisticas/02-06-2025
    """
    hoje = clock.today()
    data_formatada = hoje.strftime("%d-%m-%Y")
    url = f"{URL_BASE_TEMPO_REAL}{data_formatada}"
    r = requests.get(url, timeout=30)  # API costuma levar ~10s
    r.raise_for_status()
    r.encoding = "utf-8-sig"  # Trata BOM em UTF-8
    return r.text


def _limpar_csv(texto: str) -> str:
    linhas = texto.splitlines()
    # Remove espaços nos nomes das colunas para bater com MAPA_COLUNAS
    cabecalho = ";".join(col.strip() for col in linhas[0].split(";"))
    linhas_validas = [cabecalho] + [linha for linha in linhas if linha.startswith("1;")]
    texto = "\n".join(linhas_validas)
    texto = texto.replace(".", "")  # Remove separador de milhar
    texto = texto.replace(",", ".")  # Troca vírgula decimal por ponto
    return texto


def _csv_para_df(texto: str) -> pl.DataFrame:
    return pl.read_csv(
        io.StringIO(texto),
        separator=";",
        null_values="-",
    )


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    agora = clock.now()
    hoje = agora.date()

    df = (
        df.rename(MAPEAMENTO_COL_API)
        .cast(ESQUEMA_DADOS, strict=False)  # type: ignore[call-arg]
        .drop("RowType", strict=False)
        .with_columns(
            pl.col("BondType").str.strip_chars(),
            pl.col("MaturityDate").str.to_date("%d/%m/%Y"),
            cs.contains("Rate").truediv(100).round(6),
            SettlementDate=hoje,
            CollectedAt=agora,
        )
    )

    # 3. Seleção final e reordenação
    colunas_finais = [col for col in ORDEM_COLUNAS_FINAL if col in df.columns]

    return df.select(colunas_finais)


def _mercado_selic_aberto() -> bool:
    """Verifica se o mercado SELIC está aberto no momento."""
    agora = clock.now()
    hoje = agora.date()
    hora = agora.time()
    eh_dia_util = bday.is_business_day(hoje)
    eh_horario = HORA_INICIO_TEMPO_REAL <= hora <= HORA_FIM_TEMPO_REAL

    return eh_dia_util and eh_horario


def tpf_intraday_trades() -> pl.DataFrame:
    """Obtém dados intradiários de negociações secundárias da dívida pública
    federal (TPF - títulos públicos federais) no Banco Central do Brasil (BCB).

    Os dados ficam disponíveis apenas durante o horário do SELIC
    (09:00–22:00 BRT) em dias úteis. Retorna DataFrame vazio fora desse período.

    Returns:
        pl.DataFrame: DataFrame com negociações intradiárias. Vazio se o mercado
            estiver fechado ou ocorrer erro.

    Output Columns:
        * CollectedAt (datetime): Timestamp da coleta (BRT).
        * SettlementDate (date): Data de liquidação à vista.
        * BondType (str): Sigla do título (ex.: LFT, LTN, NTN-B).
        * SelicCode (int): Código SELIC do título.
        * MaturityDate (date): Data de vencimento do título.
        * MinPrice (float): Menor preço negociado.
        * AvgPrice (float): Preço médio negociado.
        * MaxPrice (float): Maior preço negociado.
        * LastPrice (float): Último preço negociado.
        * MinRate (float): Menor taxa negociada (decimal).
        * AvgRate (float): Taxa média negociada (decimal).
        * MaxRate (float): Maior taxa negociada (decimal).
        * LastRate (float): Última taxa negociada (decimal).
        * Trades (int): Total de operações liquidadas.
        * Quantity (int): Quantidade total de títulos negociados.
        * Value (float): Valor financeiro total negociado (BRL).
        * BrokeredTrades (int): Operações liquidadas via corretagem.
        * BrokeredQuantity (int): Títulos negociados via corretagem.
        * FwdMinPrice (float): Menor preço a termo negociado.
        * FwdAvgPrice (float): Preço médio a termo negociado.
        * FwdMaxPrice (float): Maior preço a termo negociado.
        * FwdLastPrice (float): Último preço a termo negociado.
        * FwdMinRate (float): Menor taxa a termo negociada (decimal).
        * FwdAvgRate (float): Taxa média a termo negociada (decimal).
        * FwdMaxRate (float): Maior taxa a termo negociada (decimal).
        * FwdLastRate (float): Última taxa a termo negociada (decimal).
        * FwdTrades (int): Total de operações a termo contratadas.
        * FwdQuantity (int): Total de títulos a termo negociados.
        * FwdValue (float): Valor financeiro total a termo (BRL).
        * FwdBrokeredTrades (int): Operações a termo via corretagem.
        * FwdBrokeredQuantity (int): Títulos a termo via corretagem.

    Notes:
        - Retorna DataFrame vazio fora do horário do SELIC (09:00–22:00 BRT).
        - Em caso de erro na coleta, registra log e retorna DataFrame vazio.

    Examples:
        >>> from pyield import bc
        >>> df = bc.tpf_intraday_trades()
    """
    if not _mercado_selic_aberto():
        registro.info("Mercado fechado. Retornando DataFrame vazio.")
        return pl.DataFrame()

    try:
        texto_bruto = _buscar_csv()
        texto_limpo = _limpar_csv(texto_bruto)
        if not texto_limpo:
            registro.warning("Nenhum dado encontrado nas negociações intradiárias.")
            return pl.DataFrame()

        df = _csv_para_df(texto_limpo)
        df = _processar_df(df)

        valor = df["Value"].sum() / 10**9
        registro.info(f"Foram coletados {valor:,.1f} bilhões de BRL em negociações.")
        return df
    except Exception as e:
        registro.exception(
            f"Erro ao coletar dados do BCB: {e}. Retornando DataFrame vazio."
        )
        return pl.DataFrame()
