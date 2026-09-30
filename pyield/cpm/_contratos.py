"""Contratos CPM: opções digitais da B3 sobre a decisão do COPOM.

Formato do código de negociação: ``CPM{mes}{ano2}{C|P}{strike6}``, por exemplo
``CPMZ25C099500``: mês ``Z`` (dezembro), ano 2025, call, strike 99,500. A
variação da Selic em pontos-base é ``(strike - 100) * 100`` (aqui, -50 bps).

O CPM expira no primeiro dia útil após o fim da reunião do COPOM, e não no
primeiro dia útil do mês. Por isso, as datas da reunião e de expiração vêm do
calendário do COPOM, unido pelo mês e ano do código. A junção é ``left`` para
não descartar contratos de reuniões ainda ausentes do calendário.
"""

import datetime as dt

import polars as pl
import requests

from pyield import copom, du
from pyield._internal.converters import converter_datas
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike
from pyield.b3 import boletim
from pyield.futuro.contratos import _MAPA_MESES

# Mapeamento mínimo para consumo do módulo CPM a partir do schema XML bruto.
_RENOMEAR_COLUNAS_CPM = {
    "TckrSymb": "codigo_negociacao",
}


def _df_vazio() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "data_referencia": pl.Date,
            "codigo_negociacao": pl.String,
            "data_fim_reuniao": pl.Date,
            "data_expiracao": pl.Date,
            "tipo_opcao": pl.String,
            "variacao_strike_bps": pl.Int32,
            "preco_ajuste": pl.Float64,
            "dias_uteis": pl.Int32,
        }
    )


_CSV_ESQUEMA = {"Instrumento financeiro": pl.String, "Preço de referência": pl.Float64}


@retry_padrao
def _buscar_csv(data: dt.date) -> bytes:
    """Busca o CSV diário de derivativos consolidados na B3."""
    url = "https://arquivos.b3.com.br/bdi/table/export/csv"
    parametros = {"lang": "pt-BR"}
    data_str = data.strftime("%Y-%m-%d")
    carga = {
        "Name": "ConsolidatedTradesDerivatives",
        "Date": data_str,
        "FinalDate": data_str,
        "ClientId": "",
        "Filters": {},
    }
    cabecalhos = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa: E501
        "Accept": "application/json, text/plain, */*",
    }
    resposta = requests.post(
        url, params=parametros, json=carga, headers=cabecalhos, timeout=(5, 30)
    )
    resposta.raise_for_status()
    return resposta.content


def _parsear_precos_referencia(csv_bytes: bytes) -> pl.DataFrame:
    """Extrai o Preço de Referência dos contratos CPM do CSV da B3."""
    try:
        df = pl.read_csv(
            csv_bytes.replace(b".", b""),
            separator=";",
            skip_lines=2,
            null_values=["-"],
            decimal_comma=True,
            schema_overrides=_CSV_ESQUEMA,
            encoding="utf-8-sig",
        )
    except pl.exceptions.NoDataError:
        # Para datas antigas, a B3 devolve apenas o cabeçalho, sem preâmbulo.
        return pl.DataFrame(
            schema={"codigo_negociacao": pl.String, "preco_ajuste": pl.Float64}
        )

    return df.select(
        codigo_negociacao="Instrumento financeiro",
        preco_ajuste="Preço de referência",
    ).filter(pl.col("codigo_negociacao").str.starts_with("CPM"))


def contratos(data: DateLike) -> pl.DataFrame:
    """Busca os contratos CPM negociados em uma data.

    Fonte: B3. A lista de contratos vem do boletim de preços (SPR) e o preço de
    ajuste vem do "Preço de Referência" do boletim de derivativos consolidados,
    o mesmo exibido no painel "Probabilidades da Taxa Selic Meta" da B3. As
    datas da reunião e de expiração vêm do calendário do COPOM.

    Args:
        data: Data de negociação.

    Returns:
        DataFrame Polars ordenado por ``data_expiracao`` e
        ``variacao_strike_bps``. Retorna DataFrame vazio, com o schema abaixo,
        se não houver contratos CPM na data.

    Output Columns:
        * data_referencia (Date): data de negociação.
        * codigo_negociacao (String): código do contrato (ex.: ``CPMZ25C099500``).
        * data_fim_reuniao (Date): último dia da reunião do COPOM.
        * data_expiracao (Date): primeiro dia útil após ``data_fim_reuniao``,
            data de liquidação do contrato.
        * tipo_opcao (String): ``"call"`` ou ``"put"``.
        * variacao_strike_bps (Int32): variação da Selic Meta, em pontos-base,
            associada ao strike.
        * preco_ajuste (Float64): Preço de Referência da B3, em pontos (0–100).
        * dias_uteis (Int32): dias úteis de ``data_referencia`` até
            ``data_expiracao``.

    Notes:
        A B3 só publica o Preço de Referência de datas recentes (cerca de um
        mês). Para datas mais antigas, ``preco_ajuste`` é nulo.

    Examples:
        >>> df = yd.cpm.contratos("29-01-2025")  # doctest: +SKIP
    """
    data_negociacao = converter_datas(data)
    df = boletim.buscar(data_negociacao, prefixo_ticker="CPM")
    if df.is_empty():
        return _df_vazio()

    df = df.rename(_RENOMEAR_COLUNAS_CPM, strict=False).with_columns(
        data_referencia=data_negociacao,
        tipo_opcao=pl.col("codigo_negociacao")
        .str.slice(6, 1)
        .replace({"C": "call", "P": "put"}),
        variacao_strike_bps=(
            pl.col("codigo_negociacao")
            .str.slice(7, 6)
            .cast(pl.Int64, strict=False)
            .floordiv(10)
            .sub(10_000)
            .cast(pl.Int32)
        ),
        _mes_reuniao=pl.col("codigo_negociacao")
        .str.slice(3, 1)
        .replace_strict(_MAPA_MESES, default=None, return_dtype=pl.Int32),
        _ano_reuniao=(
            pl.col("codigo_negociacao")
            .str.slice(4, 2)
            .cast(pl.Int32, strict=False)
            .add(2000)
        ),
    )

    cal = copom.calendario().select(
        _mes_reuniao=pl.col("data_decisao").dt.month().cast(pl.Int32),
        _ano_reuniao=pl.col("data_decisao").dt.year().cast(pl.Int32),
        data_fim_reuniao=pl.col("data_decisao"),
        data_expiracao=pl.col("data_efetividade"),
    )

    df = df.join(cal, on=["_mes_reuniao", "_ano_reuniao"], how="left").drop(
        "_mes_reuniao", "_ano_reuniao"
    )

    # O SPR não traz o Preço de Referência das opções; ele vem do CSV consolidado.
    precos = _parsear_precos_referencia(_buscar_csv(data_negociacao))

    return (
        df.join(precos, on="codigo_negociacao", how="left")
        .with_columns(
            dias_uteis=du.contar_expr("data_referencia", "data_expiracao").cast(
                pl.Int32
            )
        )
        .select(_df_vazio().columns)
        .sort("data_expiracao", "variacao_strike_bps")
    )
