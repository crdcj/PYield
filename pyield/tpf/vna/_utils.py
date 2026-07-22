"""Operações compartilhadas pelos cálculos de VNA."""

import datetime as dt

import polars as pl

from pyield._internal.numbers import truncar

LIMITE_INFERIOR_PERCENTUAL = -100.0


def expressao_data() -> pl.Expr:
    """Converte a primeira coluna textual das planilhas em data."""
    return pl.col("column_1").str.to_datetime(strict=False).dt.date()


def calcular_vna(df: pl.DataFrame, data: dt.date) -> float:
    """Obtém o VNA publicado ou calcula o pró-rata entre pontos publicados."""
    ponto_exato = df.filter(pl.col("data") == data)
    if ponto_exato.height == 1:
        return float(ponto_exato.item(0, "vna"))

    ponto_inicial = df.filter(pl.col("data") < data).sort("data").tail(1)
    ponto_final = df.filter(pl.col("data") > data).sort("data").head(1)
    if ponto_inicial.is_empty() or ponto_final.is_empty():
        return float("nan")

    data_inicial = ponto_inicial.item(0, "data")
    data_final = ponto_final.item(0, "data")
    vna_inicial = float(ponto_inicial.item(0, "vna"))
    vna_final = float(ponto_final.item(0, "vna"))
    expoente = (data - data_inicial).days / (data_final - data_inicial).days
    variacao = vna_final / vna_inicial - 1
    return _aplicar_variacao_pro_rata(vna_inicial, variacao, expoente)


def _aplicar_variacao_pro_rata(
    vna_base: float,
    variacao: float,
    expoente: float,
) -> float:
    """Aplica uma variação exponencial e trunca o resultado em seis casas."""
    if vna_base <= 0:
        raise ValueError("O VNA-base deve ser positivo.")
    if variacao <= -1:
        raise ValueError("A variação deve ser maior que -100%.")

    valor = vna_base * (1 + variacao) ** expoente
    return truncar(valor, 6)


def calcular_vna_projetado(
    vna_base: float,
    projecao_percentual: float,
    expoente: float,
) -> float:
    """Aplica ao VNA projetado as precisões intermediárias definidas pela STN."""
    vna_base = truncar(vna_base, 6)
    projecao_percentual = round(projecao_percentual, 2)
    expoente = truncar(expoente, 14)
    return _aplicar_variacao_pro_rata(
        vna_base,
        projecao_percentual / 100,
        expoente,
    )
