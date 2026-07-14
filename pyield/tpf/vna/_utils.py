"""Operações compartilhadas pelos cálculos de VNA."""

import datetime as dt
import math

import polars as pl

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
    return calcular_pro_rata(vna_inicial, variacao, expoente)


def calcular_pro_rata(vna_base: float, variacao: float, expoente: float) -> float:
    """Aplica a projeção exponencial e trunca o resultado em seis casas."""
    if vna_base <= 0:
        raise ValueError("O VNA-base deve ser positivo.")
    if variacao <= -1:
        raise ValueError("A variação deve ser maior que -100%.")

    valor = vna_base * (1 + variacao) ** expoente
    return math.trunc(valor * 1_000_000) / 1_000_000
