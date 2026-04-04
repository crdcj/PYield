import polars as pl
import polars.selectors as cs

from pyield import dus
from pyield.b3._validar_pregao import intradia_disponivel
from pyield.b3.derivativos_intradia import derivativo_intradia
from pyield.b3.futuro.contratos import CONTRATOS_TAXA, expr_dv01
from pyield.fwd import forwards

# Renomeação preco_* → taxa_* para contratos cotados por taxa.
_PRECO_PARA_TAXA_INTRADIA = {
    "preco_ajuste_anterior": "taxa_ajuste_anterior",
    "preco_limite_minimo": "taxa_limite_minimo",
    "preco_limite_maximo": "taxa_limite_maximo",
    "preco_abertura": "taxa_abertura",
    "preco_minimo": "taxa_minima",
    "preco_maximo": "taxa_maxima",
    "preco_medio": "taxa_media",
    "preco_ultimo": "taxa_ultima",
    "preco_oferta_compra": "taxa_oferta_compra",
    "preco_oferta_venda": "taxa_oferta_venda",
}

# Ordem preferida de colunas na saída. Colunas preco_* e taxa_* são
# mutuamente exclusivas — o rename no preprocessamento garante isso.
_ORDEM_COLUNAS = (
    "data_referencia",
    "horario_referencia",
    "codigo_negociacao",
    "data_vencimento",
    "dias_uteis",
    "dias_corridos",
    "contratos_abertos",
    "numero_negocios",
    "volume_negociado",
    "volume_financeiro",
    "dv01",
    "preco_ajuste_anterior",
    "preco_limite_minimo",
    "preco_limite_maximo",
    "preco_abertura",
    "preco_minimo",
    "preco_maximo",
    "preco_medio",
    "preco_ultimo",
    "preco_oferta_compra",
    "preco_oferta_venda",
    "taxa_forward",
    "taxa_ajuste_anterior",
    "taxa_limite_minimo",
    "taxa_limite_maximo",
    "taxa_abertura",
    "taxa_minima",
    "taxa_maxima",
    "taxa_media",
    "taxa_oferta_compra",
    "taxa_oferta_venda",
    "taxa_ultima",
)


def intradia(contrato: str) -> pl.DataFrame:
    """Busca os dados intradia mais recentes da B3.

    Os dados intradia da fonte possuem atraso aproximado de 15 minutos.
    A coluna ``horario_referencia`` reflete essa defasagem.

    Args:
        contrato: Contrato futuro na B3.

    Returns:
        DataFrame Polars com dados intradia processados.

    Notes:
        As colunas com prefixo ``preco_`` aparecem para contratos cotados
        por preço (ex.: DOL, IND). As com prefixo ``taxa_`` aparecem para
        contratos cotados por taxa (ex.: DI1, DAP, DDI, FRC, FRO).
    """
    if not intradia_disponivel():
        return pl.DataFrame()

    df = derivativo_intradia(contrato)
    if df.is_empty():
        return pl.DataFrame()

    return _processar_intradia(df, contrato)


def _processar_intradia(df: pl.DataFrame, contrato: str) -> pl.DataFrame:
    df = df.filter(pl.col("codigo_mercado") == "FUT")
    if contrato in CONTRATOS_TAXA:
        df = df.rename(_PRECO_PARA_TAXA_INTRADIA, strict=False)
    df = df.drop_nulls("data_vencimento").sort("data_vencimento")

    data_negociacao = dus.ultimo_dia_util()
    df = df.with_columns(
        data_referencia=data_negociacao,
        dias_corridos=(pl.col("data_vencimento") - data_negociacao).dt.total_days(),
        dias_uteis=dus.contar_expr(data_negociacao, "data_vencimento"),
    ).filter(pl.col("dias_corridos") > 0)

    if contrato in CONTRATOS_TAXA:
        df = df.with_columns(cs.starts_with("taxa_").truediv(100).round(6))

    if contrato in {"DI1", "DAP"}:
        taxa_fwd = forwards(dias_uteis=df["dias_uteis"], taxas=df["taxa_ultima"])
        anos_uteis = pl.col("dias_uteis") / 252
        preco_ultimo = 100_000 / ((1 + pl.col("taxa_ultima")) ** anos_uteis)
        df = df.with_columns(preco_ultimo=preco_ultimo.round(2), taxa_forward=taxa_fwd)

    if contrato == "DI1":
        df = df.with_columns(
            dv01=expr_dv01("dias_uteis", "taxa_ultima", "preco_ultimo")
        )

    return df.select(c for c in _ORDEM_COLUNAS if c in df.columns)
