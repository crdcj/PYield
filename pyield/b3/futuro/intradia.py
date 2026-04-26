import polars as pl
import polars.selectors as cs

from pyield import du
from pyield.b3._validar_pregao import intradia_disponivel
from pyield.b3.derivativos_intradia import derivativo_intradia
from pyield.b3.futuro.contratos import CONTRATOS_TAXA, dv01_expr
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
    """Busca os dados intradia mais recentes de contratos futuros da B3.

    Fonte: dados intradia de derivativos da B3. Os dados possuem atraso
    aproximado de 15 minutos; a coluna ``horario_referencia`` reflete essa
    defasagem.

    Args:
        contrato: Contrato futuro negociado na B3 (ex.: ``DI1``, ``DAP``,
            ``DOL``, ``WDO``, ``IND``).

    Returns:
        DataFrame Polars com os dados intradia processados. Retorna DataFrame
        vazio fora do horário de pregão ou quando não houver dados.

    Output Columns:
        Colunas base:
            * data_referencia (Date): data de negociação.
            * horario_referencia (Datetime): horário de referência do dado.
            * codigo_negociacao (String): código de negociação na B3.
            * data_vencimento (Date): data de vencimento do contrato.
            * dias_uteis (Int64): dias úteis até o vencimento.
            * dias_corridos (Int64): dias corridos até o vencimento.
            * contratos_abertos (Int64): contratos em aberto.
            * numero_negocios (Int64): número de negócios.
            * volume_negociado (Int64): quantidade de contratos negociados.
            * volume_financeiro (Float64): volume financeiro bruto.

        Colunas de contratos cotados por preço:
            * preco_ajuste_anterior (Float64): preço de ajuste anterior.
            * preco_limite_minimo (Float64): limite mínimo de variação.
            * preco_limite_maximo (Float64): limite máximo de variação.
            * preco_abertura (Float64): preço de abertura.
            * preco_minimo (Float64): preço mínimo negociado.
            * preco_maximo (Float64): preço máximo negociado.
            * preco_medio (Float64): preço médio negociado.
            * preco_ultimo (Float64): último preço negociado.
            * preco_oferta_compra (Float64): melhor preço de compra.
            * preco_oferta_venda (Float64): melhor preço de venda.

        Colunas de contratos cotados por taxa:
            * taxa_ajuste_anterior (Float64): taxa de ajuste anterior.
            * taxa_limite_minimo (Float64): limite mínimo de variação.
            * taxa_limite_maximo (Float64): limite máximo de variação.
            * taxa_abertura (Float64): taxa de abertura.
            * taxa_minima (Float64): taxa mínima negociada.
            * taxa_maxima (Float64): taxa máxima negociada.
            * taxa_media (Float64): taxa média negociada.
            * taxa_oferta_compra (Float64): melhor taxa de compra.
            * taxa_oferta_venda (Float64): melhor taxa de venda.
            * taxa_ultima (Float64): última taxa negociada.

        Colunas específicas:
            * dv01 (Float64): variação no preço para 1bp de taxa, apenas DI1.
            * taxa_forward (Float64): taxa a termo, apenas DI1 e DAP.

    Notes:
        As colunas retornadas dependem do tipo do contrato. Contratos cotados
        por preço (ex.: DOL, WDO, IND, WIN) retornam as colunas ``preco_*``.
        Contratos cotados por taxa (ex.: DI1, DAP, DDI, FRC, FRO) retornam
        as colunas ``taxa_*``. Algumas colunas são específicas de famílias de
        contrato, como ``dv01`` para DI1 e ``taxa_forward`` para DI1 e DAP.

    Examples:
        >>> resultado = yd.futuro.intradia("DI1")
        >>> isinstance(resultado, pl.DataFrame)
        True
    """
    if not contrato:
        return pl.DataFrame()
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

    data_negociacao = du.ultimo_dia_util()
    df = df.with_columns(
        data_referencia=data_negociacao,
        dias_corridos=(pl.col("data_vencimento") - data_negociacao).dt.total_days(),
        dias_uteis=du.contar_expr(data_negociacao, "data_vencimento"),
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
            dv01=dv01_expr("dias_uteis", "taxa_ultima", "preco_ultimo")
        )

    return df.select(c for c in _ORDEM_COLUNAS if c in df.columns)
