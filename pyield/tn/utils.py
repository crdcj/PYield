import datetime as dt
import logging
from collections.abc import Callable
from decimal import Decimal
from typing import overload

import polars as pl

import pyield._internal.converters as cv
from pyield._internal.types import DateLike
from pyield.anbima.mercado_secundario import TipoTPF

logger = logging.getLogger(__name__)

COLUNAS_DADOS_TPF = (
    "data_referencia",
    "titulo",
    "codigo_selic",
    "data_base",
    "data_vencimento",
    "pu",
    "taxa_compra",
    "taxa_venda",
    "taxa_indicativa",
)


def subtrair_meses(data: dt.date, meses: int) -> dt.date:
    """Subtrai `meses` meses de `data`, preservando o dia."""
    mes = data.month - meses
    ano = data.year + (mes - 1) // 12
    mes = (mes - 1) % 12 + 1
    return data.replace(year=ano, month=mes)


def obter_tpf(
    data_referencia: DateLike,
    tipo_titulo: TipoTPF,
) -> pl.DataFrame:
    """Busca taxas indicativas de TPF no padrão de colunas usado por ``tn``."""
    from pyield.anbima.mercado_secundario import tpf  # noqa: PLC0415

    return tpf(data_referencia, tipo_titulo).select(COLUNAS_DADOS_TPF)


def adicionar_taxa_di(df: pl.DataFrame, data_ref: dt.date) -> pl.DataFrame:
    """Adiciona a coluna `taxa_di` ao DataFrame pelo método flat forward."""
    from pyield import di1  # noqa: PLC0415

    taxas_di = di1.interpolar_taxas(
        datas_referencia=data_ref,
        datas_vencimento=df["data_vencimento"],
        extrapolar=True,
    )
    if taxas_di.is_empty():
        return df
    return df.with_columns(taxa_di=taxas_di)


def adicionar_duration(
    df: pl.DataFrame,
    funcao_duration: Callable[[dt.date, dt.date, float], float],
) -> pl.DataFrame:
    """Adiciona `duration` e `prazo_medio` ao DataFrame.

    Calcula a Macaulay Duration via ``funcao_duration`` (row-wise) e define
    `prazo_medio` igual a `duration`.
    """
    return df.with_columns(
        duration=pl.struct(
            "data_referencia", "data_vencimento", "taxa_indicativa"
        ).map_elements(
            lambda s: funcao_duration(
                s["data_referencia"], s["data_vencimento"], s["taxa_indicativa"]
            ),
            return_dtype=pl.Float64,
        ),
    ).with_columns(prazo_medio=pl.col("duration"))


def adicionar_dv01(df: pl.DataFrame, data_ref: dt.date) -> pl.DataFrame:
    """Adiciona `dv01` e `dv01_usd` ao DataFrame. Requer coluna `duration`."""
    from pyield.bc.sgs import ptax  # noqa: PLC0415

    expr_duracao_mod = pl.col("duration") / (1 + pl.col("taxa_indicativa"))
    df = df.with_columns(dv01=0.0001 * expr_duracao_mod * pl.col("pu"))

    try:
        taxa_ptax = ptax(data=data_ref)
        df = df.with_columns(dv01_usd=pl.col("dv01") / taxa_ptax)
    except Exception as e:
        logger.error("Erro ao adicionar DV01 em USD: %s", e)
    return df


@overload
def truncar(values: float | int | Decimal, decimals: int) -> float: ...
@overload
def truncar(values: pl.Series, decimals: int) -> pl.Series: ...


def truncar(
    values: float | int | Decimal | pl.Series, decimals: int
) -> float | pl.Series:
    """Trunca números (scalar ou ``polars.Series``) em direção a zero.

    Implementação unificada usando apenas operações de ``polars``: escalares
    são embrulhados em uma série temporária e depois desembrulhados.

    Args:
        values: Escalar (float/int/Decimal) ou ``pl.Series``.
        decimals: Casas decimais (>= 0).

    Returns:
        Float se entrada era escalar ou ``pl.Series`` se entrada era série.

    Examples:
        >>> truncar(3.14159, 3)
        3.141
        >>> truncar(pl.Series([3.14159, 2.71828]), 3)
        shape: (2,)
        Series: '' [f64]
        [
           3.141
           2.718
        ]
    """
    if decimals < 0:
        raise ValueError("decimals must be non-negative")

    factor = 10.0**decimals

    if isinstance(values, pl.Series):
        return (values * factor).cast(pl.Int64).cast(pl.Float64) / factor
    elif isinstance(values, (float, int, Decimal)):
        return int(float(values) * factor) / factor
    else:
        raise TypeError("values must be a float, int, Decimal or pl.Series")


def calcular_pv(
    fluxos_caixa: pl.Series | list[float],
    taxas: pl.Series | list[float],
    prazos: pl.Series | list[float],
) -> float:
    """Calcula o valor presente de uma série de fluxos de caixa de forma estrita.

    O valor presente é calculado descontando cada fluxo de caixa pela taxa
    correspondente e período, usando a fórmula: VP = CF / (1 + r)^t

    Args:
        fluxos_caixa: Fluxos de caixa a descontar.
        taxas: Taxas de desconto para cada fluxo (em decimal, ex: 0.10 para 10%).
        prazos: Prazos (em anos) para cada fluxo de caixa.

    Returns:
        Soma dos valores presentes. Retorna ``float('nan')`` se:
        - As listas/séries de entrada estiverem vazias.
        - As listas/séries de entrada tiverem tamanhos diferentes.
        - Qualquer valor de entrada ou resultado do cálculo for null, NaN ou inf.

    Examples:
        Título com cupons anuais de 10% e principal de R$1000, descontado a 8% a.a.:
        >>> fluxos_caixa = [100, 100, 1100]  # Cupons de R$100 + principal no vencimento
        >>> taxas = [0.08, 0.08, 0.08]  # Taxa de desconto de 8% a.a.
        >>> prazos = [1.0, 2.0, 3.0]  # Pagamentos anuais
        >>> round(calcular_pv(fluxos_caixa, taxas, prazos), 2)
        1051.54

        Retorna NaN para entradas vazias:
        >>> import math
        >>> math.isnan(calcular_pv([], [], []))
        True

        Retorna NaN para tamanhos incompatíveis:
        >>> math.isnan(calcular_pv([100], [0.10, 0.10], [1.0]))
        True
    """
    try:
        # A criação do DataFrame agora pode levantar um ShapeError
        df = pl.DataFrame(
            {
                "fluxos_caixa": fluxos_caixa,
                "taxas": taxas,
                "prazos": prazos,
            },
            schema={
                "fluxos_caixa": pl.Float64,
                "taxas": pl.Float64,
                "prazos": pl.Float64,
            },
        )
    except pl.exceptions.ShapeError:
        return float("nan")

    if df.is_empty():
        return float("nan")

    valores_presentes = df["fluxos_caixa"] / (1 + df["taxas"]) ** df["prazos"]

    if valores_presentes.has_nulls():
        return float("nan")

    return float(valores_presentes.sum())


def _encontrar_intervalo_raiz(
    func: Callable[[float], float],
) -> tuple[float, float] | None:
    """
    Encontra um intervalo [a, b] para a TAXA DE JUROS que zera a função.

    Otimizado para o contexto financeiro, buscando a taxa apenas em um
    intervalo realista. A função 'func' é a que calcula a diferença de
    preço dado uma taxa.
    """
    # --- LIMITES DE BOM SENSO PARA A *TAXA* QUE ESTAMOS PROCURANDO ---
    taxa_inicial: float = 0.01
    passo: float = 0.01
    fator_crescimento: float = 1.6
    max_tentativas: int = 100

    taxa_min: float = -1.0
    taxa_max: float = 10.00
    # -----------------------------------------------------------------

    f0 = func(taxa_inicial)
    if abs(f0) == 0:
        return (taxa_inicial, taxa_inicial)

    # 1. Busca na direção positiva
    a, fa = taxa_inicial, f0
    b = taxa_inicial + passo
    passo_atual = passo

    for _ in range(max_tentativas):
        if b > taxa_max:
            break
        fb = func(b)
        if fa * fb < 0:
            return (a, b)
        a, fa = b, fb
        passo_atual *= fator_crescimento
        b += passo_atual

    # 2. Busca na direção negativa
    a, fa = taxa_inicial, f0
    b = taxa_inicial - passo
    passo_atual = passo

    for _ in range(max_tentativas):
        if b < taxa_min:
            break
        fb = func(b)
        if fa * fb < 0:
            return (b, a)
        a, fa = b, fb
        passo_atual *= fator_crescimento
        b -= passo_atual

    return None


def _metodo_bissecao(func: Callable[[float], float], a: float, b: float) -> float:
    """Método da bisseção para encontrar raiz."""
    tolerancia = 1e-8
    max_iter = 100
    fa, fb = func(a), func(b)
    if fa * fb > 0:
        logger.warning(
            "Falha no método da bisseção: a função não muda de sinal no intervalo."
        )
        return float("nan")

    for _ in range(max_iter):
        ponto_medio = (a + b) / 2
        fmeio = func(ponto_medio)
        if abs(fmeio) < tolerancia or (b - a) / 2 < tolerancia:
            return ponto_medio
        if fmeio * fa < 0:
            b, fb = ponto_medio, fmeio
        else:
            a, fa = ponto_medio, fmeio

    return (a + b) / 2


def encontrar_raiz(func_diferenca_preco: Callable[[float], float]) -> float:
    """Encontra a raiz de uma função de diferença de preço.

    Versão robusta que encontra automaticamente um intervalo válido e
    aplica o método da bisseção.
    """
    intervalo = _encontrar_intervalo_raiz(func_diferenca_preco)

    if intervalo is None:
        logger.warning("Não foi possível encontrar intervalo de busca válido")
        return float("nan")

    a, b = intervalo
    return _metodo_bissecao(func_diferenca_preco, a, b)


def premio_pre(
    data: DateLike,
    pontos_base: bool = False,
) -> pl.DataFrame:
    """Calcula o prêmio dos títulos prefixados (LTN e NTN-F) sobre o DI.

    Em linguagem de mercado, esse valor é chamado de prêmio. Em termos
    descritivos, trata-se do spread sobre o DI.

    Definição do prêmio:
        premio = taxa indicativa do PRE - taxa de ajuste do DI

    Quando ``pontos_base=False`` a coluna retorna essa diferença em formato
    decimal (ex: 0.000439 ≈ 4.39 bps). Quando ``pontos_base=True`` o valor
    é automaticamente multiplicado por 10_000 e exibido diretamente em
    basis points.

    Args:
        data: Data da consulta para buscar as taxas.
        pontos_base: Se True, retorna o prêmio já convertido em basis
            points. Padrão False.

    Returns:
        pl.DataFrame: DataFrame com as colunas do prêmio.

    Output Columns:
        - titulo (String): Tipo do título.
        - data_vencimento (Date): Data de vencimento.
        - premio (Float64): prêmio em decimal ou bps conforme parâmetro
            (spread sobre o DI).

    Examples:
        >>> from pyield.tn import utils
        >>> utils.premio_pre("30-05-2025", pontos_base=True)
        shape: (18, 3)
        ┌────────┬─────────────────┬────────┐
        │ titulo ┆ data_vencimento ┆ premio │
        │ ---    ┆ ---             ┆ ---    │
        │ str    ┆ date            ┆ f64    │
        ╞════════╪═════════════════╪════════╡
        │ LTN    ┆ 2025-07-01      ┆ 4.39   │
        │ LTN    ┆ 2025-10-01      ┆ -9.0   │
        │ LTN    ┆ 2026-01-01      ┆ -4.88  │
        │ LTN    ┆ 2026-04-01      ┆ -4.45  │
        │ LTN    ┆ 2026-07-01      ┆ 0.81   │
        │ …      ┆ …               ┆ …      │
        │ NTN-F  ┆ 2027-01-01      ┆ -3.31  │
        │ NTN-F  ┆ 2029-01-01      ┆ 14.21  │
        │ NTN-F  ┆ 2031-01-01      ┆ 21.61  │
        │ NTN-F  ┆ 2033-01-01      ┆ 11.51  │
        │ NTN-F  ┆ 2035-01-01      ┆ 22.0   │
        └────────┴─────────────────┴────────┘
    """
    df = obter_tpf(data, "PRE").select("titulo", "data_vencimento", "taxa_indicativa")
    if df.is_empty():
        return df.select(
            pl.lit("").alias("titulo"),
            pl.lit(None, dtype=pl.Date).alias("data_vencimento"),
            pl.lit(None, dtype=pl.Float64).alias("premio"),
        ).clear()
    data_ref = cv.converter_datas(data)
    df = adicionar_taxa_di(df, data_ref)
    df = (
        df.with_columns(premio=pl.col("taxa_indicativa") - pl.col("taxa_di"))
        .select("titulo", "data_vencimento", "premio")
        .sort("titulo", "data_vencimento")
    )

    if pontos_base:
        df = df.with_columns(pl.col("premio") * 10_000)

    return df
