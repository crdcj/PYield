import datetime as dt
import math
import re
from collections.abc import Callable
from decimal import Decimal, InvalidOperation

import polars as pl

import pyield._internal.converters as conversores
from pyield._internal.numbers import truncar
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf._taxas import TipoTPF

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


def obter_tpf(
    data_referencia: DateLike,
    tipo_titulo: TipoTPF,
) -> pl.DataFrame:
    """Busca taxas indicativas de TPF no padrão de colunas usado por ``tn``."""
    from pyield.tpf._taxas import taxas  # noqa: PLC0415

    return taxas(data_referencia, tipo_titulo).select(COLUNAS_DADOS_TPF)


def adicionar_taxa_di(df: pl.DataFrame, data_ref: DateLike) -> pl.DataFrame:
    """Adiciona a coluna `taxa_di` ao DataFrame pelo método flat forward."""
    from pyield.futuro import di1  # noqa: PLC0415

    taxas_di = di1.interpolar_taxas(
        datas_referencia=data_ref,
        datas_vencimento=df["data_vencimento"],
        extrapolar=True,
    )
    if taxas_di.is_empty():
        return df
    return df.with_columns(taxa_di=taxas_di)


def coluna_ou_expr(valor: pl.Expr | str, nome: str) -> pl.Expr:
    """Normaliza nome de coluna ou expressão Polars para uso em ``pl.struct``."""
    if isinstance(valor, str):
        return pl.col(valor).alias(nome)
    return valor.alias(nome)


def subtrair_meses(data: dt.date, meses: int) -> dt.date:
    """Subtrai `meses` meses de `data`, preservando o dia."""
    mes = data.month - meses
    ano = data.year + (mes - 1) // 12
    mes = (mes - 1) % 12 + 1
    return data.replace(year=ano, month=mes)


def gerar_datas_pagamento(
    inicio: DateLike,
    fim: DateLike,
    intervalo_meses: int = 6,
) -> pl.Series:
    """Gera datas contratuais entre o início exclusivo e o fim inclusivo."""
    if intervalo_meses <= 0:
        raise ValueError("O intervalo em meses deve ser maior que zero.")

    if any_is_empty(inicio, fim):
        return pl.Series(name="datas_pagamento", dtype=pl.Date)

    data_inicio = conversores.converter_datas(inicio)
    data_pagamento = conversores.converter_datas(fim)
    datas_pagamento = []

    while data_pagamento > data_inicio:
        datas_pagamento.append(data_pagamento)
        data_pagamento = subtrair_meses(data_pagamento, intervalo_meses)

    return pl.Series(
        name="datas_pagamento",
        values=datas_pagamento,
        dtype=pl.Date,
    ).sort()


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


def adicionar_dv01(df: pl.DataFrame) -> pl.DataFrame:
    """Adiciona `dv01` ao DataFrame. Requer coluna `duration`."""
    expr_duracao_mod = pl.col("duration") / (1 + pl.col("taxa_indicativa"))
    return df.with_columns(dv01=0.0001 * expr_duracao_mod * pl.col("pu"))


def converter_taxa(taxa: float | Decimal | str) -> float | Decimal:
    """Converte percentual explícito para decimal, sem arredondar ou truncar."""
    if not isinstance(taxa, str):
        return taxa
    texto = taxa.strip()
    if not re.fullmatch(r"[+-]?[0-9]+(?:[.,][0-9]+)?\s*%", texto):
        raise ValueError('Taxa textual deve ter formato percentual, como "5.75%".')
    return Decimal(texto[:-1].strip().replace(",", ".")) / 100


def normalizar_taxa_precificacao(taxa: float | Decimal) -> float:
    """Trunca a taxa decimal em seis casas percentuais, conforme a STN."""
    return truncar(taxa, 8)


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
        Soma dos valores presentes. Retorna ``0.0`` se todas as entradas estiverem
        vazias. Retorna ``float('nan')`` se:
        - Qualquer valor de entrada ou resultado do cálculo for null ou NaN.

    Examples:
        Título com cupons anuais de 10% e principal de R$1000, descontado a 8% a.a.:
        >>> fluxos_caixa = [100, 100, 1100]  # Cupons de R$100 + principal no vencimento
        >>> taxas = [0.08, 0.08, 0.08]  # Taxa de desconto de 8% a.a.
        >>> prazos = [1.0, 2.0, 3.0]  # Pagamentos anuais
        >>> round(calcular_pv(fluxos_caixa, taxas, prazos), 2)
        1051.54

        Retorna zero para entradas vazias:
        >>> calcular_pv([], [], [])
        0.0

        Tamanhos incompatíveis seguem a validação do Polars:
        >>> calcular_pv([100], [0.10, 0.10], [1.0])
        Traceback (most recent call last):
        ...
        polars.exceptions.ShapeError: could not create a new DataFrame: height of
        column 'taxas' (2) does not match height of column 'fluxos_caixa' (1)
    """
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

    if df.is_empty():
        return 0.0

    valores_presentes = df["fluxos_caixa"] / (1 + df["taxas"]) ** df["prazos"]
    if valores_presentes.has_nulls():
        return float("nan")

    return float(valores_presentes.sum())


def _encontrar_intervalo_raiz(  # noqa: PLR0911
    func: Callable[[float], float],
) -> tuple[float, float] | None:
    """
    Encontra um intervalo [a, b] para a TAXA DE JUROS que zera a função.

    Otimizado para o contexto financeiro, buscando a taxa apenas em um
    intervalo entre o próximo float acima de -1 e 10, inclusive. A função
    'func' calcula a diferença de preço dada uma taxa.
    """
    taxa_inicial = 0.01
    taxa_min = math.nextafter(-1.0, 0.0)
    taxa_max = 10.0

    f0 = func(taxa_inicial)
    if not math.isfinite(f0):
        return None
    if f0 == 0:
        return (taxa_inicial, taxa_inicial)

    for limite in (taxa_max, taxa_min):
        a, fa = taxa_inicial, f0
        passo = math.copysign(0.01, limite - taxa_inicial)
        while a != limite:
            b = min(max(a + passo, taxa_min), taxa_max)
            try:
                fb = func(b)
            except (OverflowError, InvalidOperation):
                # Taxas próximas de -1 podem exceder a precisão da precificação.
                return None
            if not math.isfinite(fb):
                return None
            if fb == 0:
                return (b, b)
            if (fa < 0) != (fb < 0):
                return (min(a, b), max(a, b))
            a, fa = b, fb
            passo *= 1.6

    return None


def _metodo_bissecao(  # noqa: PLR0911
    func: Callable[[float], float], a: float, b: float
) -> float:
    """Método da bisseção para encontrar raiz."""
    TOLERANCIA = 1e-12
    MAX_ITERACOES = 100
    fa, fb = func(a), func(b)
    if not math.isfinite(fa) or not math.isfinite(fb):
        return float("nan")
    if fa == 0:
        return a
    if fb == 0:
        return b
    if (fa < 0) == (fb < 0):
        return float("nan")

    for _ in range(MAX_ITERACOES):
        ponto_medio = a / 2 + b / 2
        fmeio = func(ponto_medio)
        if not math.isfinite(fmeio):
            return float("nan")
        if fmeio == 0 or b / 2 - a / 2 < TOLERANCIA:
            return ponto_medio
        if (fmeio < 0) != (fa < 0):
            b = ponto_medio
        else:
            a, fa = ponto_medio, fmeio

    return float("nan")


def encontrar_raiz(
    func_diferenca_preco: Callable[[float], float],
    intervalo: tuple[float, float] | None = None,
) -> float:
    """Encontra a raiz de uma função de diferença de preço.

    Aplica o método da bisseção no intervalo informado. Quando omitido,
    procura automaticamente um intervalo válido para a taxa de juros.

    Args:
        func_diferenca_preco: Função cuja raiz será encontrada.
        intervalo: Limites inferior e superior da busca. Se ``None``, usa
            a busca automática de intervalo entre o próximo float acima de -1
            e 10, inclusive.

    Returns:
        Raiz aproximada, com parada por zero exato da função ou tolerância
        absoluta de 1e-12 na metade da largura do intervalo. Retorna NaN se não
        encontrar mudança de sinal, houver avaliação não finita ou não convergir
        em 100 iterações.
        A busca automática também retorna NaN se a expansão exceder a capacidade
        numérica da precificação (OverflowError ou InvalidOperation).

    Raises:
        ValueError: Limites não finitos ou em ordem decrescente.
    """
    if intervalo is None:
        intervalo = _encontrar_intervalo_raiz(func_diferenca_preco)
        if intervalo is None:
            return float("nan")

    a, b = intervalo
    if not math.isfinite(a) or not math.isfinite(b) or a > b:
        raise ValueError("Os limites do intervalo devem ser finitos e ordenados.")
    return _metodo_bissecao(func_diferenca_preco, a, b)


def cotacao_por_taxas(pagamentos: pl.DataFrame) -> float:
    """
    Soma os valores presentes dos fluxos, arredondados na 10ª casa decimal.

    Args:
        pagamentos: DataFrame com uma linha por fluxo e as colunas
            ``valor_pagamento`` (Float64), ``dias_uteis`` (Int64) e
            ``taxa`` (Float64) alinhadas por linha.
    """
    anos_uteis = truncar(pagamentos["dias_uteis"] / 252, 14)
    fatores = (1 + pagamentos["taxa"]) ** anos_uteis
    valores_presentes = pagamentos["valor_pagamento"] / fatores
    return float(valores_presentes.round(10).sum())
