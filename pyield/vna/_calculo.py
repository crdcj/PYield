"""Cálculos genéricos de VNA."""

from decimal import ROUND_HALF_EVEN, Decimal, localcontext

import polars as pl

from pyield._internal.converters import converter_datas
from pyield._internal.numbers import para_decimal, truncar_decimal
from pyield._internal.types import DateLike

TIPO_VNA = pl.Decimal(precision=18, scale=6)
PRECISAO_CALCULO = 28


def expressao_data() -> pl.Expr:
    """Converte a primeira coluna textual das planilhas em data."""
    return pl.col("column_1").str.to_datetime(strict=False).dt.date()


def expressao_vna(coluna: str) -> pl.Expr:
    """Converte o VNA da planilha, inclusive células de texto no formato BR.

    A conversão é estrita: aplique-a somente às linhas com data válida, para
    que um valor ilegível gere erro em vez de ser descartado em silêncio.
    """
    texto = pl.col(coluna).cast(pl.String).str.strip_chars()
    return (
        pl.when(texto.str.contains(",", literal=True))
        .then(
            texto.str.replace_all(".", "", literal=True).str.replace(
                ",", ".", literal=True
            )
        )
        .otherwise(texto)
        .cast(TIPO_VNA)
    )


def calcular_base(
    vna_base: float | Decimal,
    indice_base: float | Decimal,
    indice_atual: float | Decimal,
) -> Decimal:
    """Aplica ``T6(vna_base * T16(indice_atual / indice_base))``.

    Primitiva interna; as origens por título e a documentação pública ficam
    em ``_consulta.calcular_base``. Não trunca a base de entrada, preservando
    o ajuste de emissão da série de NTN-C de 2002/2006.
    """
    base = para_decimal(vna_base)
    inicial = para_decimal(indice_base)
    atual = para_decimal(indice_atual)
    if not base.is_finite() or base <= 0:
        raise ValueError("O VNA-base deve ser positivo e finito.")
    if not inicial.is_finite() or inicial <= 0:
        raise ValueError("O índice-base deve ser positivo e finito.")
    if not atual.is_finite() or atual <= 0:
        raise ValueError("O índice atual deve ser positivo e finito.")
    with localcontext(prec=PRECISAO_CALCULO):
        fator = truncar_decimal(atual / inicial, 16)
        return truncar_decimal(base * fator, 6)


def calcular_vna(
    df: pl.DataFrame,
    data: DateLike,
    *,
    fator_variacao: float | Decimal | None = None,
) -> Decimal:
    """Obtém o VNA publicado ou calcula o pró-rata entre pontos publicados.

    Args:
        df: DataFrame com as colunas ``data`` (Date) e ``vna`` (Decimal ou
            Float64), contendo no máximo um ponto publicado por data.
        data: Data para a qual o VNA será obtido ou interpolado. Aceita
            ``DD-MM-YYYY``, ``DD/MM/YYYY``, ``YYYY-MM-DD`` ou objeto de data.
        fator_variacao: Fator multiplicativo entre o ponto inicial e o final.
            Se omitido, é calculado pela razão entre os VNAs dos dois pontos.

    Returns:
        Decimal: VNA publicado ou calculado por pró-rata exponencial. Retorna
        ``Decimal('NaN')`` quando não houver pontos publicados dos dois lados
        da data ou quando ela estiver fora do intervalo disponível.

    Notes:
        Um ponto publicado exatamente na data tem prioridade e ignora
        ``fator_variacao``. O cálculo usa Decimal com 28 dígitos de precisão;
        o expoente do pró-rata é truncado em catorze casas e o resultado em
        seis casas. Entradas float são convertidas pela representação textual.

        O DataFrame deve conter uma única série de VNA, como a retornada por
        ``yd.vna.historico`` com o filtro de vencimento quando necessário.
        A função seleciona as referências imediatamente anterior e posterior
        à data, sem exigir que o DataFrame esteja ordenado. Não extrapola após
        a última referência, mesmo quando ``fator_variacao`` é informado.
        Para projetar a partir de um VNA-base escalar, use ``yd.vna.projetado``.

        ``fator_variacao`` é multiplicativo: 1,0045 representa alta de 0,45%.
        Para reproduzir o VNA oficial da NTN-B entre referências, prefira
        ``yd.vna.valor``, que obtém o fator dos números-índice do IPCA; a razão
        entre VNAs publicados pode produzir diferenças de truncamento.

    Raises:
        ValueError: Se o VNA-base não for positivo ou a variação for menor ou
            igual a -100%.

    Examples:
        Pró-rata entre referências da NTN-C com vencimento em 01/01/2031,
        consultadas no histórico publicado pelo Tesouro Nacional:

        >>> referencias = yd.vna.historico("NTN-C", vencimento="01-01-2031")
        >>> yd.vna.calcular_vna(referencias, "16-12-2025")
        Decimal('6449.641358')
    """
    data = converter_datas(data)
    if data is None:
        return Decimal("NaN")
    ponto_exato = df.filter(pl.col("data") == data)
    if ponto_exato.height == 1:
        return para_decimal(ponto_exato.item(0, "vna"))

    ponto_inicial = df.filter(pl.col("data") < data).sort("data").tail(1)
    ponto_final = df.filter(pl.col("data") > data).sort("data").head(1)
    if ponto_inicial.is_empty() or ponto_final.is_empty():
        return Decimal("NaN")

    data_inicial = ponto_inicial.item(0, "data")
    vna_inicial = para_decimal(ponto_inicial.item(0, "vna"))
    if fator_variacao is None:
        with localcontext(prec=PRECISAO_CALCULO):
            fator = para_decimal(ponto_final.item(0, "vna")) / vna_inicial
    else:
        fator = para_decimal(fator_variacao)
    return aplicar_pro_rata(
        vna_inicial,
        fator,
        (data - data_inicial).days,
        (ponto_final.item(0, "data") - data_inicial).days,
    )


def aplicar_pro_rata(
    vna_base: Decimal,
    fator: Decimal,
    dias: int,
    dias_periodo: int,
) -> Decimal:
    """Calcula ``T6(vna_base * fator ** T14(dias / dias_periodo))``."""
    if not vna_base.is_finite() or vna_base <= 0:
        raise ValueError("O VNA-base deve ser positivo.")
    if not fator.is_finite() or fator <= 0:
        raise ValueError("A variação deve ser maior que -100%.")

    with localcontext(prec=PRECISAO_CALCULO):
        expoente = truncar_decimal(Decimal(dias) / dias_periodo, 14)
        return truncar_decimal(vna_base * fator**expoente, 6)


def calcular_vna_projetado(
    vna_base: Decimal,
    inflacao: Decimal,
    dias: int,
    dias_periodo: int,
) -> Decimal:
    """Aplica ao VNA projetado as precisões intermediárias definidas pela STN.

    VNA-base T-6, projeção decimal A-4 (A-2 percentual, meio para o par), expoente
    T-14 e resultado T-6.
    """
    with localcontext(prec=PRECISAO_CALCULO):
        base = truncar_decimal(vna_base, 6)
        projecao = inflacao.quantize(Decimal("0.0001"), rounding=ROUND_HALF_EVEN)
        return aplicar_pro_rata(base, 1 + projecao, dias, dias_periodo)


__all__ = ["calcular_vna"]
