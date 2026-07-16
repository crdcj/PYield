import polars as pl

# As fontes publicam taxas percentuais com no máximo 8 casas decimais. Após a
# divisão por 100, 10 casas preservam essa precisão de origem e limitam o
# resultado à precisão decimal do domínio; o Float64 continua sendo uma
# aproximação IEEE-754.
_CASAS_DECIMAIS_TAXA = 10


def pct_para_decimal(expr: pl.Expr) -> pl.Expr:
    """Converte expressão percentual (ex: 12.15) para decimal (0.1215).

    Aceita qualquer ``pl.Expr``, incluindo seletores como
    ``cs.starts_with("taxa_")``, que também são ``Expr``.
    """
    return expr.truediv(100).round(_CASAS_DECIMAIS_TAXA)


def float_br(coluna: str) -> pl.Expr:
    """Converte coluna string no formato numérico brasileiro para Float64.

    Remove separadores de milhar (.) e troca vírgula decimal (,) por ponto.
    """
    return (
        pl.col(coluna)
        .str.strip_chars()
        .str.replace_all(".", "", literal=True)
        .str.replace(",", ".", literal=True)
        .cast(pl.Float64)
    )


def taxa_br(coluna: str) -> pl.Expr:
    """Converte taxa percentual BR (string) para decimal Float64.

    Args:
        coluna: Nome da coluna com taxa em formato BR (ex.: "12,3456").
    """
    return pct_para_decimal(float_br(coluna))


def inteiro_br(coluna: str) -> pl.Expr:
    """Converte coluna string no formato numérico brasileiro para inteiro.

    Remove separadores de milhar (.) e troca vírgula decimal (,) por ponto,
    depois converte para Int64.
    """
    return float_br(coluna).round(0).cast(pl.Int64)


def inteiro_m(coluna: str) -> pl.Expr:
    """Converte coluna numérica BR em milhares para inteiro (unidades)."""
    return (float_br(coluna) * 1000).round(0).cast(pl.Int64)
