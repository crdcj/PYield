import polars as pl

# 10 casas decimais após divisão por 100: limpa ruído IEEE-754 (ex:
# 12.15 / 100 → 0.12150000000000001) sem truncar dado real. BCB e B3
# publicam taxas com no máximo 6–8 casas decimais; 10 é margem segura.
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
