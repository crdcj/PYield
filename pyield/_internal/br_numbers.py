import polars as pl


def numero_br(coluna: str) -> pl.Expr:
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


def taxa_br(coluna: str, casas_pct: int = 4) -> pl.Expr:
    """Converte taxa percentual BR (string) para decimal Float64.

    Args:
        coluna: Nome da coluna com taxa em formato BR (ex.: "12,3456").
        casas_pct: Casas decimais da taxa percentual de entrada. Default 4,
            padrão do mercado brasileiro de renda fixa.
    """
    return (numero_br(coluna) / 100).round(casas_pct + 2)


def inteiro_m(coluna: str) -> pl.Expr:
    """Converte coluna numérica BR em milhares para inteiro (unidades)."""
    return (numero_br(coluna) * 1000).round(0).cast(pl.Int64)
