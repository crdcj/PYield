"""Parser da aba 2.1 do RMD."""

import datetime as dt

import polars as pl

from ._common import limpar_rotulo, parsear_periodo

_LINHA_PERIODOS = 2
_LINHA_INICIO_DADOS = _LINHA_PERIODOS + 1

# Rótulos (uppercase) que definem transições de estado hierárquico.
# Valor: (detentor, tipo, categoria, pode_emitir)
# pode_emitir=False indica estado intermediário; linhas folha são ignoradas até
# a próxima transição com pode_emitir=True.
_TRANSICOES: dict[str, tuple[str | None, str | None, str | None, bool]] = {
    "DPF EM PODER DO PÚBLICO": (None, None, None, False),
    "DPMFI": ("Público", "DPMFi", None, False),
    "TESOURO NACIONAL": ("Público", "DPMFi", "Tesouro Nacional", True),
    "BANCO CENTRAL": ("Público", "DPMFi", "Banco Central", True),
    "DPFE": ("Público", "DPFe", None, False),
    "DÍVIDA MOBILIÁRIA": ("Público", "DPFe", "Mobiliária", True),
    "DÍVIDA CONTRATUAL": ("Público", "DPFe", "Contratual", True),
    "DPMFI EM PODER DO BANCO CENTRAL": ("Banco Central", "DPMFi", None, True),
}


def _obter_periodos_mensais(
    df_bruto: pl.DataFrame,
) -> list[tuple[int, dt.date]]:
    """Extrai os pares (índice_coluna, data) dos períodos mensais válidos."""
    periodos_raw = [str(p) for p in df_bruto.row(_LINHA_PERIODOS)[1:] if p is not None]
    return [
        (i, data)
        for i, periodo in enumerate(periodos_raw)
        if (data := parsear_periodo(periodo)) is not None
    ]


def _montar_registros(df_bruto: pl.DataFrame) -> list[tuple[object, ...]]:
    """Converte o bloco hierárquico da aba em registros longos (somente folhas)."""
    periodos = _obter_periodos_mensais(df_bruto)
    linhas = df_bruto[_LINHA_INICIO_DADOS:]
    detentor: str | None = None
    tipo: str | None = None
    categoria: str | None = None
    pode_emitir: bool = False
    registros: list[tuple[object, ...]] = []

    for linha in linhas.iter_rows():
        bruto = linha[0]
        if bruto is None:
            continue

        rotulo = limpar_rotulo(bruto)
        if not rotulo:
            continue

        transicao = _TRANSICOES.get(rotulo.upper())
        if transicao is not None:
            detentor, tipo, categoria, pode_emitir = transicao
            continue

        if not pode_emitir:
            continue

        valores = linha[1:]
        for indice, data in periodos:
            registros.append((data, detentor, tipo, categoria, rotulo, valores[indice]))

    return registros


def estruturar_dados(conteudo_excel: bytes) -> pl.DataFrame:
    """Lê a aba ``2.1`` do Excel e retorna DataFrame longo."""
    df_bruto = pl.read_excel(
        conteudo_excel,
        sheet_name="2.1",
        has_header=False,
    )

    return (
        pl.DataFrame(
            _montar_registros(df_bruto),
            schema={
                "periodo": pl.Date,
                "detentor": pl.String,
                "tipo": pl.String,
                "categoria": pl.String,
                "titulo": pl.String,
                "valor": pl.Float64,
            },
            orient="row",
        )
        .with_columns(valor=pl.col("valor").mul(1_000_000_000).round(2))
        .filter(pl.col("valor").is_not_null())
    )
