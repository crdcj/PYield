"""Parser da aba 1.3 do RMD."""

import datetime as dt

import polars as pl

from ._common import parsear_periodo

_LINHA_PERIODOS = 2
_LINHA_INICIO_DADOS = _LINHA_PERIODOS + 1

_TITULOS = ("LFT", "LTN", "NTN-B", "NTN-B1", "NTN-F", "NTN-C", "NTN-D", "Demais")
_SECOES = {"I - EMISSÕES": "Emissões", "II - RESGATES": "Resgates"}
_SUBGRUPOS = {"Vendas", "Trocas", "Vencimentos", "Compras"}
_SUBGRUPO_TD = "Tesouro Direto"
_SUBGRUPOS_DIRETOS = (
    "Transferência de Carteira",
    "Emissão Direta com Financeiro",
    "Emissão Direta sem Financeiro",
    "Pagamento de Dividendos",
    "Cancelamentos",
)
_PREFIXOS_IGNORAR = ("IMPACTO", "OPERAÇÕES", "III -", "RESGATE")


def _classificar_categorias(
    categorias: list[str],
) -> list[tuple[int, str, str, str | None]]:
    """Percorre rótulos de categoria e classifica linhas de dados."""
    grupo = ""
    subgrupo = ""
    eventos: list[tuple[int, str, str, str | None]] = []

    for i, cat in enumerate(categorias):
        c = cat.strip()
        if c in _SECOES:
            grupo, subgrupo = _SECOES[c], ""
        elif any(c.startswith(p) for p in _PREFIXOS_IGNORAR):
            grupo = ""
        elif grupo:
            if c in _SUBGRUPOS:
                subgrupo = c
            elif c.startswith(_SUBGRUPO_TD):
                subgrupo = _SUBGRUPO_TD
            elif c in _TITULOS:
                eventos.append((i, grupo, subgrupo, c))
            else:
                prefixo = next((p for p in _SUBGRUPOS_DIRETOS if c.startswith(p)), None)
                if prefixo:
                    eventos.append((i, grupo, prefixo, None))

    return eventos


def _montar_registros(
    eventos: list[tuple[int, str, str, str | None]],
    datas_mensais: list[dt.date],
    matriz: pl.DataFrame,
) -> pl.DataFrame:
    """Monta DataFrame longo com todos os registros de emissões e resgates."""
    linhas = [
        (data, grupo, subgrupo, titulo, valor)
        for idx, grupo, subgrupo, titulo in eventos
        for data, valor in zip(datas_mensais, matriz.row(idx))
    ]
    return pl.DataFrame(
        linhas,
        schema={
            "periodo": pl.Date,
            "grupo": pl.String,
            "subgrupo": pl.String,
            "titulo": pl.String,
            "valor": pl.Float64,
        },
        orient="row",
    )


def estruturar_dados(conteudo_excel: bytes) -> pl.DataFrame:
    """Lê a aba ``1.3`` do Excel e retorna DataFrame longo."""
    df_bruto = pl.read_excel(
        conteudo_excel,
        sheet_name="1.3",
        has_header=False,
    )

    periodos_raw = [str(p) for p in df_bruto.row(_LINHA_PERIODOS)[1:] if p is not None]

    datas_e_indices = [
        (i, data)
        for i, periodo in enumerate(periodos_raw)
        if (data := parsear_periodo(periodo)) is not None
    ]
    indices_mensais = [i for i, _ in datas_e_indices]
    datas_mensais = [data for _, data in datas_e_indices]

    df_dados = df_bruto[_LINHA_INICIO_DADOS:]
    df_dados = df_dados.filter(df_dados[:, 0].is_not_null())

    eventos = _classificar_categorias([str(c) for c in df_dados[:, 0].to_list()])
    matriz = df_dados[:, 1:].cast(pl.Float64, strict=False)[:, indices_mensais]

    return (
        _montar_registros(eventos, datas_mensais, matriz)
        .with_columns(valor=pl.col("valor").mul(1_000_000).round(2))
        .filter(pl.col("valor").is_not_null() & (pl.col("valor") != 0))
    )
