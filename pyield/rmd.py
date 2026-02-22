"""Módulo para buscar dados do Relatório Mensal da Dívida (RMD) do Tesouro Nacional."""

import datetime
import io
import logging
import zipfile as zf

import polars as pl
import requests
from lxml import html

from pyield._internal.retry import retry_padrao

registro = logging.getLogger(__name__)

URL_BASE = (
    "https://www.tesourotransparente.gov.br/publicacoes/relatorio-mensal-da-divida-rmd"
)
_ABAS_DISPONIVEIS = ("1.3",)

# Índices de linha (0-based) na planilha após leitura com fastexcel (sem cabeçalho)
# O fastexcel compacta linhas totalmente vazias, resultando em 81 linhas ao invés das
# 101 do Excel bruto. Os índices abaixo refletem o layout observado no arquivo atual.
_LINHA_PERIODOS = 2  # Rótulos de período: "Nov/06", "Dez/06", ..., "2025"
_LINHA_INICIO_DADOS = 3  # Primeira linha de dados: "I - EMISSÕES"
_LINHA_FIM_DADOS = 67  # Exclusivo: notas de rodapé a partir desta linha

# Tipos de título que viram colunas (em ordem)
_TITULOS = ("LFT", "LTN", "NTN-B", "NTN-B1", "NTN-F", "NTN-C", "NTN-D", "Demais")

# Mapeamento de rótulo de seção → nome limpo
_SECOES = {"I - EMISSÕES": "Emissões", "II - RESGATES": "Resgates"}

# Rótulos de subgrupo conhecidos e prefixo do Tesouro Direto
_SUBGRUPOS = {"Vendas", "Trocas", "Vencimentos", "Compras"}
_SUBGRUPO_TD = "Tesouro Direto"

# Subgrupos sem detalhamento por tipo de título (valor direto na linha)
# Tuple para ordem determinística; correspondência por prefixo (ignora notas de rodapé)
_SUBGRUPOS_DIRETOS = (
    "Transferência de Carteira",
    "Emissão Direta com Financeiro",
    "Emissão Direta sem Financeiro",
    "Pagamento de Dividendos",
    "Cancelamentos",
)

# Prefixos que sinalizam fim da área de interesse (seções a ignorar)
_PREFIXOS_IGNORAR = ("IMPACTO", "OPERAÇÕES", "III -", "RESGATE")

_MESES_PT = {
    "Jan": 1,
    "Fev": 2,
    "Mar": 3,
    "Abr": 4,
    "Mai": 5,
    "Jun": 6,
    "Jul": 7,
    "Ago": 8,
    "Set": 9,
    "Out": 10,
    "Nov": 11,
    "Dez": 12,
}

# Ordem canônica das combinações (Section, Subgroup)
_ORDEM_SUBGRUPOS = [
    ("Emissões", "Vendas"),
    ("Emissões", "Trocas"),
    ("Emissões", "Tesouro Direto"),
    ("Resgates", "Vencimentos"),
    ("Resgates", "Compras"),
    ("Resgates", "Trocas"),
    ("Resgates", "Tesouro Direto"),
]


def _parsear_periodo(periodo: str) -> datetime.date | None:
    """Converte string de período para datetime.date ou None para totais anuais."""
    try:
        mes_str, ano_str = periodo.split("/")
    except ValueError:
        return None  # ex: "2025" (total anual) → descartado
    mes = _MESES_PT.get(mes_str)
    if mes is None:
        return None
    ano = 2000 + int(ano_str)
    return datetime.date(ano, mes, 1)


@retry_padrao
def _buscar_pagina(url: str) -> bytes:
    """Busca o conteúdo de uma URL, seguindo redirects, com retry."""
    resposta = requests.get(url, timeout=15)
    resposta.raise_for_status()
    return resposta.content


@retry_padrao
def _baixar_zip(url: str) -> bytes:
    """Baixa o arquivo ZIP com retry."""
    resposta = requests.get(url, timeout=60)
    resposta.raise_for_status()
    return resposta.content


def _buscar_url_anexo() -> str:
    """Encontra a URL do arquivo ZIP do anexo mais recente do RMD.

    A URL base redireciona automaticamente para a página do mês atual.
    O lxml localiza o link do anexo ZIP nessa página.
    """
    conteudo_pagina = _buscar_pagina(URL_BASE)
    arvore = html.fromstring(conteudo_pagina)
    resultado = arvore.xpath("//a[contains(@href, 'publicacao-anexo')]/@href")
    if not isinstance(resultado, list) or not resultado:
        raise ValueError("Link do anexo ZIP não encontrado na página do RMD.")
    return str(resultado[0])


def _extrair_excel(conteudo_zip: bytes) -> bytes:
    """Extrai o arquivo Excel do ZIP."""
    with zf.ZipFile(io.BytesIO(conteudo_zip), "r") as arquivo_zip:
        nomes_excel = [
            n for n in arquivo_zip.namelist() if n.lower().endswith((".xlsx", ".xls"))
        ]
        if not nomes_excel:
            raise ValueError("Nenhum arquivo Excel encontrado no ZIP do RMD.")
        return arquivo_zip.read(nomes_excel[0])


def _classificar_categorias(
    categorias: list[str],
) -> tuple[list[tuple[int, str, str, str]], list[tuple[int, str, str]]]:
    """Percorre rótulos de categoria e emite eventos de título e de subgrupo direto.

    Implementa uma máquina de estados que rastreia o grupo (Emissões/Resgates) e o
    subgrupo corrente. Emite dois tipos de evento:
    - eventos_titulos: linhas de detalhe por título (idx, grupo, subgrupo, titulo)
    - eventos_diretos: subgrupos sem detalhamento por título (idx, grupo, subgrupo)

    Args:
        categorias: Lista de rótulos de categoria lidos da coluna 0 do Excel.

    Returns:
        Par (eventos_titulos, eventos_diretos) com as listas de eventos detectados.
    """
    grupo = ""
    subgrupo = ""
    eventos_titulos: list[tuple[int, str, str, str]] = []
    eventos_diretos: list[tuple[int, str, str]] = []
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
                eventos_titulos.append((i, grupo, subgrupo, c))
            else:
                prefixo = next((p for p in _SUBGRUPOS_DIRETOS if c.startswith(p)), None)
                if prefixo:
                    eventos_diretos.append((i, grupo, prefixo))
    return eventos_titulos, eventos_diretos


def _construir_grupos(
    eventos: list[tuple[int, str, str, str]],
) -> dict[tuple[str, str], dict[str, int]]:
    """Agrupa eventos por (grupo, subgrupo): {chave → {titulo → índice na matriz}}."""
    grupos: dict[tuple[str, str], dict[str, int]] = {}
    for idx, grupo, subgrupo, titulo in eventos:
        chave = (grupo, subgrupo)
        if chave not in grupos:
            grupos[chave] = {}
        grupos[chave][titulo] = idx
    return grupos


def _montar_df_titulos(
    grupos: dict[tuple[str, str], dict[str, int]],
    datas_mensais: list[datetime.date],
    matriz: pl.DataFrame,
) -> pl.DataFrame:
    """Monta DataFrame de detalhamento por título na ordem canônica, após unpivot."""
    n_periodos = len(datas_mensais)
    partes: list[pl.DataFrame] = []
    for grupo, subgrupo in _ORDEM_SUBGRUPOS:
        chave = (grupo, subgrupo)
        if chave not in grupos:
            continue
        titulos_presentes = grupos[chave]
        colunas: dict[str, pl.Series] = {
            "periodo": pl.Series("periodo", datas_mensais, dtype=pl.Date),
            "grupo": pl.Series("grupo", [grupo] * n_periodos),
            "subgrupo": pl.Series("subgrupo", [subgrupo] * n_periodos),
        }
        for titulo in _TITULOS:
            if titulo in titulos_presentes:
                valores = list(matriz.row(titulos_presentes[titulo]))
            else:
                valores = [None] * n_periodos
            colunas[titulo] = pl.Series(titulo, valores, dtype=pl.Float64)
        partes.append(pl.DataFrame(colunas))
    return pl.concat(partes).unpivot(
        on=list(_TITULOS),
        index=["periodo", "grupo", "subgrupo"],
        variable_name="titulo",
        value_name="valor",
    )


def _montar_partes_diretos(
    eventos: list[tuple[int, str, str]],
    datas_mensais: list[datetime.date],
    matriz: pl.DataFrame,
) -> list[pl.DataFrame]:
    """Monta lista de DataFrames para subgrupos sem detalhamento por título."""
    n_periodos = len(datas_mensais)
    return [
        pl.DataFrame(
            {
                "periodo": pl.Series("periodo", datas_mensais, dtype=pl.Date),
                "grupo": pl.Series("grupo", [grupo] * n_periodos),
                "subgrupo": pl.Series("subgrupo", [subgrupo] * n_periodos),
                "titulo": pl.Series("titulo", [None] * n_periodos, dtype=pl.String),
                "valor": pl.Series("valor", list(matriz.row(idx)), dtype=pl.Float64),
            }
        )
        for idx, grupo, subgrupo in eventos
    ]


def _estruturar_dados(conteudo_excel: bytes) -> pl.DataFrame:
    """Lê a aba '1.3' do Excel e retorna DataFrame longo com emissões e resgates."""
    df_bruto = pl.read_excel(
        conteudo_excel,
        sheet_name="1.3",
        has_header=False,
    )

    # Rótulos de período: "Nov/06", "Dez/06", ..., "Dez/25", "2025"
    periodos_raw = [str(p) for p in df_bruto.row(_LINHA_PERIODOS)[1:] if p is not None]

    # Filtra totais anuais e converte para datetime.date
    datas_e_indices = [
        (i, d)
        for i, periodo in enumerate(periodos_raw)
        if (d := _parsear_periodo(periodo)) is not None
    ]
    indices_mensais = [i for i, _ in datas_e_indices]
    datas_mensais = [d for _, d in datas_e_indices]

    # Linhas de dados, excluindo linhas sem rótulo de categoria
    df_dados = df_bruto[_LINHA_INICIO_DADOS:_LINHA_FIM_DADOS]
    df_dados = df_dados.filter(df_dados[:, 0].is_not_null())

    eventos_titulos, eventos_diretos = _classificar_categorias(
        [str(c) for c in df_dados[:, 0].to_list()]
    )

    # Matriz de valores (apenas períodos mensais), cast para Float64
    matriz = df_dados[:, 1:].cast(pl.Float64, strict=False)[:, indices_mensais]

    grupos = _construir_grupos(eventos_titulos)
    df_titulos = _montar_df_titulos(grupos, datas_mensais, matriz)
    partes_diretas = _montar_partes_diretos(eventos_diretos, datas_mensais, matriz)
    return (
        pl.concat([df_titulos] + partes_diretas)
        .with_columns(valor=pl.col("valor").mul(1_000_000).round(2))
        .filter(pl.col("valor").is_not_null() & (pl.col("valor") != 0))
    )


_PROCESSADORES: dict[str, object] = {"1.3": _estruturar_dados}


def rmd(tab: str) -> pl.DataFrame:
    """Retorna dados do Relatório Mensal da Dívida (RMD) do Tesouro Nacional.

    Baixa e processa a planilha do RMD, extraindo dados de emissões e resgates
    de Títulos Públicos Federais da Dívida Pública Mobiliária Federal interna
    (DPMFi). A publicação mais recente é descoberta automaticamente via parse
    HTML da página oficial.

    Args:
        tab: Número da aba a processar (ex: "1.3"). Abas implementadas: "1.3".

    Returns:
        DataFrame longo com dados de emissões e resgates por período, seção,
        subgrupo e tipo de título. Registros com valor nulo ou zero são excluídos.
        Em caso de erro, retorna DataFrame vazio e registra log da exceção.

    Output Columns (para tab='1.3'):
        * periodo (Date): primeiro dia do mês de referência (ex: date(2006, 11, 1)).
        * grupo (String): seção principal — "Emissões" ou "Resgates".
        * subgrupo (String): categoria dentro do grupo:
            - Emissões:
                "Vendas"
                "Trocas"
                "Tesouro Direto"
                "Transferência de Carteira"
                "Emissão Direta com Financeiro"
                "Emissão Direta sem Financeiro".
            - Resgates:
                "Vencimentos"
                "Compras"
                "Trocas"
                "Tesouro Direto"
                "Pagamento de Dividendos"
                "Cancelamentos".
        * titulo (String): tipo de título:
            - "LFT"
            - "LTN"
            - "NTN-B"
            - "NTN-B1"
            - "NTN-F"
            - "NTN-C"
            - "NTN-D"
            - "Demais"
            - null para subgrupos sem detalhamento por título
              (ex: "Transferência de Carteira").
        * valor (Float64): valor em R$.

    Raises:
        ValueError: Se `tab` não estiver entre as abas implementadas.

    Notes:
        - A função sempre busca a publicação mais recente disponível.
        - Totais anuais são excluídos; podem ser recalculados via group_by.
        - Operações do Banco Central e totais agregados são excluídos; podem ser
          recalculados por agrupamento quando necessário.
        - Registros com valor nulo ou zero são omitidos.
        - Totais de referência para 2025 (validados contra o Excel do Tesouro Nacional):
            Emissões = R$ 1.840.946.621.648,18
            Resgates = R$ 1.395.109.062.272,45.

    Examples:
        >>> import polars as pl
        >>> from pyield import rmd
        >>> df = rmd("1.3")
        >>> df_2025 = df.filter(pl.col("periodo").dt.year() == 2025)
        >>> # Totais de 2025 — ver valores de referência do Tesouro Nacional nas Notes
        >>> emissoes_2025 = df_2025.filter(pl.col("grupo") == "Emissões")["valor"].sum()
        >>> round(emissoes_2025, 2)
        1840946621648.18
        >>> resgates_2025 = df_2025.filter(pl.col("grupo") == "Resgates")["valor"].sum()
        >>> round(resgates_2025, 2)
        1395109062272.45
    """
    if tab not in _PROCESSADORES:
        disponiveis = ", ".join(f'"{t}"' for t in sorted(_PROCESSADORES))
        raise ValueError(
            f"Aba '{tab}' não disponível. Abas implementadas: {disponiveis}."
        )
    processador = _PROCESSADORES[tab]
    try:
        url_anexo = _buscar_url_anexo()
        registro.debug(f"URL do anexo RMD: {url_anexo}")
        conteudo_zip = _baixar_zip(url_anexo)
        conteudo_excel = _extrair_excel(conteudo_zip)
        df = processador(conteudo_excel)  # type: ignore[operator]
    except Exception as e:
        registro.exception(f"Erro ao coletar dados do RMD (aba {tab!r}): {e}")
        return pl.DataFrame()

    registro.info(f"Dados do RMD (aba {tab!r}) processados. Shape: {df.shape}.")
    return df
