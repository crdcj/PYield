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
NOME_ABA = "1.3"

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
) -> list[tuple[int, str, str, str]]:
    """Percorre rótulos de categoria e emite eventos (índice, grupo, subgrupo, título).

    Implementa uma máquina de estados que rastreia o grupo (Emissões/Resgates) e o
    subgrupo corrente. Para cada linha de detalhe de título dentro de uma área de
    interesse, emite uma tupla com o índice original, grupo, subgrupo e tipo de título.

    Args:
        categorias: Lista de rótulos de categoria lidos da coluna 0 do Excel.

    Returns:
        Lista de tuplas (idx, grupo, subgrupo, titulo) para linhas de detalhe.
    """
    grupo = ""
    subgrupo = ""
    eventos: list[tuple[int, str, str, str]] = []
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
    return eventos


def _estruturar_dados(conteudo_excel: bytes) -> pl.DataFrame:
    """Lê a aba '1.3' do Excel e retorna DataFrame tidy semi-largo.

    Cada linha representa uma combinação (Date, Section, Subgroup).
    Os tipos de título aparecem como colunas com null quando não aplicável.
    """
    df_bruto = pl.read_excel(
        conteudo_excel,
        sheet_name=NOME_ABA,
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
    n_periodos = len(datas_mensais)

    # Linhas de dados, excluindo linhas sem rótulo de categoria
    df_dados = df_bruto[_LINHA_INICIO_DADOS:_LINHA_FIM_DADOS]
    df_dados = df_dados.filter(df_dados[:, 0].is_not_null())

    eventos = _classificar_categorias([str(c) for c in df_dados[:, 0].to_list()])

    # Matriz de valores (apenas períodos mensais), cast para Float64
    matriz = df_dados[:, 1:].cast(pl.Float64, strict=False)[:, indices_mensais]

    # Agrupa eventos por (grupo, subgrupo): {chave → {titulo → índice na matriz}}
    grupos: dict[tuple[str, str], dict[str, int]] = {}
    for idx, grupo, subgrupo, titulo in eventos:
        chave = (grupo, subgrupo)
        if chave not in grupos:
            grupos[chave] = {}
        grupos[chave][titulo] = idx

    # Monta um DataFrame parcial por subgrupo na ordem canônica e concatena
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

    return (
        pl.concat(partes)
        .with_columns(demais=pl.sum_horizontal("Demais", "NTN-D"))
        .drop(["NTN-D", "Demais"])
    )


def data() -> pl.DataFrame:
    """Retorna dados de emissões e resgates de títulos públicos da aba 1.3 do RMD.

    Baixa e processa a planilha do Relatório Mensal da Dívida (RMD) do Tesouro
    Nacional, extraindo dados de emissões e resgates de Títulos Públicos Federais
    da Dívida Pública Mobiliária Federal interna (DPMFi). A publicação mais
    recente é descoberta automaticamente via parse HTML da página oficial.

    Returns:
        DataFrame tidy com dados de emissões e resgates por período, seção e
        subgrupo. Em caso de erro, retorna DataFrame vazio e registra log da exceção.

    Output Columns:
        * periodo (Date): primeiro dia do mês de referência (ex: date(2006, 11, 1)).
        * grupo (String): grupo — "Emissões" ou "Resgates".
        * subgrupo (String): subgrupo — "Vendas", "Trocas", "Tesouro Direto",
          "Vencimentos" ou "Compras".
        * LFT (Float64): valor para Letras Financeiras do Tesouro; null quando N/A.
        * LTN (Float64): valor para Letras do Tesouro Nacional; null quando N/A.
        * NTN-B (Float64): valor para NTN-B; null quando N/A.
        * NTN-B1 (Float64): valor para NTN-B1 (apenas Tesouro Direto); null nos demais.
        * NTN-F (Float64): valor para NTN-F; null quando N/A.
        * NTN-C (Float64): valor para NTN-C; null para Tesouro Direto e quando N/A.
        * demais (Float64): demais títulos, incluindo NTN-D; null para Tesouro Direto
          e quando N/A.

    Notes:
        - A função sempre busca a publicação mais recente disponível.
        - Dados em R$ milhões.
        - Totais anuais são excluídos; podem ser recalculados via group_by.
        - Operações do Banco Central e totais agregados são excluídos; podem ser
          recalculados por agrupamento quando necessário.
        - Cada período gera 7 linhas: 3 de Emissões (Vendas, Trocas, Tesouro Direto)
          e 4 de Resgates (Vencimentos, Compras, Trocas, Tesouro Direto).

    Examples:
        >>> from pyield import tn
        >>> df = tn.rmd.data()
        >>> "grupo" in df.columns and "subgrupo" in df.columns
        True
        >>> df.shape[1]
        10
        >>> df["periodo"].dtype == pl.Date
        True
    """
    try:
        url_anexo = _buscar_url_anexo()
        registro.debug(f"URL do anexo RMD: {url_anexo}")
        conteudo_zip = _baixar_zip(url_anexo)
        conteudo_excel = _extrair_excel(conteudo_zip)
        df = _estruturar_dados(conteudo_excel)
    except Exception as e:
        registro.exception(f"Erro ao coletar dados do RMD: {e}")
        return pl.DataFrame()

    registro.info(f"Dados do RMD processados. Shape: {df.shape}.")
    return df
