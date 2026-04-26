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
_TIMEOUT_SEGUNDOS = 60

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
def _buscar_conteudo(url: str) -> bytes:
    """Busca o conteúdo de uma URL, seguindo redirects, com retry."""
    resposta = requests.get(url, timeout=_TIMEOUT_SEGUNDOS)
    resposta.raise_for_status()
    return resposta.content


def _buscar_url_anexo() -> str:
    """Encontra a URL do arquivo ZIP do anexo mais recente do RMD.

    A URL base redireciona automaticamente para a página do mês atual.
    O lxml localiza o link do anexo ZIP nessa página.
    """
    conteudo_pagina = _buscar_conteudo(URL_BASE)
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
) -> list[tuple[int, str, str, str | None]]:
    """Percorre rótulos de categoria e classifica linhas de dados.

    Máquina de estados que rastreia grupo (Emissões/Resgates) e subgrupo.
    Retorna lista de eventos (idx, grupo, subgrupo, titulo). Para subgrupos
    sem detalhamento por título, titulo é None.

    Args:
        categorias: Lista de rótulos de categoria lidos da coluna 0 do Excel.

    Returns:
        Lista de eventos (idx, grupo, subgrupo, titulo) detectados.
    """
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
    datas_mensais: list[datetime.date],
    matriz: pl.DataFrame,
) -> pl.DataFrame:
    """Monta DataFrame longo com todos os registros de emissões e resgates."""
    linhas = [
        (data, grupo, subgrupo, titulo, val)
        for idx, grupo, subgrupo, titulo in eventos
        for data, val in zip(datas_mensais, matriz.row(idx))
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


def _estruturar_dados(conteudo_excel: bytes) -> pl.DataFrame:
    """Lê a aba '1.3' do Excel e retorna DataFrame longo com emissões e resgates."""
    df_bruto = pl.read_excel(
        conteudo_excel,
        sheet_name="1.3",
        has_header=False,
    )

    periodos_raw = [str(p) for p in df_bruto.row(_LINHA_PERIODOS)[1:] if p is not None]

    datas_e_indices = [
        (i, d)
        for i, periodo in enumerate(periodos_raw)
        if (d := _parsear_periodo(periodo)) is not None
    ]
    indices_mensais = [i for i, _ in datas_e_indices]
    datas_mensais = [d for _, d in datas_e_indices]

    df_dados = df_bruto[_LINHA_INICIO_DADOS:_LINHA_FIM_DADOS]
    df_dados = df_dados.filter(df_dados[:, 0].is_not_null())

    eventos = _classificar_categorias([str(c) for c in df_dados[:, 0].to_list()])

    matriz = df_dados[:, 1:].cast(pl.Float64, strict=False)[:, indices_mensais]

    return (
        _montar_registros(eventos, datas_mensais, matriz)
        .with_columns(valor=pl.col("valor").mul(1_000_000).round(2))
        .filter(pl.col("valor").is_not_null() & (pl.col("valor") != 0))
    )


def rmd(aba: str) -> pl.DataFrame:
    """Retorna dados do Relatório Mensal da Dívida (RMD) do Tesouro Nacional.

    Baixa e processa a planilha do RMD, extraindo dados de emissões e resgates
    de Títulos Públicos Federais da Dívida Pública Mobiliária Federal interna
    (DPMFi). A publicação mais recente é descoberta automaticamente via parse
    HTML da página oficial.

    Args:
        aba: Número da aba a processar (ex: ``"1.3"``). Abas implementadas: ``"1.3"``.

    Returns:
        DataFrame longo com dados de emissões e resgates por período, seção,
        subgrupo e tipo de título. Registros com valor nulo ou zero são excluídos.
        Em caso de erro, retorna DataFrame vazio e registra log da excessão.

    Output Columns:
        * periodo (Date): primeiro dia do mês de referência.
        * grupo (String): seção principal — ``"Emissões"`` ou ``"Resgates"``.
        * subgrupo (String): categoria dentro do grupo.
        * titulo (String): tipo de título (``"LFT"``, ``"LTN"``, ``"NTN-B"``,
            ``"NTN-B1"``, ``"NTN-F"``, ``"NTN-C"``, ``"NTN-D"``, ``"Demais"``,
            ou ``null`` para subgrupos sem detalhamento por título).
        * valor (Float64): valor em R$.

    Raises:
        ValueError: Se ``aba`` não estiver entre as abas implementadas.

    Notes:
        - A função sempre busca a publicação mais recente disponível.
        - Totais anuais são excluídos; podem ser recalculados via group_by.
        - Totais de referência para 2025:
            Emissões = R$ 1.840.946.621.648,18
            Resgates = R$ 1.395.109.062.272,45.

    Examples:
        >>> df = yd.tpf.rmd(aba="1.3")  # doctest: +SKIP
    """
    if aba not in _ABAS_DISPONIVEIS:
        disponiveis = ", ".join(f'"{t}"' for t in sorted(_ABAS_DISPONIVEIS))
        raise ValueError(
            f"Aba '{aba}' não disponível. Abas implementadas: {disponiveis}."
        )
    try:
        url_anexo = _buscar_url_anexo()
        registro.debug(f"URL do anexo RMD: {url_anexo}")
        conteudo_zip = _buscar_conteudo(url_anexo)
        conteudo_excel = _extrair_excel(conteudo_zip)
        df = _estruturar_dados(conteudo_excel)
    except Exception as e:
        registro.exception(f"Erro ao coletar dados do RMD (aba {aba!r}): {e}")
        return pl.DataFrame()

    registro.info(f"Dados do RMD (aba {aba!r}) processados. Shape: {df.shape}.")
    return df
