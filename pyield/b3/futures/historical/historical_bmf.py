import datetime as dt
import logging

import polars as pl
import requests
from lxml import html

import pyield.b3.common as cm
from pyield import bday
from pyield.fwd import forwards
from pyield._internal.retry import retry_padrao

logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DE CONTRATOS ---
CONTRATOS_TAXA = {"DI1", "DAP", "DDI", "FRC", "FRO"}

CONVENCOES_CONTAGEM = {"DAP": 252, "DI1": 252, "DDI": 360}
DIAS_UTEIS_ANO = 252
DIAS_CORRIDOS_ANO = 360

# --- Mapeamento de Colunas HTML ---

# 1. Colunas FIXAS (Nomes de destino constantes)
MAPA_BASE = {
    "VENCTO": ("ExpirationCode", pl.Utf8),
    "CONTR. ABERT.(1)": ("OpenContracts", pl.Int64),
    "CONTR. FECH.(2)": ("OpenContractsEndSession", pl.Int64),
    "NÚM. NEGOC.": ("TradeCount", pl.Int64),
    "CONTR. NEGOC.": ("TradeVolume", pl.Int64),
    "VOL.": ("FinancialVolume", pl.Int64),
    "AJUSTE ANTER. (3)": ("PrevSettlementPrice", pl.Float64),
    "AJUSTE CORRIG. (4)": ("AdjSettlementPrice", pl.Float64),
    "AJUSTE": ("SettlementPrice", pl.Float64),  # Sempre Preço
    "AJUSTE\n       DE REF.": ("SettlementRate", pl.Float64),  # Somente FRC
    "VAR. PTOS.": ("PointsVariation", pl.Float64),
}

# 2. Colunas VARIÁVEIS (Sufixo Rate ou Price dependendo do contrato)
MAPA_VARIAVEL = {
    "PREÇO ABERTU.": ("Open", pl.Float64),
    "PREÇO MÍN.": ("Min", pl.Float64),
    "PREÇO MÁX.": ("Max", pl.Float64),
    "PREÇO MÉD.": ("Avg", pl.Float64),
    "ÚLT. PREÇO": ("Close", pl.Float64),
    "ÚLT.OF. COMPRA": ("CloseAsk", pl.Float64),
    "ÚLT.OF. VENDA": ("CloseBid", pl.Float64),
}

# Tipagem para o casting (mapeia o nome FINAL para o Tipo)
# Criamos um dicionário que contém as duas versões (Rate e Price) para o casting.
TIPOS_COLUNAS_FINAIS = {v[0]: v[1] for v in MAPA_BASE.values()}
for prefixo, tipo in MAPA_VARIAVEL.values():
    TIPOS_COLUNAS_FINAIS[f"{prefixo}Rate"] = tipo
    TIPOS_COLUNAS_FINAIS[f"{prefixo}Price"] = tipo

CODIGOS_MESES_ANTIGOS = {
    "JAN": 1,
    "FEV": 2,
    "MAR": 3,
    "ABR": 4,
    "MAI": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SET": 9,
    "OUT": 10,
    "NOV": 11,
    "DEZ": 12,
}

COLUNAS_SAIDA = [
    "TradeDate",
    "TickerSymbol",
    "ExpirationDate",
    "BDaysToExp",
    "DaysToExp",
    "OpenContracts",
    "TradeCount",
    "TradeVolume",
    "FinancialVolume",
    "DV01",
    "SettlementPrice",
    "OpenRate",
    "OpenPrice",
    "MinRate",
    "MinPrice",
    "AvgRate",
    "AvgPrice",
    "MaxRate",
    "MaxPrice",
    "CloseAskRate",
    "CloseAskPrice",
    "CloseBidRate",
    "CloseBidPrice",
    "CloseRate",
    "ClosePrice",
    "SettlementRate",
    "ForwardRate",
]


def _obter_mapa_renomeacao(codigo_contrato: str) -> dict[str, str]:
    """Gera o mapa de renomeação dinâmico baseado no tipo de contrato."""
    sufixo = "Rate" if codigo_contrato in CONTRATOS_TAXA else "Price"

    # Mapeia base
    mapa_renomeacao = {k: v[0] for k, v in MAPA_BASE.items()}
    # Mapeia variáveis com o sufixo correto
    for nome_html, (prefixo, _) in MAPA_VARIAVEL.items():
        mapa_renomeacao[nome_html] = f"{prefixo}{sufixo}"

    return mapa_renomeacao


def _calcular_data_vencimento_legado(
    data: dt.date, codigo_vencimento: str
) -> dt.date | None:
    """Calcula a data de vencimento no formato legado."""
    try:
        mes = CODIGOS_MESES_ANTIGOS[codigo_vencimento[:3]]
        digito_ano = int(codigo_vencimento[-1])
        ano = data.year // 10 * 10 + digito_ano
        if ano < data.year:
            ano += 10
        data_vencimento = dt.date(ano, mes, 1)
        return bday.offset(dates=data_vencimento, offset=0)
    except (KeyError, ValueError):
        return None


def _converter_precos_para_taxas(
    precos: pl.Series,
    dias_ate_vencimento: pl.Series,
    convencao_contagem: int,
) -> pl.Series:
    """Converte preços em taxas com a convenção de contagem informada."""
    if convencao_contagem == DIAS_CORRIDOS_ANO:
        taxas = (100_000 / precos - 1) * (DIAS_CORRIDOS_ANO / dias_ate_vencimento)
    else:  # 252
        taxas = (100_000 / precos) ** (DIAS_UTEIS_ANO / dias_ate_vencimento) - 1
    return taxas.round(5)


@retry_padrao
def _buscar_html(data: dt.date, codigo_contrato: str) -> str:
    """Busca o HTML da página de pregão da BMF."""
    data_url = data.strftime("%d/%m/%Y")
    url_base = "https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp"
    parametros = {"Data": data_url, "Mercadoria": codigo_contrato, "XLS": "true"}
    resposta = requests.get(url_base, params=parametros, timeout=10)
    resposta.raise_for_status()
    resposta.encoding = "iso-8859-1"
    return resposta.text


def _parsear_html_lxml(html_texto: str) -> pl.DataFrame:
    """Extrai a tabela principal do HTML e retorna um DataFrame."""
    if not html_texto:
        return pl.DataFrame()
    arvore = html.fromstring(html_texto)
    linhas_cabecalho = arvore.xpath('(//tr[@class="tabelaSubTitulo"])[1]')
    if not linhas_cabecalho:
        return pl.DataFrame()
    primeira_linha_cabecalho = linhas_cabecalho[0]  # type: ignore
    celulas_cabecalho = primeira_linha_cabecalho.xpath(".//th | .//td")  # type: ignore
    nomes_colunas = [
        cell.text_content().strip()
        for cell in celulas_cabecalho  # type: ignore
    ]
    if "VENCTO" not in nomes_colunas:
        return pl.DataFrame()
    container_tabela = primeira_linha_cabecalho.getparent()  # type: ignore
    linhas = container_tabela.xpath(  # type: ignore
        './/tr[@class="tabelaConteudo1" or @class="tabelaConteudo2"]'
    )
    dados = []
    for linha in linhas:  # type: ignore
        celulas = linha.xpath(".//td")
        celulas_limpas = [cell.text_content().strip() for cell in celulas]
        if len(celulas_limpas) == len(nomes_colunas):
            dados.append(celulas_limpas)
    return pl.DataFrame(dados, schema=nomes_colunas, orient="row")


def _limpar_valores_texto(df: pl.DataFrame) -> pl.DataFrame:
    """Limpa valores textuais e normaliza separadores numéricos."""
    if "PointsVariation" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("PointsVariation").str.ends_with("-"))
            .then("-" + pl.col("PointsVariation").str.replace("-", "", literal=True))
            .otherwise(pl.col("PointsVariation").str.replace("+", "", literal=True))
            .alias("PointsVariation")
        )
    df = df.select(
        pl.all()
        .str.strip_chars()
        .str.replace_all(".", "", literal=True)
        .str.replace(",", ".")
    )
    return df


def _converter_tipos_colunas(df: pl.DataFrame) -> pl.DataFrame:
    """Realiza o casting baseado nos nomes de colunas já traduzidos."""
    # Filtra apenas os tipos das colunas que realmente existem no DF atual
    tipos_aplicar = {k: v for k, v in TIPOS_COLUNAS_FINAIS.items() if k in df.columns}
    return df.cast(tipos_aplicar, strict=False)


def _adicionar_vencimentos(
    df: pl.DataFrame, data: dt.date, codigo_contrato: str
) -> pl.DataFrame:
    df = df.with_columns(
        TradeDate=data,
        TickerSymbol=codigo_contrato + pl.col("ExpirationCode"),
    )
    if data < dt.date(2006, 5, 22):
        datas_vencimento = [
            _calcular_data_vencimento_legado(data, codigo_vencimento)
            for codigo_vencimento in df["ExpirationCode"]
        ]
        df = df.with_columns(pl.Series("ExpirationDate", datas_vencimento))
    else:
        df = cm.adicionar_vencimento(df, codigo_contrato, "TickerSymbol")

    df = df.with_columns(
        BDaysToExp=bday.count_expr(data, "ExpirationDate"),
        DaysToExp=(pl.col("ExpirationDate") - pl.col("TradeDate")).dt.total_days(),
    ).filter(pl.col("DaysToExp") > 0)
    return df


def _converter_zeros_para_nulos(df: pl.DataFrame) -> pl.DataFrame:
    """Converte zeros numéricos para nulos."""
    return df.with_columns(pl.all().replace(0, None))


def _ajustar_taxas_di1_legado(df: pl.DataFrame, colunas_taxa: list) -> pl.DataFrame:
    for coluna in colunas_taxa:
        coluna_taxa = _converter_precos_para_taxas(
            df[coluna], df["BDaysToExp"], DIAS_UTEIS_ANO
        )
        df = df.with_columns(coluna_taxa.alias(coluna))
    if {"MinRate", "MaxRate"}.issubset(set(colunas_taxa)):
        df = df.with_columns(MinRate=pl.col("MaxRate"), MaxRate=pl.col("MinRate"))
    return df


def _transformar_taxas(
    df: pl.DataFrame, data: dt.date, codigo_contrato: str
) -> pl.DataFrame:
    # Seleciona apenas o que terminou com "Rate"
    colunas_taxa = [c for c in df.columns if c.endswith("Rate")]

    data_troca = dt.date(2002, 1, 17)
    if data <= data_troca and codigo_contrato == "DI1":
        df = _ajustar_taxas_di1_legado(df, colunas_taxa)
    else:
        if codigo_contrato in {"FRC", "FRO"} and "PointsVariation" in df.columns:
            colunas_taxa.append("PointsVariation")

        if colunas_taxa:
            df = df.with_columns(pl.col(colunas_taxa).truediv(100).round(5))
    return df


def _adicionar_colunas_derivadas(
    df: pl.DataFrame, codigo_contrato: str
) -> pl.DataFrame:
    convencao = CONVENCOES_CONTAGEM.get(codigo_contrato)
    if convencao in {252, 360} and "SettlementPrice" in df.columns:
        dias = df["BDaysToExp"] if convencao == DIAS_UTEIS_ANO else df["DaysToExp"]
        df = df.with_columns(
            SettlementRate=_converter_precos_para_taxas(
                df["SettlementPrice"], dias, convencao
            )
        )
    if codigo_contrato == "DI1" and {"SettlementRate", "SettlementPrice"}.issubset(
        df.columns
    ):
        duracao = pl.col("BDaysToExp") / 252
        duracao_mod = duracao / (1 + pl.col("SettlementRate"))
        df = df.with_columns(DV01=0.0001 * duracao_mod * pl.col("SettlementPrice"))

    if codigo_contrato in {"DI1", "DAP"} and "SettlementRate" in df.columns:
        df = df.with_columns(
            ForwardRate=forwards(df["BDaysToExp"], df["SettlementRate"])
        )
    return df


def _buscar_df_historico(data: dt.date, codigo_contrato: str) -> pl.DataFrame:
    """Obtém e processa o histórico da BMF para um contrato e data."""
    html_texto = _buscar_html(data, codigo_contrato)
    df = _parsear_html_lxml(html_texto)
    if df.is_empty():
        return pl.DataFrame()

    # 1. Renomeação Dinâmica (Ponto central da mudança)
    mapa_renomeacao = _obter_mapa_renomeacao(codigo_contrato)
    df = df.rename(mapa_renomeacao, strict=False)

    # 2. Limpeza e Tipagem
    df = _limpar_valores_texto(df)
    df = _converter_tipos_colunas(df)

    # 3. Processamento
    df = _adicionar_vencimentos(df, data, codigo_contrato)
    df = _converter_zeros_para_nulos(df)
    df = _transformar_taxas(df, data, codigo_contrato)
    df = _adicionar_colunas_derivadas(df, codigo_contrato)

    return df.select([c for c in COLUNAS_SAIDA if c in df.columns])
