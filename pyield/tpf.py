"""Títulos Públicos Federais."""

from collections.abc import Sequence

import polars as pl

from pyield._internal.types import DateLike
from pyield.anbima.imaq import estoque_anbima
from pyield.anbima.mercado_secundario import (
    TipoTPF,
    taxas_indicativas,
    vencimentos_taxas_indicativas,
)
from pyield.bc.tpf_intradia import secundario_intradia_bcb
from pyield.bc.tpf_mensal import secundario_mensal_bcb
from pyield.tn.leiloes import leilao_tn


def taxas(
    data: DateLike,
    titulo: TipoTPF | None = None,
) -> pl.DataFrame:
    """Busca taxas e preços indicativos de TPFs.

    Fonte: ANBIMA. Primeiro consulta o cache local de dados históricos; se a
    data não estiver no cache, busca diretamente na fonte da ANBIMA.

    Args:
        data: Data de referência.
        titulo: Tipo do título público federal. Aceita ``LFT``, ``NTN-B``,
            ``NTN-C``, ``LTN``, ``NTN-F`` ou ``PRE``.

    Returns:
        DataFrame Polars com taxas e preços indicativos. Retorna DataFrame
        vazio se não houver dados para a data.

    Output Columns:
        * titulo (String): tipo do título público.
        * data_referencia (Date): data de referência dos dados.
        * codigo_selic (Int64): código do título no SELIC.
        * data_base (Date): data base ou de emissão do título.
        * data_vencimento (Date): data de vencimento do título.
        * pu (Float64): preço unitário para liquidação em D0.
        * taxa_compra (Float64): taxa de compra em D0.
        * taxa_venda (Float64): taxa de venda em D0.
        * taxa_indicativa (Float64): taxa indicativa em D0.

    Notes:
        Para obter o dado completo direto da fonte, sem cache nem seleção de
        colunas, use ``pyield.anbima.tpf_fonte``.
    """
    return taxas_indicativas(data=data, titulo=titulo)


def vencimentos(data: DateLike, titulo: TipoTPF) -> pl.Series:
    """Busca vencimentos de TPFs disponíveis nas taxas indicativas.

    Fonte: ANBIMA, mesma base usada por ``tpf.taxas``.

    Args:
        data: Data de referência.
        titulo: Tipo do título público federal. Aceita ``LFT``, ``NTN-B``,
            ``NTN-C``, ``LTN``, ``NTN-F`` ou ``PRE``.

    Returns:
        Series ordenada com os vencimentos disponíveis.
    """
    return vencimentos_taxas_indicativas(data=data, titulo=titulo)


def estoque(data: DateLike) -> pl.DataFrame:
    """Busca dados de estoque de TPFs.

    Fonte: IMA-Q da ANBIMA. Contém quantidade em mercado, valor de mercado
    e variação diária da quantidade dos títulos.

    Args:
        data: Data de referência.

    Returns:
        DataFrame Polars com dados de estoque. Retorna DataFrame vazio se a
        data for inválida ou não houver dados.

    Output Columns:
        * data_referencia (Date): data de referência dos dados.
        * titulo (String): tipo do título público.
        * data_vencimento (Date): data de vencimento do título.
        * codigo_selic (Int64): código SELIC do título.
        * isin (String): código ISIN.
        * pu (Float64): preço unitário do título em reais.
        * quantidade_mercado (Int64): quantidade em mercado.
        * valor_mercado (Int64): valor de mercado em reais.
        * variacao_quantidade (Int64): variação diária da quantidade.
        * status_titulo (String): status do título.
    """
    return estoque_anbima(data)


def secundario_intradia() -> pl.DataFrame:
    """Busca dados intradia do mercado secundário de TPFs.

    Fonte: Banco Central do Brasil, sistema SELIC. Os dados ficam disponíveis
    apenas durante o horário do SELIC (09:00-22:00 BRT) em dias úteis.

    Returns:
        DataFrame Polars com negociações intradia do mercado secundário.
        Retorna DataFrame vazio fora do horário do SELIC.

    Output Columns:
        * data_hora_consulta (Datetime): data e hora da consulta.
        * data_liquidacao (Date): data de liquidação à vista.
        * titulo (String): sigla do título público.
        * codigo_selic (Int64): código SELIC do título.
        * data_vencimento (Date): data de vencimento do título.
        * pu_minimo (Float64): menor preço negociado.
        * pu_medio (Float64): preço médio negociado.
        * pu_maximo (Float64): maior preço negociado.
        * pu_ultimo (Float64): último preço negociado.
        * taxa_minima (Float64): menor taxa negociada.
        * taxa_media (Float64): taxa média negociada.
        * taxa_maxima (Float64): maior taxa negociada.
        * taxa_ultima (Float64): última taxa negociada.
        * operacoes (Int64): total de operações liquidadas.
        * quantidade (Int64): quantidade total de títulos negociados.
        * financeiro (Float64): valor financeiro total negociado.
        * operacoes_corretagem (Int64): operações via corretagem.
        * quantidade_corretagem (Int64): títulos via corretagem.
        * termo_pu_minimo (Float64): menor preço a termo negociado.
        * termo_pu_medio (Float64): preço médio a termo negociado.
        * termo_pu_ultimo (Float64): último preço a termo negociado.
        * termo_pu_maximo (Float64): maior preço a termo negociado.
        * termo_taxa_ultima (Float64): última taxa a termo negociada.
        * termo_taxa_minima (Float64): menor taxa a termo negociada.
        * termo_taxa_media (Float64): taxa média a termo negociada.
        * termo_taxa_maxima (Float64): maior taxa a termo negociada.
        * termo_operacoes (Int64): total de operações a termo.
        * termo_quantidade (Int64): total de títulos a termo negociados.
        * termo_financeiro (Float64): valor financeiro total a termo.
        * termo_operacoes_corretagem (Int64): operações a termo via corretagem.
        * termo_quantidade_corretagem (Int64): títulos a termo via corretagem.
    """
    return secundario_intradia_bcb()


def secundario_mensal(
    data: DateLike,
    extragrupo: bool = False,
) -> pl.DataFrame:
    """Busca dados mensais do mercado secundário de TPFs.

    Fonte: Banco Central do Brasil, sistema SELIC. Baixa o arquivo mensal de
    negociações secundárias para o mês correspondente à data informada.

    Args:
        data: Data de referência.
        extragrupo: Se verdadeiro, busca apenas negociações extragrupo.

    Returns:
        DataFrame Polars com dados mensais do mercado secundário.

    Output Columns:
        * data_liquidacao (Date): data de liquidação da negociação.
        * titulo (String): sigla do título público.
        * codigo_selic (Int64): código único no sistema SELIC.
        * isin (String): código ISIN.
        * data_emissao (Date): data de emissão do título.
        * data_vencimento (Date): data de vencimento do título.
        * operacoes (Int64): número total de operações.
        * quantidade (Int64): quantidade total negociada.
        * financeiro (Float64): valor financeiro negociado.
        * pu_minimo (Float64): preço unitário mínimo.
        * pu_medio (Float64): preço unitário médio.
        * pu_maximo (Float64): preço unitário máximo.
        * pu_lastro (Float64): preço unitário de lastro.
        * valor_par (Float64): valor par do título.
        * taxa_minima (Float64): taxa mínima.
        * taxa_media (Float64): taxa média.
        * taxa_maxima (Float64): taxa máxima.
        * operacoes_corretagem (Int64): operações com corretagem.
        * quantidade_corretagem (Int64): quantidade com corretagem.
    """
    return secundario_mensal_bcb(data=data, extragrupo=extragrupo)


def leilao(data: DateLike | Sequence[DateLike]) -> pl.DataFrame:
    """Busca resultados de leilões de TPFs.

    Fonte: Tesouro Nacional. Retorna dados de quantidades, financeiros,
    taxas de colocação, duration e DV01 dos leilões nas datas informadas.

    Args:
        data: Data ou sequência de datas do leilão.

    Returns:
        DataFrame Polars com os dados processados do leilão. Se ``data`` for
        uma sequência, concatena os resultados das datas informadas. Retorna
        DataFrame vazio se não houver dados para as datas.

    Output Columns:
        * data_1v (Date): data de realização do leilão.
        * data_liquidacao_1v (Date): data de liquidação financeira da 1ª volta.
        * data_liquidacao_2v (Date): data de liquidação financeira da 2ª volta.
        * numero_edital (Int64): número do edital do leilão.
        * tipo_leilao (String): tipo da operação.
        * titulo (String): código do título público leiloado.
        * benchmark (String): descrição de referência do título.
        * data_vencimento (Date): data de vencimento do título.
        * dias_uteis (Int32): dias úteis entre liquidação e vencimento.
        * dias_corridos (Int32): dias corridos entre liquidação e vencimento.
        * duration (Float64): duration de Macaulay em anos.
        * prazo_medio (Float64): maturidade média em anos.
        * quantidade_ofertada_1v (Int64): quantidade ofertada na 1ª volta.
        * quantidade_ofertada_2v (Int64): quantidade ofertada na 2ª volta.
        * quantidade_aceita_1v (Int64): quantidade aceita na 1ª volta.
        * quantidade_aceita_2v (Int64): quantidade aceita na 2ª volta.
        * quantidade_aceita_total (Int64): quantidade aceita total.
        * financeiro_ofertado_1v (Float64): financeiro ofertado na 1ª volta.
        * financeiro_ofertado_2v (Float64): financeiro ofertado na 2ª volta.
        * financeiro_ofertado_total (Float64): financeiro ofertado total.
        * financeiro_aceito_1v (Float64): financeiro aceito na 1ª volta.
        * financeiro_aceito_2v (Float64): financeiro aceito na 2ª volta.
        * financeiro_aceito_total (Float64): financeiro aceito total.
        * quantidade_bcb (Int64): quantidade adquirida pelo Banco Central.
        * financeiro_bcb (Int64): financeiro adquirido pelo Banco Central.
        * colocacao_1v (Float64): taxa de colocação da 1ª volta.
        * colocacao_2v (Float64): taxa de colocação da 2ª volta.
        * colocacao_total (Float64): taxa de colocação total.
        * dv01_1v (Float64): DV01 da 1ª volta em reais.
        * dv01_2v (Float64): DV01 da 2ª volta em reais.
        * dv01_total (Float64): DV01 total em reais.
        * ptax (Float64): PTAX usada na conversão para dólar.
        * dv01_1v_usd (Float64): DV01 da 1ª volta em dólar.
        * dv01_2v_usd (Float64): DV01 da 2ª volta em dólar.
        * dv01_total_usd (Float64): DV01 total em dólar.
        * pu_minimo (Float64): preço unitário mínimo aceito.
        * pu_medio (Float64): preço unitário médio ponderado aceito.
        * tipo_pu_medio (String): origem do PU médio.
        * taxa_media (Float64): taxa média aceita.
        * taxa_maxima (Float64): taxa máxima aceita.
    """
    return leilao_tn(data)


__all__ = [
    "estoque",
    "leilao",
    "secundario_intradia",
    "secundario_mensal",
    "taxas",
    "vencimentos",
]
