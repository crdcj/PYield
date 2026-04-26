"""Títulos Públicos Federais."""

from collections.abc import Sequence

import polars as pl

from pyield._internal.types import DateLike
from pyield.anbima import imaq as _imaq
from pyield.anbima import mercado_secundario as _ms
from pyield.anbima.mercado_secundario import TipoTPF
from pyield.bc import tpf_intradia as _tpf_intradia
from pyield.bc import tpf_mensal as _tpf_mensal
from pyield.tn import benchmark as _benchmark
from pyield.tn import leiloes as _leiloes
from pyield.tn import pre as _pre
from pyield.tn import rmd as _rmd
from pyield.tn import utils as _utils


def taxas(
    data: DateLike,
    titulo: TipoTPF | None = None,
    completo: bool = False,
) -> pl.DataFrame:
    """Busca taxas e preços indicativos de TPFs.

    Fonte: ANBIMA. Primeiro consulta o cache local de dados históricos; se a
    data não estiver no cache, busca diretamente na fonte da ANBIMA.

    Args:
        data: Data de referência.
        titulo: Tipo do título público federal. Aceita ``LFT``, ``NTN-B``,
            ``NTN-C``, ``LTN``, ``NTN-F`` ou ``PRE``.
        completo: Se verdadeiro, retorna os dados da ANBIMA sem cache nem filtro de colunas.

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

    Examples:
        >>> df = yd.tpf.taxas(data="06-02-2026")
    """
    return _ms.taxas(data=data, titulo=titulo, completo=completo)


def vencimentos(data: DateLike, titulo: TipoTPF) -> pl.Series:
    """Busca vencimentos de TPFs disponíveis nas taxas indicativas.

    Fonte: ANBIMA, mesma base usada por ``tpf.taxas``.

    Args:
        data: Data de referência.
        titulo: Tipo do título público federal. Aceita ``LFT``, ``NTN-B``,
            ``NTN-C``, ``LTN``, ``NTN-F`` ou ``PRE``.

    Returns:
        Series ordenada com os vencimentos disponíveis.

    Examples:
        >>> yd.tpf.vencimentos(data="22-08-2025", titulo="PRE")
        shape: (18,)
        Series: 'data_vencimento' [date]
        [
            2025-10-01
            2026-01-01
            2026-04-01
            2026-07-01
            2026-10-01
            …
            2030-01-01
            2031-01-01
            2032-01-01
            2033-01-01
            2035-01-01
        ]
    """
    return _ms.vencimentos(data=data, titulo=titulo)


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

    Examples:
        >>> yd.tpf.estoque("04-02-2026")  # doctest: +SKIP
    """
    return _imaq.estoque(data)


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

    Examples:
        >>> df = yd.tpf.secundario_intradia()  # doctest: +SKIP
    """
    return _tpf_intradia.secundario_intradia()


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

    Examples:
        >>> df = yd.tpf.secundario_mensal("07-01-2025", extragrupo=True)
    """
    return _tpf_mensal.secundario_mensal(data=data, extragrupo=extragrupo)


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
    return _leiloes.leilao(data)


def benchmarks(
    titulo: str | None = None,
    incluir_historico: bool = False,
) -> pl.DataFrame:
    """Busca benchmarks de títulos públicos brasileiros.

    Fonte: API do Tesouro Nacional.

    Args:
        titulo: Tipo do título a filtrar (ex.: ``"LFT"``). Se ``None``,
            retorna todos os títulos.
        incluir_historico: Se ``True``, inclui benchmarks históricos; se
            ``False`` (padrão), retorna apenas benchmarks vigentes
            (on-the-run).

    Returns:
        DataFrame Polars com os benchmarks. Retorna DataFrame vazio se
        não houver dados.

    Output Columns:
        * titulo (String): tipo do título (ex.: ``"LTN"``, ``"LFT"``).
        * data_vencimento (Date): data de vencimento do benchmark.
        * benchmark (String): nome/identificador do benchmark.
        * data_inicio (Date): data de início da vigência.
        * data_fim (Date): data de término da vigência.

    Notes:
        Documentação da API:
        https://portal-conhecimento.tesouro.gov.br/catalogo-componentes/api-leil%C3%B5es

    Examples:
        >>> df = yd.tpf.benchmarks()  # doctest: +SKIP
    """
    return _benchmark.benchmarks(titulo=titulo, incluir_historico=incluir_historico)


def premio_pre(
    data: DateLike,
    pontos_base: bool = False,
) -> pl.DataFrame:
    """Calcula o prêmio dos títulos prefixados (LTN e NTN-F) sobre o DI.

    Em linguagem de mercado, esse valor é chamado de prêmio. Em termos
    descritivos, trata-se do spread sobre o DI.

    Definição do prêmio:
        premio = taxa indicativa do PRE - taxa de ajuste do DI

    Quando ``pontos_base=False`` a coluna retorna essa diferença em formato
    decimal (ex: 0.000439 ≈ 4.39 bps). Quando ``pontos_base=True`` o valor
    é automaticamente multiplicado por 10_000 e exibido diretamente em
    basis points.

    Args:
        data: Data da consulta para buscar as taxas.
        pontos_base: Se True, retorna o prêmio já convertido em basis
            points. Padrão False.

    Returns:
        DataFrame com as colunas do prêmio. Retorna DataFrame vazio se
        não houver dados.

    Output Columns:
        * titulo (String): tipo do título.
        * data_vencimento (Date): data de vencimento.
        * premio (Float64): prêmio em decimal ou bps conforme parâmetro
            (spread sobre o DI).

    Examples:
        >>> yd.tpf.premio_pre("30-05-2025", pontos_base=True)
        shape: (18, 3)
        ┌────────┬─────────────────┬────────┐
        │ titulo ┆ data_vencimento ┆ premio │
        │ ---    ┆ ---             ┆ ---    │
        │ str    ┆ date            ┆ f64    │
        ╞════════╪═════════════════╪════════╡
        │ LTN    ┆ 2025-07-01      ┆ 4.39   │
        │ LTN    ┆ 2025-10-01      ┆ -9.0   │
        │ LTN    ┆ 2026-01-01      ┆ -4.88  │
        │ LTN    ┆ 2026-04-01      ┆ -4.45  │
        │ LTN    ┆ 2026-07-01      ┆ 0.81   │
        │ …      ┆ …               ┆ …      │
        │ NTN-F  ┆ 2027-01-01      ┆ -3.31  │
        │ NTN-F  ┆ 2029-01-01      ┆ 14.21  │
        │ NTN-F  ┆ 2031-01-01      ┆ 21.61  │
        │ NTN-F  ┆ 2033-01-01      ┆ 11.51  │
        │ NTN-F  ┆ 2035-01-01      ┆ 22.0   │
        └────────┴─────────────────┴────────┘
    """
    return _utils.premio_pre(data, pontos_base=pontos_base)


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
    return _rmd.rmd(aba)


def curva_pre(data: DateLike) -> pl.DataFrame:
    """Constrói a curva PRE (taxas zero cupom prefixadas).

    Combina taxas de LTN (já zero cupom) com taxas spot derivadas de NTN-F
    via bootstrap. O resultado é a curva de juros prefixada brasileira expressa
    em taxas zero cupom.

    Fonte: ANBIMA (taxas indicativas de LTN e NTN-F).

    Args:
        data: Data de referência.

    Returns:
        DataFrame com a curva PRE para a data solicitada. Retorna DataFrame
        vazio se não houver dados de LTN disponíveis.

    Output Columns:
        * data_vencimento (Date): data de vencimento do vértice.
        * dias_uteis (Int64): dias úteis entre a data de referência e o vencimento.
        * taxa_zero (Float64): taxa zero cupom anualizada (base 252).

    Raises:
        ValueError: Se houver NTN-F sem dados de LTN para bootstrap.

    Examples:
        >>> df = yd.tpf.curva_pre("18-06-2025")  # doctest: +SKIP
    """
    return _pre.taxas_zero(data)
