"""Cálculos e dados de LFT.

Convenções de precificação (STN, tabela 3):
    - Cotação em base 100, truncada a 4 casas.
    - VNA recebido no cálculo do PU: truncado a 6 casas.
    - Prazo de desconto: dias úteis / 252, truncado a 14 casas.
    - PU: truncado a 6 casas.
"""

from decimal import Decimal

import polars as pl

from pyield import du
from pyield._internal.numbers import truncar_decimal
from pyield._internal.types import DateLike, any_is_empty

from . import _utils

BASE_COTACAO = 100

_SCHEMA_DADOS = {
    "data_referencia": pl.Date,
    "titulo": pl.String,
    "codigo_selic": pl.Int64,
    "data_base": pl.Date,
    "data_vencimento": pl.Date,
    "dias_uteis": pl.Int64,
    "prazo_medio": pl.Float64,
    "pu": pl.Float64,
    "taxa_compra": pl.Float64,
    "taxa_venda": pl.Float64,
    "taxa_indicativa": pl.Float64,
    "taxa_di": pl.Float64,
    "rentabilidade": pl.Float64,
}


def dados(data: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de LFT para a data de referência na ANBIMA.

    Args:
        data: Data da consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de LFT. Na ausência de dados,
            retorna vazio com as mesmas colunas e tipos.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "LFT").
        - codigo_selic (Int64): Código do título no SELIC.
        - data_base (Date): Data base de emissão do título.
        - data_vencimento (Date): Data de vencimento do título.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - prazo_medio (Float64): Prazo médio do título em anos.
        - pu (Float64): Preço unitário (PU).
        - taxa_compra (Float64): Taxa de compra (decimal).
        - taxa_venda (Float64): Taxa de venda (decimal).
        - taxa_indicativa (Float64): Taxa indicativa (decimal).
        - taxa_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo
            método flat forward.
        - rentabilidade (Float64): Razão entre a taxa diária equivalente da
            LFT, usando o DI como referência para a Selic, e a taxa diária DI.

    Examples:
        >>> from pyield import lft
        >>> df_lft = lft.dados("23-08-2024")  # doctest: +SKIP
    """
    df = _utils.obter_tpf(data, "LFT")
    if df.is_empty():
        return pl.DataFrame(schema=_SCHEMA_DADOS)

    df = df.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
    ).with_columns(
        prazo_medio=pl.col("dias_uteis") / 252,
    )
    df = _utils.adicionar_taxa_di(df, data)

    df = df.with_columns(
        rentabilidade=rentabilidade_expr("taxa_indicativa", "taxa_di"),
    )

    return df.select(*_SCHEMA_DADOS)


def vencimentos(data: DateLike) -> pl.Series:
    """
    Busca os vencimentos disponíveis para a data de referência.

    Args:
        data: Data da consulta.

    Returns:
        pl.Series: Série de datas de vencimento disponíveis.

    Examples:
        >>> from pyield import lft
        >>> lft.vencimentos("22-08-2024")
        shape: (14,)
        Series: 'data_vencimento' [date]
        [
            2024-09-01
            2025-03-01
            2025-09-01
            2026-03-01
            2026-09-01
            …
            2029-03-01
            2029-09-01
            2030-03-01
            2030-06-01
            2030-09-01
        ]
    """
    return dados(data)["data_vencimento"]


def cotacao(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float | Decimal | str,
) -> Decimal:
    r"""
    Calcula a cotação de uma LFT pela metodologia da STN para leilões primários.

    Args:
        data_liquidacao: Data de liquidação do título.
        data_vencimento: Data de vencimento do título.
        taxa: Taxa de ágio/deságio da LFT sobre a Selic, anual efetiva, em
            decimal, na base de 252 dias úteis.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
            Antes do cálculo, é truncada em oito casas decimais (seis na
            forma percentual), descartando as casas excedentes sem arredondar.

    Returns:
        Decimal: Cotação em base 100, truncada em 4 casas decimais.
            Retorna ``Decimal("NaN")`` para entradas nulas ou quando o prazo
            até o vencimento não é positivo.

    Notes:
        A cotação teórica em base 100 é:

        \[
        \mathrm{COT} = \frac{100}{(1+s)^t}
        \]

        onde:

        - \(\mathrm{DU}\): dias úteis entre liquidação e vencimento.
        - \(t = \mathrm{DU}/252\): prazo até o vencimento, em anos de
          252 dias úteis.
        - \(s\): taxa de ágio/deságio informada em `taxa`, anual efetiva,
          em decimal, na base de 252 dias úteis. Taxa positiva corresponde
          a deságio; negativa, a ágio; zero, a cotação ao par.

        No cálculo oficial, além do truncamento da taxa descrito em `Args`,
        o prazo é truncado em 14 casas decimais.

        A cotação é calculada e retornada na escala percentual (base 100),
        truncada em 4 casas conforme a STN. Por exemplo, ``99.3651`` representa
        99,3651% do VNA; o PU é calculado como ``VNA * cotacao / 100``.

    Examples:
        Calcula a cotação de uma LFT com taxa de 0,1717%:
        >>> from pyield import lft
        >>> lft.cotacao(
        ...     data_liquidacao="24-07-2024",
        ...     data_vencimento="01-09-2030",
        ...     taxa="0.1717%",
        ... )
        Decimal('98.9645')

    """
    taxa = _utils.converter_taxa(taxa)
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return Decimal("NaN")
    taxa = _utils.normalizar_taxa_precificacao(taxa)
    # Número de dias úteis entre liquidação (inclusivo) e vencimento (exclusivo)
    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return Decimal("NaN")

    # Número de períodos truncado conforme regras da STN
    anos_truncados = _utils.truncar(dias_uteis / 252, 14)

    cotacao_percentual = BASE_COTACAO / (1 + taxa) ** anos_truncados

    return truncar_decimal(cotacao_percentual, 4)


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    vna: float | Decimal,
    pu: float | Decimal,
) -> float:
    r"""
    Calcula a taxa implícita de uma LFT a partir do preço (PU).

    A função resolve numericamente a taxa que aproxima o PU informado por
    seu valor teórico, sem arredondamento nem truncamento
    intermediário. O resultado não pretende reproduzir exatamente a taxa usada
    para gerar um PU canônico por ``pu(vna, cotacao(...))``.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        vna: Valor nominal atualizado (VNA).
        pu: Preço unitário (PU) do título.

    Returns:
        float: Taxa implícita em formato decimal, sem arredondamento. Retorna
            NaN para entradas ausentes, PU não positivo, prazo útil não
            positivo ou falha na resolução numérica.

    Notes:
        A taxa de ágio/deságio \(s\), anual efetiva, em decimal e na base
        de 252 dias úteis, é encontrada resolvendo:

        \[
        \frac{\mathrm{VNA}}{(1+s)^t} = \mathrm{PU}
        \]

        onde:

        - \(\mathrm{VNA}\): valor nominal atualizado recebido em `vna`.
        - \(\mathrm{PU}\): preço recebido em `pu`.
        - \(\mathrm{DU}\): dias úteis entre liquidação e vencimento.
        - \(t = \mathrm{DU}/252\): prazo até o vencimento, em anos de
          252 dias úteis.

        Essa taxa é aplicada sobre a atualização pela Selic, representada
        pelo VNA; não é a taxa de retorno total do título. Taxa positiva
        corresponde a deságio, negativa a ágio e zero a preço igual ao VNA.

    Examples:
        Exibe as taxas em formato decimal:

        >>> from pyield import lft
        >>> lft.taxa("24-07-2024", "01-09-2030", 15785.324502, 15621.867466)
        0.0017170148895820606
        >>> lft.taxa("24-07-2024", "01-03-2025", 15785.324502, 15774.132706) * 100
        0.11612671421607959
        >>> lft.taxa("21-05-2008", "07-03-2014", 3451.215345, 3426.649594) * 100
        0.12345862697111447
    """
    if any_is_empty(data_liquidacao, data_vencimento, vna, pu):
        return float("nan")

    pu_float = float(pu)
    if pu_float <= 0:
        return float("nan")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return float("nan")
    vna_float = float(vna)

    def diferenca_preco(taxa: float) -> float:
        return vna_float / (1 + taxa) ** (dias_uteis / 252) - pu_float

    return _utils.encontrar_raiz(diferenca_preco)


def rentabilidade(taxa_lft: float | str, taxa_di: float | str) -> float:
    r"""
    Calcula a rentabilidade da LFT sobre a taxa de DI Futuro.

    Segue a metodologia do Anexo 2, item 3, de *Dívida Pública: a experiência
    brasileira*, do Tesouro Nacional, que denomina esse indicador prêmio da LFT.

    Args:
        taxa_lft: Taxa de ágio/deságio da LFT sobre a Selic, anual efetiva,
            em decimal, na base de 252 dias úteis.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
        taxa_di: Taxa DI anual efetiva, em decimal, na base de 252 dias úteis,
            para o mesmo vencimento da LFT (interpolada quando necessário).
            Aceita também percentual explícito: "5.75%" ou "5,75%".

    Returns:
        float: Razão entre a taxa diária equivalente da LFT, usando o DI como
            referência para a Selic, e a taxa diária DI. Por exemplo, 1.01
            representa 101% da taxa diária equivalente DI.

    Notes:
        A rentabilidade relativa \(q\) combina o fator diário da taxa da
        LFT com o fator diário DI e compara a taxa resultante com a taxa DI:

        \[
        q = \frac{(1+s)^{1/252}(1+\mathrm{DI})^{1/252}-1}
        {(1+\mathrm{DI})^{1/252}-1}
        \]

        onde:

        - \(s\): taxa de ágio/deságio informada em `taxa_lft`.
        - \(\mathrm{DI}\): taxa DI informada em `taxa_di`, para o mesmo
          vencimento da LFT.

        O cálculo usa o DI como referência para a atualização pela Selic.
        A composição é multiplicativa entre fatores; não soma a taxa da LFT
        à taxa DI. O indicador não representa o retorno realizado entre
        compra e venda.

        A função retorna a razão \(q\). O livro apresenta o indicador em
        percentual, correspondente a \(100q\).

        Referência: Tesouro Nacional, *Dívida Pública: a experiência brasileira*,
        Parte 3, capítulo 2, Anexo 2, item 3, páginas 330–331:
        [Prêmio das LFTs](https://thot-arquivos.tesouro.gov.br/publicacao-anexo/4710).

    Examples:
        Calcula a rentabilidade de uma LFT em 28/04/2025:
        >>> from pyield import lft
        >>> taxa_lft = "0.1124%"
        >>> taxa_di = "13.967670224373396%"
        >>> lft.rentabilidade(taxa_lft, taxa_di)
        1.008594331960501
    """
    if isinstance(taxa_lft, str):
        taxa_lft = float(_utils.converter_taxa(taxa_lft))
    if isinstance(taxa_di, str):
        taxa_di = float(_utils.converter_taxa(taxa_di))
    if any_is_empty(taxa_lft, taxa_di):
        return float("nan")
    # Taxa diária
    fator_lft = (taxa_lft + 1) ** (1 / 252)
    fator_di = (taxa_di + 1) ** (1 / 252)
    return (fator_lft * fator_di - 1) / (fator_di - 1)


def rentabilidade_expr(
    taxa_lft: pl.Expr | str,
    taxa_di: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para a rentabilidade da LFT sobre o DI.

    Args:
        taxa_lft: Nome de coluna ou expressão Polars com a taxa de ágio/deságio
            da LFT sobre a Selic, anual efetiva, em decimal, na base de
            252 dias úteis.
        taxa_di: Nome de coluna ou expressão Polars com a taxa DI anual efetiva,
            em decimal, na base de 252 dias úteis, para o mesmo vencimento da
            LFT (interpolada quando necessário).

    Returns:
        pl.Expr: Expressão sem alias com a rentabilidade da LFT sobre o DI.
    """
    expr_lft = taxa_lft if isinstance(taxa_lft, pl.Expr) else pl.col(taxa_lft)
    expr_di = taxa_di if isinstance(taxa_di, pl.Expr) else pl.col(taxa_di)
    fator_lft = (expr_lft + 1) ** (1 / 252)
    fator_di = (expr_di + 1) ** (1 / 252)
    return (fator_lft * fator_di - 1) / (fator_di - 1)


def _calcular_pu(
    vna: float | Decimal,
    cotacao: float | Decimal,
) -> Decimal:
    """Calcula o preço unitário da LFT a partir do VNA e da cotação."""
    if any_is_empty(vna, cotacao):
        return Decimal("NaN")
    vna_decimal = truncar_decimal(vna, 6)
    cotacao_decimal = truncar_decimal(cotacao, 4)
    return truncar_decimal(vna_decimal * cotacao_decimal / BASE_COTACAO, 6)


def pu(
    vna: float | Decimal,
    cotacao: float | Decimal,
) -> Decimal:
    r"""
    Calcula o PU da LFT pela metodologia da STN para leilões primários.

    Args:
        vna: Valor nominal atualizado (VNA), truncado em seis casas decimais
            antes do cálculo, sem arredondar.
        cotacao: Cotação da LFT em base 100.
            Truncada em quatro casas decimais antes do cálculo, sem arredondar.

    Returns:
        Decimal: Preço da LFT truncado em 6 casas decimais.

    Notes:
        O preço unitário é:

        \[
        \mathrm{PU} = \mathrm{VNA}\,\frac{\mathrm{COT}}{100}
        \]

        onde:

        - \(\mathrm{VNA}\): valor nominal atualizado recebido em `vna`.
        - \(\mathrm{COT}\): cotação recebida em `cotacao`, em base 100.

        A cotação representa o percentual do VNA usado no preço. O cálculo
        aplica os truncamentos de VNA e cotação descritos em `Args` e trunca
        o PU em seis casas decimais.

    References:
        - Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos
          Públicos Federais Ofertados nos Leilões Primários.
          https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

    Examples:
        >>> from pyield import lft
        >>> lft.pu(15785.324502, 99.9291)
        Decimal('15774.132706')
    """
    return _calcular_pu(vna, cotacao)


__all__ = [
    "cotacao",
    "dados",
    "pu",
    "rentabilidade",
    "rentabilidade_expr",
    "taxa",
    "vencimentos",
]


def __dir__() -> list[str]:
    return __all__
