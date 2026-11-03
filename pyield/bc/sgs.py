"""Séries do Sistema Gerenciador de Séries (SGS) do Banco Central.

Séries disponíveis:
    - IGP-M mensal (SGS 189)
    - PTAX Venda (SGS 1)
    - SELIC Diária (SGS 11)
    - DI Over diária (SGS 12)
    - SELIC Meta (SGS 432)
    - SELIC Over (SGS 1178)
    - DI Over anualizada base 252 (SGS 4389)

Exemplos de chamada à API:
    https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?formato=json&dataInicial=29/01/2025&dataFinal=31/01/2025
    https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados/ultimos/5?formato=json

Exemplo de resposta JSON da API do BCB:
    [{"data":"29/01/2025","valor":"12.15"},
     {"data":"30/01/2025","valor":"13.15"},
     {"data":"31/01/2025","valor":"13.15"}]

Notas de implementação:
    - Intervalos longos são divididos automaticamente em blocos seguros.
    - SELIC Over, Meta e DI: percentuais convertidos diretamente do texto
      para Decimal e divididos por 100; escala 4 para taxas anuais e 8
      para taxas diárias.
    - PTAX Venda: valor em R$/US$, convertido do texto para Decimal com escala 4.
"""

import datetime as dt
from decimal import Decimal
from enum import Enum
from http import HTTPStatus

import polars as pl
import requests

from pyield import relogio
from pyield._internal.converters import converter_datas, data_referencia_valida
from pyield._internal.retry import requisitar
from pyield._internal.types import DateLike, any_is_empty

URL_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs."

ESQUEMA_BRUTO = {"data": pl.Date, "valor": pl.String}

# Limite de segurança em dias, correspondendo a ~9.5 anos.
# Evita a complexidade do cálculo exato de 10 anos-calendário.
LIMITE_DIAS_SEGURO = 3500  # aprox 365 * 9.5

_DATA_INICIO_SELIC_META = dt.date(1999, 3, 5)
_LIMITE_DIAS_SELIC_META = 1800

_TIPO_TAXA_ANUAL = pl.Decimal(precision=18, scale=4)
_TIPO_TAXA_DIARIA = pl.Decimal(precision=18, scale=8)
_TIPO_PTAX = pl.Decimal(precision=18, scale=4)


class SerieSGS(Enum):
    """Enum para as séries disponíveis no SGS do Banco Central."""

    PTAX_VENDA = 1
    SELIC_DIARIA = 11
    DI_DIARIA = 12
    IGPM = 189
    SELIC_META = 432
    SELIC_OVER = 1178
    DI_OVER = 4389


# ── Infraestrutura de acesso à API SGS ──────────────────────────────


def _chamar_api(url_api: str) -> list[dict[str, str]]:
    resposta = requisitar(requests.get, url_api, timeout_leitura=30)
    return resposta.json()


def _montar_url_intervalo(
    serie: SerieSGS, inicio: dt.date, fim: dt.date | None = None
) -> str:
    inicio_str = inicio.strftime("%d/%m/%Y")
    url = f"{URL_BASE}{serie.value}/dados?formato=json&dataInicial={inicio_str}"
    if fim:
        url += f"&dataFinal={fim.strftime('%d/%m/%Y')}"
    return url


def _montar_url_ultimos(serie: SerieSGS, n: int) -> str:
    return f"{URL_BASE}{serie.value}/dados/ultimos/{n}?formato=json"


def _buscar_api(url_api: str) -> pl.DataFrame:
    """Busca dados da API e retorna DataFrame bruto {data, valor}."""
    try:
        dados = _chamar_api(url_api)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == HTTPStatus.NOT_FOUND:
            return pl.DataFrame(schema=ESQUEMA_BRUTO)
        raise

    if not dados:
        return pl.DataFrame(schema=ESQUEMA_BRUTO)

    return pl.from_dicts(dados).select(
        pl.col("data").str.to_date("%d/%m/%Y"),
        "valor",
    )


def _buscar_dados_url(
    serie: SerieSGS,
    inicio: DateLike,
    fim: DateLike | None = None,
) -> pl.DataFrame:
    """Orquestra a busca, dividindo intervalos longos em blocos seguros."""
    data_inicio = converter_datas(inicio)
    data_fim = converter_datas(fim) if fim else relogio.hoje()
    limite_dias = LIMITE_DIAS_SEGURO

    if serie == SerieSGS.SELIC_META:
        data_inicio = max(data_inicio, _DATA_INICIO_SELIC_META)
        limite_dias = _LIMITE_DIAS_SELIC_META

    if data_inicio > data_fim:
        return pl.DataFrame(schema=ESQUEMA_BRUTO)

    # O SGS rejeita dataInicial == dataFinal (HTTP 400). Um bloco de um dia
    # consulta também o dia anterior, descartado no filtro final.
    urls = []
    inicio_bloco = data_inicio
    while inicio_bloco <= data_fim:
        fim_bloco = min(
            inicio_bloco + dt.timedelta(days=limite_dias),
            data_fim,
        )
        inicio_consulta = min(inicio_bloco, fim_bloco - dt.timedelta(days=1))
        urls.append(_montar_url_intervalo(serie, inicio_consulta, fim_bloco))
        inicio_bloco = fim_bloco + dt.timedelta(days=1)

    # Busca sequencial para interromper na primeira falha da fonte.
    todos_dfs = [_buscar_api(url) for url in urls]

    todos_dfs = [df for df in todos_dfs if not df.is_empty()]

    if not todos_dfs:
        return pl.DataFrame(schema=ESQUEMA_BRUTO)

    return (
        pl.concat(todos_dfs)
        .filter(pl.col("data") >= data_inicio)
        .unique(subset=["data"], keep="first")
        .sort("data")
    )


def _buscar_serie(
    serie: SerieSGS,
    inicio: DateLike | None,
    fim: DateLike | None,
    ultimos: int | None,
) -> pl.DataFrame:
    """Busca genérica para qualquer série SGS."""
    if ultimos is not None:
        return _buscar_api(_montar_url_ultimos(serie, ultimos))
    if inicio is not None:
        return _buscar_dados_url(serie, inicio, fim)
    raise ValueError("Informe 'inicio' ou 'ultimos'.")


# ── Helpers de transformação ─────────────────────────────────────────

ESQUEMA_PTAX = {"data": pl.Date, "cotacao": _TIPO_PTAX}


def _converter_para_taxa(df: pl.DataFrame, *, anualizada: bool = True) -> pl.DataFrame:
    """Converte valor percentual para decimal e renomeia para 'taxa'."""
    tipo = _TIPO_TAXA_ANUAL if anualizada else _TIPO_TAXA_DIARIA
    if df.is_empty():
        return pl.DataFrame(schema={"data": pl.Date, "taxa": tipo})
    return df.select(
        "data",
        taxa=(pl.col("valor").cast(tipo) / 100).cast(tipo),
    )


def _extrair_escalar(df: pl.DataFrame, coluna: str) -> Decimal:
    """Extrai um Decimal de um DataFrame ou retorna Decimal('NaN') se vazio."""
    if df.is_empty():
        return Decimal("NaN")
    return df[coluna].item(0)


def igpm_taxa_serie(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    *,
    ultimos: int | None = None,
) -> pl.DataFrame:
    """Obtém as taxas mensais do IGP-M via SGS do Banco Central (série 189).

    O índice é produzido pela FGV. A série representa a variação mensal,
    não o número-índice nem a variação acumulada em 12 meses.

    Args:
        inicio: Qualquer data no primeiro mês do intervalo.
        fim: Qualquer data no último mês do intervalo. Se ``None``, usa a
            data atual no Brasil.
        ultimos: Número de meses mais recentes a recuperar. Se informado,
            tem prioridade sobre ``inicio`` e ``fim``.

    Returns:
        DataFrame com as taxas mensais em decimal, ou vazio se não houver dados.

    Output Columns:
        * periodo (Int64): competência mensal no formato YYYYMM.
        * taxa (Decimal(18, 4)): variação mensal em decimal (0.005 = 0,50%).

    Notes:
        Os valores percentuais são convertidos diretamente do texto do SGS
        para Decimal e divididos por 100, sem passagem por float.

    Raises:
        ValueError: Se não informar ``inicio`` nem ``ultimos``, ou se
            ``ultimos`` for menor ou igual a 0.

    Examples:
        >>> yd.igpm.taxa_serie("01-01-2025", "31-03-2025")
        shape: (3, 2)
        ┌─────────┬───────────────┐
        │ periodo ┆ taxa          │
        │ ---     ┆ ---           │
        │ i64     ┆ decimal[18,4] │
        ╞═════════╪═══════════════╡
        │ 202501  ┆ 0.0027        │
        │ 202502  ┆ 0.0106        │
        │ 202503  ┆ -0.0034       │
        └─────────┴───────────────┘

        A taxa é mensal e decimal: março registra deflação de 0,34%.
        Para acompanhar as últimas competências, sem fixar os valores:

        >>> df = yd.igpm.taxa_serie(ultimos=12)
    """
    if ultimos is not None:
        if ultimos <= 0:
            raise ValueError("O número de meses deve ser maior que 0.")
    else:
        if inicio is None:
            raise ValueError("Informe 'inicio' ou 'ultimos'.")
        inicio = converter_datas(inicio).replace(day=1)
        fim = converter_datas(fim).replace(day=1) if fim is not None else None
    return (
        _buscar_serie(SerieSGS.IGPM, inicio, fim, ultimos)
        .select(
            periodo=pl.col("data").dt.strftime("%Y%m").cast(pl.Int64),
            # A divisão decimal do Polars preserva a escala do numerador.
            taxa=(pl.col("valor").cast(pl.Decimal(precision=18, scale=4)) / 100).cast(
                pl.Decimal(precision=18, scale=4)
            ),
        )
        .sort("periodo")
    )


def igpm_taxa(data: DateLike) -> Decimal:
    """Taxa mensal do IGP-M via SGS do Banco Central (série 189, fonte FGV).

    Args:
        data: Qualquer data dentro do mês desejado.

    Returns:
        Decimal: Variação mensal em decimal com quatro casas decimais, ou
        ``Decimal('NaN')`` se não disponível.

    Examples:
        >>> taxa = yd.igpm.taxa("15-01-2025")
        >>> taxa
        Decimal('0.0027')

        Qualquer dia seleciona a competência mensal. Uma taxa negativa
        representa deflação, como os -0,34% de março de 2025:

        >>> yd.igpm.taxa("31-03-2025")
        Decimal('-0.0034')
    """
    if any_is_empty(data):
        return Decimal("NaN")
    df = igpm_taxa_serie(data, data)
    if df.is_empty():
        return Decimal("NaN")
    return df.item(0, "taxa")


# ── SELIC Over ───────────────────────────────────────────────────────


def selic_over_serie(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    *,
    ultimos: int | None = None,
    anualizada: bool = True,
) -> pl.DataFrame:
    """Taxa SELIC Over anualizada ou diária via SGS do Banco Central.

    Taxa de juros média diária praticada no mercado interbancário,
    com títulos públicos como garantia. Consulta a série SGS 1178 para
    a taxa anualizada em base 252 ou a série SGS 11 para a taxa diária.

    Args:
        inicio: Data inicial.
        fim: Data final. Se ``None``, usa a data mais recente.
        ultimos: Número de registros mais recentes a retornar.
            Mutuamente exclusivo com ``inicio``/``fim``.
        anualizada: Se ``True``, retorna a taxa ao ano, em base 252 dias úteis.
            Se ``False``, retorna a taxa ao dia útil publicada pela fonte.

    Returns:
        DataFrame com colunas data e taxa (decimal), ou DataFrame vazio.

    Output Columns:
        data (pl.Date): Data de referência da observação.
        taxa (pl.Decimal(18, 4) ou pl.Decimal(18, 8)): Taxa em decimal, ao ano
            com escala 4 se ``anualizada=True`` ou ao dia útil com escala 8
            se ``anualizada=False``.

    Notes:
        A frequência das observações é diária nas duas modalidades. Os valores
        percentuais da série escolhida são convertidos diretamente do texto
        para Decimal e divididos por 100, sem passagem por float e sem
        recalcular uma modalidade a partir da outra. As escalas preservam
        as duas casas percentuais das taxas anuais e as seis das taxas diárias.

    Examples:
        >>> import pyield as yd
        >>> # Copom de 29-01-2025: over subiu de 12,15% para 13,15% a.a.
        >>> yd.selic.over_serie("28-01-2025", "31-01-2025")
        shape: (4, 2)
        ┌────────────┬───────────────┐
        │ data       ┆ taxa          │
        │ ---        ┆ ---           │
        │ date       ┆ decimal[18,4] │
        ╞════════════╪═══════════════╡
        │ 2025-01-28 ┆ 0.1215        │
        │ 2025-01-29 ┆ 0.1215        │
        │ 2025-01-30 ┆ 0.1315        │
        │ 2025-01-31 ┆ 0.1315        │
        └────────────┴───────────────┘

        Taxas ao dia útil para o mesmo intervalo, com oito casas decimais:

        >>> yd.selic.over_serie("28-01-2025", "31-01-2025", anualizada=False)
        shape: (4, 2)
        ┌────────────┬───────────────┐
        │ data       ┆ taxa          │
        │ ---        ┆ ---           │
        │ date       ┆ decimal[18,8] │
        ╞════════════╪═══════════════╡
        │ 2025-01-28 ┆ 0.00045513    │
        │ 2025-01-29 ┆ 0.00045513    │
        │ 2025-01-30 ┆ 0.00049037    │
        │ 2025-01-31 ┆ 0.00049037    │
        └────────────┴───────────────┘
    """
    serie = SerieSGS.SELIC_OVER if anualizada else SerieSGS.SELIC_DIARIA
    return _converter_para_taxa(
        _buscar_serie(serie, inicio, fim, ultimos), anualizada=anualizada
    )


def selic_over(data: DateLike, *, anualizada: bool = True) -> Decimal:
    """Taxa SELIC Over para uma data específica.

    Fonte: SGS do Banco Central, série 1178 para a taxa anualizada em base 252
    ou série 11 para a taxa diária publicada pela fonte.

    Args:
        data: Data da consulta.
        anualizada: Se ``True``, retorna a taxa ao ano, em base 252 dias úteis.
            Se ``False``, retorna a taxa ao dia útil publicada pela fonte.

    Returns:
        Decimal: Taxa SELIC Over com quatro casas decimais ao ano ou oito
        ao dia útil, conforme ``anualizada``, ou ``Decimal('NaN')`` se
        não disponível.

    Examples:
        >>> import pyield as yd
        >>> taxa = yd.selic.over("31-05-2024")  # decimal (0.104 = 10,4% a.a.)
        >>> taxa
        Decimal('0.1040')
        >>> f"{taxa:.1%}"
        '10.4%'

        Taxa ao dia útil para a mesma data, com oito casas decimais:

        >>> yd.selic.over("31-05-2024", anualizada=False)
        Decimal('0.00039270')
    """
    if any_is_empty(data):
        return Decimal("NaN")
    return _extrair_escalar(
        selic_over_serie(data, data, anualizada=anualizada), "taxa"
    )


# ── SELIC Meta ───────────────────────────────────────────────────────


def selic_meta_serie(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    *,
    ultimos: int | None = None,
) -> pl.DataFrame:
    """Taxa SELIC Meta (série SGS 432).

    Taxa de juros oficial definida pelo COPOM. Fonte: Banco Central do Brasil
    (BCB), série SGS 432, disponível desde 05/03/1999.

    Args:
        inicio: Data inicial.
        fim: Data final. Se ``None``, usa a data mais recente.
        ultimos: Número de registros mais recentes a retornar.
            Mutuamente exclusivo com ``inicio``/``fim``.

    Returns:
        DataFrame com colunas data e taxa (decimal), ou DataFrame vazio.

    Output Columns:
        * data (Date): data de referência da taxa.
        * taxa (pl.Decimal(18, 4)): taxa Selic Meta anual em formato decimal.

    Notes:
        Consultas iniciadas antes de 05/03/1999 consideram automaticamente essa
        data como início da série. Intervalos inteiramente anteriores retornam um
        DataFrame vazio.
        Os percentuais são convertidos diretamente do texto para Decimal e
        divididos por 100, sem passagem por float, com quatro casas decimais,
        preservando as duas casas percentuais publicadas.

    Examples:
        >>> import pyield as yd
        >>> # Copom de 29-01-2025: meta subiu de 12,25% para 13,25% a.a.
        >>> yd.selic.meta_serie("28-01-2025", "31-01-2025")
        shape: (4, 2)
        ┌────────────┬───────────────┐
        │ data       ┆ taxa          │
        │ ---        ┆ ---           │
        │ date       ┆ decimal[18,4] │
        ╞════════════╪═══════════════╡
        │ 2025-01-28 ┆ 0.1225        │
        │ 2025-01-29 ┆ 0.1225        │
        │ 2025-01-30 ┆ 0.1325        │
        │ 2025-01-31 ┆ 0.1325        │
        └────────────┴───────────────┘
    """
    return _converter_para_taxa(
        _buscar_serie(SerieSGS.SELIC_META, inicio, fim, ultimos)
    )


def selic_meta(data: DateLike) -> Decimal:
    """Taxa SELIC Meta para uma data específica.

    Fonte: SGS do Banco Central, série 432.

    Args:
        data: Data da consulta.

    Returns:
        Decimal: Taxa SELIC Meta em formato decimal com quatro casas decimais ou
        ``Decimal('NaN')`` se não disponível, inclusive para datas anteriores
        a 05/03/1999.

    Examples:
        >>> import pyield as yd
        >>> taxa = yd.selic.meta("31-05-2024")  # decimal (0.105 = 10,5% a.a.)
        >>> f"{taxa:.1%}"
        '10.5%'
    """
    if any_is_empty(data):
        return Decimal("NaN")
    return _extrair_escalar(selic_meta_serie(data, data), "taxa")


# ── PTAX ─────────────────────────────────────────────────────────────


def ptax_serie(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    *,
    ultimos: int | None = None,
) -> pl.DataFrame:
    """Cotação PTAX de venda do dólar (série SGS 1).

    Fonte: Banco Central do Brasil (BCB). Frequência diária.

    A cotação retornada é a PTAX de **venda**, a taxa de câmbio
    oficial do dólar no Brasil, usada como referência para
    liquidação de contratos, marcação a mercado de derivativos,
    conversão de DV01 e índices de fundos cambiais.

    Args:
        inicio: Data inicial.
        fim: Data final. Se ``None``, usa a data mais recente.
        ultimos: Número de registros mais recentes a retornar.
            Mutuamente exclusivo com ``inicio``/``fim``.

    Returns:
        DataFrame com colunas data e cotacao, ou DataFrame vazio
        se não houver dados.

    Output Columns:
        * data (Date): data da cotação.
        * cotacao (pl.Decimal(18, 4)): cotação PTAX de venda em R$/US$.

    Notes:
        Disponível desde 28.11.1984. Refere-se às taxas administradas até
        março de 1990 e às taxas livres a partir de então (Resolução 1690,
        de 18.3.1990). A partir de março de 1992, essa taxa recebeu a
        denominação PTAX. Desde 1 de julho de 2011 (Circular 3506), a PTAX
        corresponde à média aritmética das taxas obtidas em quatro consultas
        diárias aos dealers de câmbio.
        Os valores são convertidos diretamente do texto para Decimal, sem
        passagem por float, com quatro casas decimais.

    Examples:
        >>> import pyield as yd
        >>> yd.ptax_serie("20-04-2025", "25-04-2025")
        shape: (4, 2)
        ┌────────────┬───────────────┐
        │ data       ┆ cotacao       │
        │ ---        ┆ ---           │
        │ date       ┆ decimal[18,4] │
        ╞════════════╪═══════════════╡
        │ 2025-04-22 ┆ 5.7496        │
        │ 2025-04-23 ┆ 5.6880        │
        │ 2025-04-24 ┆ 5.6738        │
        │ 2025-04-25 ┆ 5.6846        │
        └────────────┴───────────────┘
    """
    df = _buscar_serie(SerieSGS.PTAX_VENDA, inicio, fim, ultimos)
    if df.is_empty():
        return pl.DataFrame(schema=ESQUEMA_PTAX)
    return df.select(
        "data",
        cotacao=pl.col("valor").cast(_TIPO_PTAX),
    )


def ptax(data: DateLike) -> Decimal:
    """Cotação PTAX de venda para uma data específica.

    Retorna a PTAX de venda, taxa de câmbio oficial do dólar no
    Brasil, via SGS do Banco Central (série 1).

    Args:
        data: Data desejada.

    Returns:
        Decimal: Cotação PTAX de venda em R$/US$ com quatro casas decimais, ou
        ``Decimal('NaN')`` se não houver cotação (feriado, fim de semana ou
        data futura).

    Examples:
        >>> import pyield as yd
        >>> yd.ptax("22-04-2025")
        Decimal('5.7496')

        >>> yd.ptax("20-04-2025")
        Decimal('NaN')
    """
    if any_is_empty(data):
        return Decimal("NaN")
    data = converter_datas(data)
    if not data_referencia_valida(data):
        return Decimal("NaN")

    return _extrair_escalar(ptax_serie(data, data), "cotacao")


# ── DI Over ─────────────────────────────────────────────────────────


def di_over_serie(
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    *,
    ultimos: int | None = None,
    anualizada: bool = True,
) -> pl.DataFrame:
    """Obtém a série de taxas DI Over via SGS do Banco Central.

    Consulta a taxa DI (Depósito Interfinanceiro) da B3/CETIP, republicada
    pelo Banco Central na série SGS 4389 (anualizada em base 252) ou
    SGS 12 (diária).

    Args:
        inicio: Data inicial do intervalo, inclusive.
        fim: Data final do intervalo, inclusive. Se ``None``, usa a data
            atual no Brasil.
        ultimos: Número de registros mais recentes a retornar. Se informado,
            tem prioridade sobre ``inicio`` e ``fim``.
        anualizada: Se ``True``, retorna a taxa ao ano, em base 252 dias úteis.
            Se ``False``, retorna a taxa ao dia útil publicada pela fonte.

    Returns:
        DataFrame com as taxas em decimal, ou vazio se não houver dados.

    Output Columns:
        data (pl.Date): Data de referência da observação.
        taxa (pl.Decimal(18, 4) ou pl.Decimal(18, 8)): Taxa em decimal, ao ano
            com escala 4 se ``anualizada=True`` ou ao dia útil com escala 8
            se ``anualizada=False``.

    Notes:
        A frequência das observações é diária nas duas modalidades. Os valores
        percentuais da série escolhida são convertidos diretamente do texto
        para Decimal e divididos por 100, sem passagem por float e sem
        recalcular uma modalidade a partir da outra. As escalas preservam
        as duas casas percentuais das taxas anuais e as seis das taxas diárias.
        O histórico começa em 06/03/1986. Datas sem observações são omitidas.
        Intervalos longos são divididos automaticamente em blocos seguros
        para consulta ao SGS.

    Raises:
        ValueError: Se não informar ``inicio`` nem ``ultimos``.

    Examples:
        >>> yd.di_over_serie("27-02-2025", "28-02-2025")
        shape: (2, 2)
        ┌────────────┬───────────────┐
        │ data       ┆ taxa          │
        │ ---        ┆ ---           │
        │ date       ┆ decimal[18,4] │
        ╞════════════╪═══════════════╡
        │ 2025-02-27 ┆ 0.1315        │
        │ 2025-02-28 ┆ 0.1315        │
        └────────────┴───────────────┘

        Taxas ao dia útil para o mesmo intervalo, com oito casas decimais:

        >>> yd.di_over_serie("27-02-2025", "28-02-2025", anualizada=False)
        shape: (2, 2)
        ┌────────────┬───────────────┐
        │ data       ┆ taxa          │
        │ ---        ┆ ---           │
        │ date       ┆ decimal[18,8] │
        ╞════════════╪═══════════════╡
        │ 2025-02-27 ┆ 0.00049037    │
        │ 2025-02-28 ┆ 0.00049037    │
        └────────────┴───────────────┘
    """
    serie = SerieSGS.DI_OVER if anualizada else SerieSGS.DI_DIARIA
    return _converter_para_taxa(
        _buscar_serie(serie, inicio, fim, ultimos), anualizada=anualizada
    )


def di_over(data: DateLike, *, anualizada: bool = True) -> Decimal:
    """Obtém a taxa DI over anualizada ou diária via SGS do Banco Central.

    Consulta a taxa DI (Depósito Interfinanceiro) da B3/CETIP, republicada
    pelo Banco Central na série SGS 4389 (anualizada em base 252) ou
    SGS 12 (diária), para a data informada.
    Use ``pyield.di_over`` na API pública principal.

    Args:
        data: data da consulta para buscar a taxa DI.
        anualizada: Se ``True``, retorna a taxa ao ano, em base 252 dias úteis.
            Se ``False``, retorna a taxa ao dia útil publicada pela fonte.

    Returns:
        Decimal: Taxa DI em formato decimal com quatro casas decimais ao ano se
        ``anualizada=True`` (0.1315 = 13,15% a.a.) ou oito casas ao dia útil se
        ``anualizada=False``. Retorna ``Decimal('NaN')`` se não houver dado
        para a data, incluindo feriados e fins de semana.

    Notes:
        As duas séries têm frequência diária. Por padrão, os valores são taxas
        anuais em base 252 dias úteis. O percentual informado pela série
        escolhida é convertido diretamente do texto para Decimal e dividido
        por 100, sem passagem por float, com quatro casas decimais na taxa anual
        e oito na diária. As escalas preservam as duas casas percentuais anuais
        e as seis diárias, sem recalcular uma modalidade a partir da outra.
        O histórico começa em 06/03/1986;
        a consulta anterior via FTP estava limitada a datas a partir de 20/08/2012.

    Examples:
        >>> import pyield as yd
        >>> taxa = yd.di_over("28/02/2025")  # decimal (0.1315 = 13,15% a.a.)
        >>> taxa
        Decimal('0.1315')
        >>> f"{taxa:.2%}"
        '13.15%'

        Taxa ao dia útil para a mesma data, com oito casas decimais:

        >>> yd.di_over("28/02/2025", anualizada=False)
        Decimal('0.00049037')

        >>> yd.di_over("01/01/2025")  # Feriado
        Decimal('NaN')
    """
    if any_is_empty(data):
        return Decimal("NaN")
    return _extrair_escalar(
        di_over_serie(data, data, anualizada=anualizada),
        "taxa",
    )
