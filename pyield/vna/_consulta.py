"""Operações de VNA selecionadas pelo título."""

import datetime as dt
from decimal import Decimal
from typing import Literal

import polars as pl

from pyield._internal.converters import converter_datas
from pyield._internal.types import DateLike, any_is_empty
from pyield.vna import _lft, _ntnb, _ntnc

TipoTitulo = Literal["LFT", "NTN-B", "NTN-C"]


def _modulo_mensal(titulo: TipoTitulo):
    if titulo == "NTN-B":
        return _ntnb
    if titulo == "NTN-C":
        return _ntnc
    raise ValueError("Esta operação aceita apenas NTN-B e NTN-C.")


def valor(
    titulo: TipoTitulo,
    data: DateLike | None = None,
    vencimento: DateLike | None = None,
) -> Decimal:
    """Consulta o VNA do título na data informada.

    Args:
        titulo: LFT, NTN-B ou NTN-C.
        data: Data de referência. Se nula, retorna ``Decimal('NaN')``.
        vencimento: Necessário para selecionar a série da NTN-C. Não se
            aplica aos demais títulos.

    Returns:
        Decimal: VNA com seis casas ou ``Decimal('NaN')`` na ausência de dados,
            conforme o contrato da consulta específica do título.

    Notes:
        Fontes: Banco Central (arquivo diário SELIC) para LFT; Tesouro
        Nacional para NTN-B e NTN-C. Entre referências, NTN-B usa os
        números-índice do IPCA do IBGE, e NTN-C usa a razão entre os VNAs.
        Não realiza projeção automática. NTN-C sem vencimento retorna NaN.

    Raises:
        ValueError: Se o título for inválido ou o vencimento for informado
            para um título diferente de NTN-C.
    """
    if titulo not in {"LFT", "NTN-B", "NTN-C"}:
        raise ValueError("Título deve ser LFT, NTN-B ou NTN-C.")
    if titulo == "NTN-C":
        return _ntnc.vna(data, vencimento)
    if vencimento is not None:
        raise ValueError("Vencimento aplica-se apenas à NTN-C.")
    if titulo == "LFT":
        return _lft.vna(data)
    return _ntnb.vna(data)


def historico(
    titulo: TipoTitulo,
    vencimento: DateLike | None = None,
) -> pl.DataFrame:
    """Busca os VNAs mensais publicados pelo Tesouro Nacional.

    Args:
        titulo: NTN-B ou NTN-C. Histórico de LFT não está disponível.
        vencimento: Filtro opcional de série para NTN-C. Se omitido, retorna
            todas as séries. Não se aplica à NTN-B.

    Returns:
        DataFrame Polars ordenado por data. Sem série correspondente ao
            vencimento, retorna DataFrame vazio com o mesmo schema.

    Output Columns:
        - data (Date): Data de referência publicada.
        - vna (Float64): Valor nominal atualizado publicado.
        - anos_vencimento (List[Int64]): Apenas NTN-C; anos da série.

    Notes:
        Fonte: planilhas de Valor Nominal de NTN-B e NTN-C do Tesouro
        Transparente. Preserva o schema das consultas específicas por título.

    Raises:
        ValueError: Se o título não for NTN-B ou NTN-C, ou se houver filtro
            de vencimento para NTN-B.
    """
    modulo = _modulo_mensal(titulo)
    if vencimento is not None and titulo != "NTN-C":
        raise ValueError("Vencimento aplica-se apenas à NTN-C.")
    ano = None
    if vencimento is not None and not any_is_empty(vencimento):
        ano = converter_datas(vencimento).year
    df = modulo.vnas()
    if ano is not None:
        df = df.filter(pl.col("anos_vencimento").list.contains(ano))
    return df.sort("data")


def ultimo(
    titulo: TipoTitulo,
    vencimento: DateLike | None = None,
) -> pl.DataFrame:
    """Busca a última referência publicada de cada série de VNA.

    Args:
        titulo: NTN-B ou NTN-C. Última publicação de LFT não está disponível.
        vencimento: Filtro opcional da NTN-C. Sem filtro, retorna a última
            referência de cada série. Não se aplica à NTN-B.

    Returns:
        DataFrame Polars com data e valor da última publicação de cada série.
            Retorna vazio quando o histórico consultado estiver vazio.

    Output Columns:
        - data (Date): Data de referência da última publicação da série.
        - vna (Float64): VNA publicado nessa referência.
        - anos_vencimento (List[Int64]): Apenas NTN-C; anos da série.

    Notes:
        Fonte: históricos do Tesouro Nacional obtidos por ``historico``.
        O resultado é publicado, sem projeção até a data atual. Preserva
        as colunas e os tipos do histórico.
    """
    df = historico(titulo, vencimento)
    if titulo == "NTN-C":
        return df.unique(subset="anos_vencimento", keep="last", maintain_order=True)
    return df.tail(1)


def vigencia(titulo: TipoTitulo, data: DateLike) -> tuple[dt.date, dt.date]:
    """Obtém o intervalo mensal de atualização do VNA, sem acesso à rede.

    Args:
        titulo: NTN-B ou NTN-C; LFT não possui essa vigência mensal.
        data: Data contida na vigência.

    Returns:
        tuple[date, date]: Início inclusivo e fim exclusivo, sem ajuste útil.
            NTN-B usa dia 15 a dia 15; NTN-C usa primeiro dia a primeiro dia.

    Notes:
        Segue os calendários de atualização do Tesouro Nacional implementados
        nas funções específicas de cada título.

    Raises:
        ValueError: Se o título não for NTN-B ou NTN-C ou a data for inválida.

    Examples:
        >>> yd.vna.vigencia("NTN-B", "15-07-2026")
        (datetime.date(2026, 7, 15), datetime.date(2026, 8, 15))
    """
    return _modulo_mensal(titulo).vigencia(data)


def projetado(
    titulo: TipoTitulo,
    data: DateLike,
    vna_base: float | Decimal,
    inflacao: float | Decimal,
) -> Decimal:
    """Projeta o VNA mensal com base e inflação fornecidas pelo consumidor.

    Args:
        titulo: NTN-B ou NTN-C. Projeção de LFT não está disponível.
        data: Data para a qual projetar o VNA.
        vna_base: VNA no início da vigência que contém a data.
        inflacao: Variação mensal percentual; 0.45 representa 0,45%.

    Returns:
        Decimal: VNA projetado com seis casas; NaN para entradas nulas.

    Notes:
        Segue a metodologia STN das funções de projeção de NTN-B e NTN-C:
        base truncada em seis casas, inflação arredondada em duas e expoente
        em dias corridos truncado em catorze. Não busca projeções externas.

    Raises:
        ValueError: Se o título não for NTN-B ou NTN-C, a base não for positiva
            ou a inflação for menor ou igual a -100%.
    """
    return _modulo_mensal(titulo).vna_projetado(data, vna_base, inflacao)
