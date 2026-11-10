"""Leilões de TPFs: escolha explícita entre Tesouro Nacional e BCB."""

from typing import Literal

import polars as pl

from pyield._internal.types import DateLike, DatesLike
from pyield.tpf._leiloes_bc import leiloes_bc
from pyield.tpf._leiloes_tn import leiloes_tn


def leiloes(
    *,
    data: DateLike | DatesLike | None = None,
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
    fonte: Literal["tn", "bc"] = "tn",
) -> pl.DataFrame:
    """Busca resultados de leilões de TPFs.

    Fonte: Tesouro Nacional (``fonte="tn"``, padrão), com dados disponíveis a
    partir de 04/01/2018, ou Banco Central do Brasil (``fonte="bc"``), com
    dados disponíveis desde 12/11/2012. Retorna dados de quantidades,
    financeiros, taxas de colocação, duration e DV01 dos leilões no período
    informado.

    O BCB é uma alternativa para quando o Tesouro estiver indisponível ou ainda
    não trouxer a 2ª volta. As duas fontes recebem os mesmos parâmetros e usam
    os mesmos nomes de colunas; colunas exclusivas de cada fonte ficam ausentes
    na outra. Os resultados podem ser unidos com
    ``pl.concat(..., how="diagonal")``.

    Args:
        data: Data ou sequência de datas do leilão. Padrão é ``None``.
        inicio: Data inicial da consulta. Padrão é ``None``.
        fim: Data final da consulta. Padrão é ``None``.
        fonte: ``"tn"`` (Tesouro Nacional) ou ``"bc"`` (Banco Central do
            Brasil). Padrão é ``"tn"``. Não há troca automática de fonte.

    Returns:
        DataFrame Polars com os dados processados dos leilões. Se ``data`` for
        uma sequência, concatena os resultados das datas informadas. Se
        ``inicio`` for informado, retorna os leilões a partir dessa data. Se
        nenhum filtro temporal for informado, retorna o histórico completo.
        Retorna DataFrame vazio se não houver dados para o período. O resultado
        é ordenado por data, título e vencimento.

    Output Columns:
        * data_1v (Date): data de realização do leilão.
        * data_liquidacao_1v (Date): data de liquidação financeira da 1ª volta.
        * data_liquidacao_2v (Date): data de liquidação financeira da 2ª volta.
        * numero_edital (Int64): número do edital do leilão.
        * tipo_leilao (String): tipo da operação (``"Venda"`` ou ``"Compra"``).
        * tipo_publico (String): público do leilão (exclusiva do BCB).
        * tipo_ocorrencia (String): classificação da ocorrência do leilão.
        * titulo (String): código do título público leiloado.
        * codigo_selic (Int64): código do título no Selic (exclusiva do BCB).
        * benchmark (String): descrição de referência do título.
        * data_vencimento (Date): data de vencimento do título.
        * dias_uteis (Int64): dias úteis entre liquidação e vencimento.
        * dias_corridos (Int64): dias corridos entre liquidação e vencimento.
        * duration (Float64): duration de Macaulay em anos.
        * prazo_medio (Float64): maturidade média em anos.
        * quantidade_ofertada_1v (Int64): quantidade ofertada na 1ª volta.
        * quantidade_ofertada_2v (Int64): quantidade ofertada na 2ª volta.
        * quantidade_aceita_1v (Int64): quantidade aceita na 1ª volta.
        * quantidade_aceita_2v (Int64): quantidade aceita na 2ª volta.
        * quantidade_aceita_total (Int64): quantidade aceita total.
        * quantidade_liquidada_1v (Int64): quantidade liquidada na 1ª volta.
        * quantidade_liquidada_2v (Int64): quantidade liquidada na 2ª volta.
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

    Notes:
        ``data`` não pode ser combinado com ``inicio`` ou ``fim``. ``fim`` só
        pode ser usado junto com ``inicio``.

        Diferenças do BCB em relação ao Tesouro Nacional:

        - O BCB não publica ``data_liquidacao_2v``, ``tipo_ocorrencia``,
          ``benchmark``, ``quantidade_bcb``, ``financeiro_bcb`` nem
          ``tipo_pu_medio``.
        - O financeiro publicado pelo BCB é arredondado a R$ 100 mil. Por
          isso, os financeiros são calculados como quantidade vezes
          ``pu_medio``, o que reproduz o Tesouro na 1ª volta; na 2ª volta, a
          diferença típica é inferior a 0,1%.
        - Na 2ª volta, a quantidade ofertada do BCB é a nominal do edital,
          enquanto o Tesouro pode informar valores ligeiramente menores.
        - Até 10/06/2024, o BCB publica cotação, e não PU, para LFT e NTN-B.
          Nesses casos, ``pu_medio`` é estimado pelo financeiro dividido pela
          quantidade aceita (sujeito ao arredondamento acima) e
          ``pu_minimo`` é nulo.

    Examples:
        >>> df = yd.tpf.leiloes(data="19-08-2025", fonte="bc")
        >>> df.select("titulo", "quantidade_aceita_total", "pu_medio", "taxa_media")
        shape: (5, 4)
        ┌────────┬─────────────────────────┬──────────────┬────────────┐
        │ titulo ┆ quantidade_aceita_total ┆ pu_medio     ┆ taxa_media │
        │ ---    ┆ ---                     ┆ ---          ┆ ---        │
        │ str    ┆ i64                     ┆ f64          ┆ f64        │
        ╞════════╪═════════════════════════╪══════════════╪════════════╡
        │ LFT    ┆ 150000                  ┆ 17149.069465 ┆ 0.000636   │
        │ LFT    ┆ 751003                  ┆ 17072.592044 ┆ 0.001068   │
        │ NTN-B  ┆ 300759                  ┆ 4299.806542  ┆ 0.08167    │
        │ NTN-B  ┆ 500542                  ┆ 4143.249763  ┆ 0.07748    │
        │ NTN-B  ┆ 500000                  ┆ 4021.3613    ┆ 0.07323    │
        └────────┴─────────────────────────┴──────────────┴────────────┘
    """
    if fonte == "tn":
        return leiloes_tn(data=data, inicio=inicio, fim=fim)
    if fonte == "bc":
        return leiloes_bc(data=data, inicio=inicio, fim=fim)
    msg = f"fonte inválida: {fonte!r}. Use 'tn' ou 'bc'."
    raise ValueError(msg)
