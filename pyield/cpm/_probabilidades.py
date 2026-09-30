"""
probabilidades — Probabilidades implícitas das reuniões do COPOM a partir dos
preços das opções CPM.

O contrato CPM é uma opção europeia cash-or-nothing. No apreçamento neutro ao
risco, o preço de ajuste da B3 em pontos (0–100) representa a probabilidade
implícita de cada cenário de alteração da Selic, descontada pela taxa DI até o
vencimento (Manual de Apreçamento da B3, §3.5).

Convenções de probabilidade
---------------------------
    prob_bruta = preco_ajuste * fator_desconto / 100

            Probabilidade neutra ao risco direta segundo o Manual da B3 §3.5
            (fórmula invertida):

          p_n(K) = PR_n * exp(+n * r_n) / N

      onde
          PR_n  = preco_ajuste ("Preço de Referência" da B3, pontos 0–100)
          N     = 100 (valor nocional fixo)
          n     = dias_uteis / 252 (tempo em anos, convenção de dias úteis)
          r_n   = ln(1 + taxa_di1) (taxa DI1 continuamente capitalizada até o
                  vencimento)
          taxa_di1 = taxa DI1 interpolada por flat-forward entre a data de
                     referência e a data de expiração

      O preco_ajuste de cpm.contratos() é o "Preço de Referência" oficial da B3,
      obtido no endpoint CSV: o preço exibido no painel da B3 ("Probabilidades
      da Taxa Selic Meta") e resultado da metodologia P1/P2/P3/P4 da B3. Pode
      ser nulo para datas com mais de aproximadamente um mês, quando o
      endpoint CSV não está disponível.

      Pode não somar 1,0 por reunião devido ao spread entre oferta e demanda ou
      ao apreçamento P1/P2 da B3. Quando dias_uteis == 0 (o próprio dia da
      reunião), fator_desconto == 1,0 exatamente e prob_bruta se reduz a
      preco_ajuste / 100.

  prob = prob_bruta / soma(prob_bruta) no grupo de data_expiracao
      Normalizada para que cada reunião some exatamente 1,0. Este é o ajuste de
      apreçamento P3 da B3. Use esta coluna para análise de cenários e gráficos.

  prob_acumulada = soma acumulada de prob, ordenada por
      variacao_strike_bps em ordem crescente.

Observações sobre preco_ajuste nulo
-----------------------------------
Às vezes os contratos CPM são listados sem preço de ajuste (não há apreçamento
oficial da B3 para aquele strike na data, ou o endpoint CSV não está disponível
para datas antigas). Strikes com preco_ajuste nulo são excluídos da saída de
probabilidades porque:

  1. Sua contribuição para a distribuição normalizada é indefinida.
  2. ``group_by().agg(sum())`` do Polars retorna 0,0 (e não nulo) para grupos
     totalmente nulos, o que quebraria a invariável ``prob.sum() == 1.0`` por
     reunião.

Como consequência, uma reunião em que TODOS os strikes listados tenham preços
nulos (por exemplo, CPMH25 no dia da reunião do COPOM de janeiro de 2025) não
aparecerá na saída. O ranking_reuniao é, portanto, atribuído somente entre as
reuniões com preços e sempre forma uma sequência consecutiva [1, 2, ..., n].

Observações sobre o DI1
-----------------------
Se não houver taxa DI1 para a data de referência, taxa_di1, fator_desconto e
as colunas de probabilidade ficam nulas. Erros de rede são propagados.
"""

from typing import Literal

import polars as pl

from pyield._internal.types import DateLike
from pyield.cpm._contratos import contratos
from pyield.futuro import di1


def _df_vazio() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "data_referencia": pl.Date,
            "data_fim_reuniao": pl.Date,
            "data_expiracao": pl.Date,
            "ranking_reuniao": pl.Int32,
            "variacao_strike_bps": pl.Int32,
            "dias_uteis": pl.Int32,
            "preco_ajuste": pl.Float64,
            "taxa_di1": pl.Float64,
            "fator_desconto": pl.Float64,
            "prob_bruta": pl.Float64,
            "prob": pl.Float64,
            "prob_acumulada": pl.Float64,
        }
    )


def _adicionar_ranking_reuniao(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona ranking_reuniao: 1 = data_expiracao mais próxima, 2 = seguinte, etc.
    Calculado como dense rank sobre data_expiracao.
    """
    return df.with_columns(
        ranking_reuniao=pl.col("data_expiracao").rank("dense").cast(pl.Int32)
    )


def _adicionar_fatores_desconto(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona as colunas de taxa DI1 e fator de desconto.

    A função usa ``di1.interpolar_taxas()`` de forma vetorizada para buscar
    todas as taxas DI1 em uma única chamada, com uma busca e um interpolador por
    data_referencia única. Em seguida, calcula os fatores de desconto com
    expressões Polars.

    Método de interpolação
    ----------------------
    ``di1.interpolar_taxas()`` implementa o Manual da B3 §1.4.2 — Flat Forward
    252, que interpola de forma log-linear os fatores acumulados do DI1 (valores
    de PU):

        fa_j = (1 + r_j)^(du_j/252)          # fator acumulado no vértice j
        fa_k = (1 + r_k)^(du_k/252)          # fator acumulado no vértice k
        ft   = (du - du_j) / (du_k - du_j)   # fração do intervalo
        r    = (fa_j * (fa_k / fa_j)^ft)^(252/du) - 1

    Isso equivale à interpolação log-linear dos preços de ajuste do DI1
    (PU = 100_000 / (1+r)^(du/252)), daí a expressão "interpolação exponencial
    dos preços de ajuste do DI1" no Manual de Apreçamento do CPM §3.5.

    Não é o método §1.4.1 (Exponencial 252), que interpola diretamente as taxas:
        r = (1 + r_j) * ((1 + r_k)/(1 + r_j))^ft - 1
    Os dois métodos divergem alguns pontos-base em vencimentos intermediários
    (por exemplo, aproximadamente 4,6 pontos-base em du=17 para taxas na faixa
    da Selic em 2026).
    """
    pares = (
        df.select("data_referencia", "data_expiracao", "dias_uteis")
        .unique(subset=["data_referencia", "data_expiracao"])
        .sort("data_referencia", "data_expiracao")
    )

    taxas = di1.interpolar_taxas(
        datas_referencia=pares["data_referencia"],
        datas_vencimento=pares["data_expiracao"],
        extrapolar=True,
    )

    df_desconto = (
        pares.with_columns(taxa_di1=taxas.fill_nan(None))
        .with_columns(
            fator_desconto=(
                (pl.col("dias_uteis") / 252 * (1 + pl.col("taxa_di1")).log()).exp()
            ),
        )
        .select("data_referencia", "data_expiracao", "taxa_di1", "fator_desconto")
    )

    return df.join(df_desconto, on=["data_referencia", "data_expiracao"], how="left")


def _adicionar_probabilidades(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona as colunas prob_bruta, prob e prob_acumulada conforme o Manual da
    B3 §3.5.

    Assume que o DataFrame já foi filtrado para um tipo de opção e para linhas
    com preco_ajuste não nulo.

    prob_bruta = preco_ajuste * fator_desconto / 100
    prob = prob_bruta / soma(prob_bruta) no grupo de data_expiracao
    prob_acumulada = soma acumulada de prob, ordenada por
        variacao_strike_bps em ordem crescente.
    """
    df = _adicionar_fatores_desconto(df)

    return (
        df.sort("data_expiracao", "variacao_strike_bps")
        .with_columns(
            prob_bruta=(pl.col("preco_ajuste") * pl.col("fator_desconto") / 100),
        )
        .with_columns(
            prob=(
                pl.col("prob_bruta") / pl.col("prob_bruta").sum().over("data_expiracao")
            ),
        )
        .with_columns(prob_acumulada=pl.col("prob").cum_sum().over("data_expiracao"))
    )


def probabilidades(
    data: DateLike,
    tipo_opcao: Literal["call", "put"] = "call",
) -> pl.DataFrame:
    """Calcula as probabilidades implícitas das reuniões do COPOM.

    Fonte: B3, a partir dos preços dos contratos CPM (``yd.cpm.contratos``),
    descontados pela curva de DI1. Inclui todas as reuniões com contratos
    negociados na data. Somente strikes com ``preco_ajuste`` não nulo são
    incluídos; reuniões em que todos os strikes têm preço nulo são excluídas.

    Args:
        data: Data de negociação.
        tipo_opcao: Tipo de opção, ``"call"`` ou ``"put"``. O padrão é
            ``"call"``, que na prática é o lado mais líquido.

    Returns:
        DataFrame Polars ordenado por ``ranking_reuniao`` e
        ``variacao_strike_bps``. Retorna DataFrame vazio, com o schema abaixo,
        se não houver dados.

    Output Columns:
        * data_referencia (Date): data de negociação.
        * data_fim_reuniao (Date): último dia da reunião do COPOM.
        * data_expiracao (Date): data de expiração do contrato CPM.
        * ranking_reuniao (Int32): 1 para a reunião mais próxima com preços,
            2 para a seguinte, e assim por diante.
        * variacao_strike_bps (Int32): variação do strike em pontos-base,
            crescente dentro de cada reunião.
        * dias_uteis (Int32): dias úteis até a expiração.
        * preco_ajuste (Float64): Preço de Referência da B3 em pontos (0–100).
        * taxa_di1 (Float64): taxa DI1 interpolada até a expiração.
        * fator_desconto (Float64): fator aplicado na inversão da fórmula da
            B3, ``exp(dias_uteis / 252 * ln(1 + taxa_di1))``.
        * prob_bruta (Float64): probabilidade antes da normalização,
            ``preco_ajuste * fator_desconto / 100``.
        * prob (Float64): probabilidade normalizada,
            ``prob_bruta / soma(prob_bruta)`` por ``data_expiracao``, com soma
            1,0 por reunião.
        * prob_acumulada (Float64): probabilidade acumulada em ordem crescente
            de strike.

    Notes:
        Para uma única reunião, filtre o resultado, por exemplo
        ``df.filter(pl.col("ranking_reuniao") == 1)`` para a mais próxima.

    Examples:
        >>> df = yd.cpm.probabilidades("29-01-2025")  # doctest: +SKIP
        >>> proxima = df.filter(pl.col("ranking_reuniao") == 1)  # doctest: +SKIP
    """
    df = (
        contratos(data)
        .filter(
            pl.col("tipo_opcao") == tipo_opcao,
            pl.col("preco_ajuste").is_not_null(),
        )
        .pipe(_adicionar_ranking_reuniao)
    )
    if df.is_empty():
        return _df_vazio()

    return (
        df.pipe(_adicionar_probabilidades)
        .select(_df_vazio().columns)
        .sort("ranking_reuniao", "variacao_strike_bps")
    )
