from dataclasses import dataclass

import polars as pl

from pyield import ntnb
from pyield.du import deslocar

TAXA_REINVESTIMENTO_CUPOM = (1.06) ** (1 / 2) - 1
TOLERANCIA_CHECAGEM = 0.0001


@dataclass(frozen=True)
class DadosNTNB:
    df_vna_base: pl.DataFrame
    df_vna: pl.DataFrame
    df_ntnb: pl.DataFrame


def _data(coluna: str) -> pl.Expr:
    return pl.col(coluna).cast(pl.Date)


def _obter_vna_pagamento(
    data_pagamento,
    dados: DadosNTNB,
):
    df_vna_base = dados.df_vna_base
    data_referencia = _data("reference_date")
    vna_exato = df_vna_base.filter(data_referencia == data_pagamento)
    if not vna_exato.is_empty():
        return vna_exato["vna"].item()

    data_anterior = (
        df_vna_base.filter(data_referencia <= data_pagamento)
        .select(data_referencia.max())
        .item()
    )
    print(f"Usando VNA de {data_anterior} para pagamento em {data_pagamento}")
    return dados.df_vna.filter(_data("reference_date") == data_anterior)[
        "vna_du"
    ].item()


def _ajustar_data(data):
    return deslocar(data, 0).date()


def _obter_taxa(
    data_referencia,
    data_vencimento,
    dados: DadosNTNB,
):
    df_ntnb = dados.df_ntnb
    filtro = (_data("ReferenceDate") == data_referencia) & (
        _data("MaturityDate") == data_vencimento
    )
    return df_ntnb.filter(filtro)["IndicativeRate"].item()


def _gerar_datas_calculo(data_inicial, data_final, df_pagamentos):
    if df_pagamentos.is_empty():
        datas_calculo = [data_inicial, data_final]
    else:
        datas_calculo = [data_inicial]
        datas_calculo.extend(df_pagamentos["data_pagamento"].to_list())
        datas_calculo.append(data_final)

    datas_calculo.sort(reverse=True)
    return datas_calculo


def _calcular_componentes_periodo(
    data_inicio_cupons,
    data_fim_cupons,
    data_vencimento,
    cupons_a_adicionar,
    dados: DadosNTNB,
):
    df_vna = dados.df_vna
    vna_inicio = df_vna.filter(_data("reference_date") == data_inicio_cupons)[
        "vna_du"
    ].item()
    vna_fim = df_vna.filter(_data("reference_date") == data_fim_cupons)["vna_du"].item()

    taxa_inicio = _obter_taxa(data_inicio_cupons, data_vencimento, dados)
    taxa_fim = _obter_taxa(data_fim_cupons, data_vencimento, dados)

    cotacao_inicio = ntnb.cotacao(data_inicio_cupons, data_vencimento, taxa_inicio)
    cotacao_fim = (
        ntnb.cotacao(data_fim_cupons, data_vencimento, taxa_fim) + cupons_a_adicionar
    )
    cotacao_hibrida = (
        ntnb.cotacao(data_fim_cupons, data_vencimento, taxa_inicio) + cupons_a_adicionar
    )

    retorno_total = ((cotacao_fim * vna_fim) / (cotacao_inicio * vna_inicio)) - 1
    retorno_inflacao = vna_fim / vna_inicio
    retorno_marcacao_mercado = cotacao_fim / cotacao_hibrida
    retorno_taxa_real = cotacao_hibrida / cotacao_inicio
    checagem = (retorno_marcacao_mercado * retorno_taxa_real * retorno_inflacao) - 1

    return (
        retorno_total,
        retorno_inflacao,
        retorno_marcacao_mercado,
        retorno_taxa_real,
        checagem,
    )


def obter_pagamentos_cupons(
    data_inicial,
    data_final,
    data_vencimento,
    *,
    dados: DadosNTNB,
):
    """Obtém os pagamentos de cupons recebidos entre duas datas.

    Args:
        data_inicial: Data inicial do cálculo de retorno.
        data_final: Data final do cálculo de retorno.
        data_vencimento: Data de vencimento da NTN-B.
        dados: Tabelas Polars necessárias ao cálculo, agrupadas em
            ``DadosNTNB``.

    Returns:
        DataFrame Polars com os pagamentos de cupom ocorridos no período.
    """
    df_fluxos = ntnb.fluxos_caixa(data_inicial, data_vencimento).rename(
        {"valor_pagamento": "fluxo_caixa"}
    )
    df_pagamentos = df_fluxos.filter(
        (pl.col("data_pagamento") > data_inicial)
        & (pl.col("data_pagamento") <= data_final)
    )

    if df_pagamentos.is_empty():
        return df_pagamentos.with_columns(
            valor_pagamento=pl.Series([], dtype=pl.Float64)
        )

    return (
        df_pagamentos.with_columns(
            vna=pl.col("data_pagamento").map_elements(
                lambda data: _obter_vna_pagamento(data, dados),
                return_dtype=pl.Float64,
            )
        )
        .with_columns(valor_pagamento=pl.col("vna") * pl.col("fluxo_caixa"))
        .drop("vna")
    )


def decompor_retorno_ntnb(
    data_inicial,
    data_final,
    data_vencimento,
    *,
    dados: DadosNTNB,
):
    """Decompõe o retorno de uma NTN-B entre duas datas, incluindo cupons.

    Args:
        data_inicial: Data inicial do cálculo de retorno.
        data_final: Data final do cálculo de retorno.
        data_vencimento: Data de vencimento da NTN-B.
        dados: Tabelas Polars necessárias ao cálculo, agrupadas em
            ``DadosNTNB``.

    Returns:
        Tupla com os componentes acumulados de inflação, marcação a mercado e
        retorno real, ou ``None`` em caso de falha de checagem.
    """
    df_pagamentos = obter_pagamentos_cupons(
        data_inicial,
        data_final,
        data_vencimento,
        dados=dados,
    )
    datas_calculo = _gerar_datas_calculo(data_inicial, data_final, df_pagamentos)

    retornos_inflacao = []
    retornos_marcacao_mercado = []
    retornos_taxa_real = []

    for indice in range(len(datas_calculo) - 1):
        cupons_a_adicionar = 0 if indice == 0 else TAXA_REINVESTIMENTO_CUPOM

        data_inicio_cupons = _ajustar_data(datas_calculo[indice + 1])
        data_fim_cupons = _ajustar_data(datas_calculo[indice])
        (
            retorno_total,
            retorno_inflacao,
            retorno_marcacao_mercado,
            retorno_taxa_real,
            checagem,
        ) = _calcular_componentes_periodo(
            data_inicio_cupons,
            data_fim_cupons,
            data_vencimento,
            cupons_a_adicionar,
            dados,
        )

        if checagem - retorno_total > TOLERANCIA_CHECAGEM:
            print("Falha na checagem de consistência")
            print(f"Checagem: {checagem}")
            print(f"Retorno total: {retorno_total}")

            return None

        retornos_inflacao.append(retorno_inflacao)
        retornos_marcacao_mercado.append(retorno_marcacao_mercado)
        retornos_taxa_real.append(retorno_taxa_real)

    return retornos_inflacao, retornos_marcacao_mercado, retornos_taxa_real
