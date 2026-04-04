# ruff: noqa: I001

import pandas as pd
import streamlit_functions.config as cfg  # pyright: ignore[reportMissingImports]

from pyield.dus import deslocar
from pyield.tn import ntnb

TAXA_REINVESTIMENTO_CUPOM = (1.06) ** (1 / 2) - 1
TOLERANCIA_CHECAGEM = 0.0001


def _obter_vna_pagamento(data_pagamento):
    try:
        return cfg.df_vna_base.query("reference_date == @data_pagamento")["vna"].values[
            0
        ]
    except IndexError:
        data_anterior = cfg.df_vna_base[
            cfg.df_vna_base["reference_date"] <= data_pagamento
        ]["reference_date"].max()
        print(f"Usando VNA de {data_anterior} para pagamento em {data_pagamento}")
        return cfg.df_vna.query("reference_date == @data_anterior")["vna_du"].values[0]


def _ajustar_data(data):
    return deslocar(data, 0).date()


def _gerar_datas_calculo(data_inicial, data_final, df_pagamentos):
    if df_pagamentos.empty:
        datas_calculo = [data_inicial, data_final]
    else:
        df_pagamentos["data_pagamento"] = pd.to_datetime(
            df_pagamentos["data_pagamento"]
        ).dt.date
        datas_calculo = [data_inicial]
        datas_calculo.extend(df_pagamentos["data_pagamento"].tolist())
        datas_calculo.append(data_final)

    datas_calculo.sort(reverse=True)
    return datas_calculo


def _calcular_componentes_periodo(
    data_inicio_cupons,
    data_fim_cupons,
    data_vencimento,
    cupons_a_adicionar,
):
    vna_inicio = cfg.df_vna.query("reference_date == @data_inicio_cupons")[
        "vna_du"
    ].values[0]
    vna_fim = cfg.df_vna.query("reference_date == @data_fim_cupons")["vna_du"].values[0]

    taxa_inicio = cfg.df_ntnb.query(
        "ReferenceDate == @data_inicio_cupons and MaturityDate == @data_vencimento"
    )["IndicativeRate"].values[0]
    taxa_fim = cfg.df_ntnb.query(
        "ReferenceDate == @data_fim_cupons and MaturityDate == @data_vencimento"
    )["IndicativeRate"].values[0]

    cotacao_inicio = (
        ntnb.cotacao(data_inicio_cupons, data_vencimento, taxa_inicio) / 100
    )
    cotacao_fim = (
        ntnb.cotacao(data_fim_cupons, data_vencimento, taxa_fim) / 100
        + cupons_a_adicionar
    )
    cotacao_hibrida = (
        ntnb.cotacao(data_fim_cupons, data_vencimento, taxa_inicio) / 100
        + cupons_a_adicionar
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


def obter_pagamentos_cupons(data_inicial, data_final, data_vencimento):
    """Obtém os pagamentos de cupons recebidos entre duas datas.

    Args:
        data_inicial: Data inicial do cálculo de retorno.
        data_final: Data final do cálculo de retorno.
        data_vencimento: Data de vencimento da NTN-B.

    Returns:
        DataFrame pandas com os pagamentos de cupom ocorridos no período.
    """
    df_fluxos = (
        ntnb.fluxos_caixa(data_inicial, data_vencimento)
        .to_pandas()
        .rename(columns={"valor_pagamento": "fluxo_caixa"})
    )
    df_fluxos["data_pagamento"] = pd.to_datetime(df_fluxos["data_pagamento"]).dt.date

    df_pagamentos = df_fluxos[
        (df_fluxos["data_pagamento"] > data_inicial)
        & (df_fluxos["data_pagamento"] <= data_final)
    ].copy()

    if df_pagamentos.empty:
        return pd.DataFrame(
            {
                "data_pagamento": [],
                "fluxo_caixa": [],
                "valor_pagamento": [],
            }
        )

    valores_pagamento = []
    for _, linha in df_pagamentos.iterrows():
        vna = _obter_vna_pagamento(linha["data_pagamento"])
        valores_pagamento.append(vna * (linha["fluxo_caixa"] / 100))

    df_pagamentos["valor_pagamento"] = valores_pagamento
    return df_pagamentos


def decompor_retorno_ntnb(data_inicial, data_final, data_vencimento):
    """Decompõe o retorno de uma NTN-B entre duas datas, incluindo cupons.

    Args:
        data_inicial: Data inicial do cálculo de retorno.
        data_final: Data final do cálculo de retorno.
        data_vencimento: Data de vencimento da NTN-B.

    Returns:
        Tupla com os componentes acumulados de inflação, marcação a mercado e
        retorno real, ou ``None`` em caso de falha de checagem.
    """
    df_pagamentos = obter_pagamentos_cupons(data_inicial, data_final, data_vencimento)
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
