import polars as pl

from pyield import du
from pyield._internal.types import DateLike
from pyield.tn import ntnf, utils


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
        >>> yd.tpf.curva_pre("18-06-2025")
        shape: (17, 3)
        ┌─────────────────┬────────────┬───────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_zero │
        │ ---             ┆ ---        ┆ ---       │
        │ date            ┆ i64        ┆ f64       │
        ╞═════════════════╪════════════╪═══════════╡
        │ 2025-07-01      ┆ 8          ┆ 0.14835   │
        │ 2025-10-01      ┆ 74         ┆ 0.147463  │
        │ 2026-01-01      ┆ 138        ┆ 0.147752  │
        │ 2026-04-01      ┆ 199        ┆ 0.147947  │
        │ 2026-07-01      ┆ 260        ┆ 0.147069  │
        │ …               ┆ …          ┆ …         │
        │ 2030-01-01      ┆ 1135       ┆ 0.137279  │
        │ 2031-01-01      ┆ 1387       ┆ 0.138154  │
        │ 2032-01-01      ┆ 1639       ┆ 0.13876   │
        │ 2033-01-01      ┆ 1891       ┆ 0.1393    │
        │ 2035-01-01      ┆ 2390       ┆ 0.141068  │
        └─────────────────┴────────────┴───────────┘
    """
    # Busca dados de LTN (zero cupom)
    df_ltn = utils.obter_tpf(data, "LTN").select("data_vencimento", "taxa_indicativa")

    # Busca dados de NTN-F (com cupom)
    df_ntnf = utils.obter_tpf(data, "NTN-F").select(
        "data_vencimento", "taxa_indicativa"
    )

    # Verifica se há dados para ambos os tipos
    if df_ltn.is_empty() and df_ntnf.is_empty():
        return pl.DataFrame(
            schema={
                "data_vencimento": pl.Date,
                "dias_uteis": pl.Int64,
                "taxa_zero": pl.Float64,
            }
        )

    # Se só há NTN-F, não é possível fazer bootstrap sem LTN
    if df_ltn.is_empty():
        raise ValueError(
            "Não é possível construir a curva PRE sem taxas de LTN para bootstrap"
        )

    # Se só há LTN, retorna direto (LTN já são zero cupom)
    if df_ntnf.is_empty():
        df = _processar_ltn_adicionais(data, df_ltn)
    else:
        # Usa spot_rates de NTN-F para calcular zero cupom
        df_spots = ntnf.taxas_zero(
            data_liquidacao=data,
            ltn_vencimentos=df_ltn["data_vencimento"],
            ltn_taxas=df_ltn["taxa_indicativa"],
            ntnf_vencimentos=df_ntnf["data_vencimento"],
            ntnf_taxas=df_ntnf["taxa_indicativa"],
            incluir_cupons=False,
        )

        # Encontra vencimentos de LTN que não estão no resultado de NTN-F
        ltn_mask = ~df_ltn["data_vencimento"].is_in(
            df_spots["data_vencimento"].to_list()
        )
        ltn_not_in_ntnf = df_ltn.filter(ltn_mask)

        if not ltn_not_in_ntnf.is_empty():
            # Processa vencimentos de LTN adicionais
            ltn_subset = _processar_ltn_adicionais(
                data,
                ltn_not_in_ntnf,
            )

            # Combina LTN e NTN-F
            df = pl.concat([df_spots, ltn_subset])
        else:
            df = df_spots

    # Validação final
    _validar_resultado_final(df)

    # Ordena por vencimento
    return df.sort("data_vencimento")


def _processar_ltn_adicionais(
    data_referencia: DateLike,
    ltn_nao_em_ntnf: pl.DataFrame,
) -> pl.DataFrame:
    """Processa vencimentos de LTN fora do bootstrap de NTN-F."""
    # Calcula dias úteis de forma vetorizada
    dias_uteis = du.contar(data_referencia, ltn_nao_em_ntnf["data_vencimento"])

    # Cria DataFrame de resultado
    return pl.DataFrame(
        {
            "data_vencimento": ltn_nao_em_ntnf["data_vencimento"],
            "dias_uteis": dias_uteis,
            "taxa_zero": ltn_nao_em_ntnf["taxa_indicativa"],
        }
    )


def _validar_resultado_final(df: pl.DataFrame) -> None:
    """Valida o DataFrame final combinado."""
    if df["dias_uteis"].is_null().any():
        raise ValueError("Resultado final contém NaN na coluna dias_uteis")

    if df["taxa_zero"].is_null().any():
        raise ValueError("Resultado final contém NaN na coluna taxa_zero")
