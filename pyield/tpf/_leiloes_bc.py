"""Leilões de TPFs publicados pelo BCB (fonte alternativa ao Tesouro Nacional).

Documentação da API:
    https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/aplicacao#!/recursos/leiloesTitulosPublicos
"""

import datetime as dt

import polars as pl

from pyield import du
from pyield._internal import converters as cv
from pyield._internal.br_numbers import float_br, taxa_br
from pyield._internal.types import DateLike, DatesLike, any_is_empty
from pyield.bc._olinda import buscar_csv, montar_url, parsear_csv
from pyield.tpf._leiloes_tn import _enriquecer, _validar_periodo

URL_BASE_API = "https://olinda.bcb.gov.br/olinda/servico/leiloes_selic/versao/v1/odata/leiloesTitulosPublicos(dataMovimentoInicio=@dataMovimentoInicio,dataMovimentoFim=@dataMovimentoFim,dataLiquidacao=@dataLiquidacao,codigoTitulo=@codigoTitulo,dataVencimento=@dataVencimento,edital=@edital,tipoPublico=@tipoPublico,tipoOferta=@tipoOferta)?"

MAPA_TITULOS = {
    100000: "LTN",
    210100: "LFT",
    760199: "NTN-B",
    950199: "NTN-F",
}

# Antes desta data (até 10/06/2024), o BCB publica cotação (% do VNA), e não PU,
# para LFT e NTN-B.
DATA_INICIO_PU = dt.date(2024, 6, 11)

ORDEM_FINAL_COLUNAS = [
    "data_1v",
    "data_liquidacao_1v",
    "numero_edital",
    "tipo_leilao",
    "tipo_publico",
    "titulo",
    "codigo_selic",
    "data_vencimento",
    "dias_uteis",
    "dias_corridos",
    "duration",
    "prazo_medio",
    "quantidade_ofertada_1v",
    "quantidade_ofertada_2v",
    "quantidade_aceita_1v",
    "quantidade_aceita_2v",
    "quantidade_aceita_total",
    "quantidade_liquidada_1v",
    "quantidade_liquidada_2v",
    "financeiro_ofertado_1v",
    "financeiro_ofertado_2v",
    "financeiro_ofertado_total",
    "financeiro_aceito_1v",
    "financeiro_aceito_2v",
    "financeiro_aceito_total",
    "colocacao_1v",
    "colocacao_2v",
    "colocacao_total",
    "dv01_1v",
    "dv01_2v",
    "dv01_total",
    "ptax",
    "dv01_1v_usd",
    "dv01_2v_usd",
    "dv01_total_usd",
    "pu_minimo",
    "pu_medio",
    "taxa_media",
    "taxa_maxima",
]


def _buscar_csv(inicio: dt.date | None, fim: dt.date | None) -> bytes:
    parametros = {
        "dataMovimentoInicio": inicio.isoformat() if inicio else "",
        "dataMovimentoFim": fim.isoformat() if fim else "",
    }
    return buscar_csv(montar_url(URL_BASE_API, parametros))


def _financeiro_aceito(coluna_quantidade: str) -> pl.Expr:
    """Financeiro aceito de uma volta: quantidade vezes PU médio.

    Sem quantidade aceita o financeiro é zero, mesmo quando não há PU.
    """
    quantidade = pl.col(coluna_quantidade)
    return (
        pl.when(quantidade == 0)
        .then(0.0)
        .otherwise((quantidade * pl.col("pu_medio")).round(2))
    )


def _processar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Converte o CSV bruto do BCB para os nomes e unidades de ``leiloes``."""
    cotacao_sem_pu = (pl.col("data_1v") < DATA_INICIO_PU) & pl.col("titulo").is_in(
        ["LFT", "NTN-B"]
    )
    sem_aceite = pl.col("quantidade_aceita_1v") == 0

    return (
        df.filter(pl.col("ofertante") == "Tesouro Nacional")
        .select(
            data_1v=pl.col("dataMovimento").str.to_date("%Y-%m-%d %H:%M:%S"),
            data_liquidacao_1v=pl.col("dataLiquidacao").str.to_date(
                "%Y-%m-%d %H:%M:%S"
            ),
            numero_edital=pl.col("edital").cast(pl.Int64),
            tipo_leilao=pl.col("tipoOferta"),
            tipo_publico=pl.col("tipoPublico"),
            titulo=pl.col("codigoTitulo")
            .cast(pl.Int64)
            .replace_strict(MAPA_TITULOS, return_dtype=pl.String),
            codigo_selic=pl.col("codigoTitulo").cast(pl.Int64),
            data_vencimento=pl.col("dataVencimento").str.to_date("%Y-%m-%d %H:%M:%S"),
            quantidade_ofertada_1v=pl.col("quantidadeOfertada").cast(pl.Int64),
            quantidade_ofertada_2v=pl.col("quantidadeOfertadaSegundaRodada").cast(
                pl.Int64
            ),
            quantidade_aceita_1v=pl.col("quantidadeAceita").cast(pl.Int64),
            quantidade_aceita_2v=pl.col("quantidadeAceitaSegundaRodada").cast(pl.Int64),
            quantidade_liquidada_1v=pl.col("quantidadeLiquidada").cast(pl.Int64),
            quantidade_liquidada_2v=pl.col("quantidadeLiquidadaSegundaRodada").cast(
                pl.Int64
            ),
            financeiro=float_br("financeiro") * 1_000_000,
            pu_minimo=float_br("cotacaoCorte"),
            pu_medio=float_br("cotacaoMedia"),
            taxa_media=taxa_br("taxaMedia"),
            taxa_maxima=taxa_br("taxaCorte"),
        )
        .with_columns(
            quantidade_ofertada_total=pl.sum_horizontal(
                "quantidade_ofertada_1v", "quantidade_ofertada_2v"
            ),
            quantidade_aceita_total=pl.sum_horizontal(
                "quantidade_aceita_1v", "quantidade_aceita_2v"
            ),
            dias_uteis=du.contar_expr("data_liquidacao_1v", "data_vencimento"),
            dias_corridos=(
                pl.col("data_vencimento") - pl.col("data_liquidacao_1v")
            ).dt.total_days(),
        )
        .with_columns(
            # O financeiro do BCB vem em R$ milhões com uma casa decimal.
            pu_medio=pl.when(cotacao_sem_pu)
            .then(pl.col("financeiro") / pl.col("quantidade_aceita_total"))
            .otherwise("pu_medio")
            .round(6),
            pu_minimo=pl.when(cotacao_sem_pu).then(None).otherwise("pu_minimo"),
        )
        .with_columns(
            pl.when(sem_aceite)
            .then(None)
            .otherwise(pl.col("pu_minimo", "pu_medio", "taxa_media", "taxa_maxima"))
            .name.keep()
        )
        .with_columns(
            financeiro_aceito_1v=_financeiro_aceito("quantidade_aceita_1v"),
            financeiro_aceito_2v=_financeiro_aceito("quantidade_aceita_2v"),
            financeiro_ofertado_1v=(
                pl.col("quantidade_ofertada_1v") * pl.col("pu_medio")
            ).round(2),
            financeiro_ofertado_2v=(
                pl.col("quantidade_ofertada_2v") * pl.col("pu_medio")
            ).round(2),
            colocacao_1v=pl.col("quantidade_aceita_1v")
            / pl.col("quantidade_ofertada_1v"),
            colocacao_2v=pl.col("quantidade_aceita_2v")
            / pl.col("quantidade_ofertada_2v"),
            colocacao_total=pl.col("quantidade_aceita_total")
            / pl.col("quantidade_ofertada_total"),
        )
        .with_columns(
            financeiro_aceito_total=pl.sum_horizontal(
                "financeiro_aceito_1v", "financeiro_aceito_2v"
            ),
            # Sem PU (leilão sem aceite), o financeiro ofertado é desconhecido:
            # o total fica nulo, e não zero, quando nenhuma volta tem valor.
            financeiro_ofertado_total=pl.when(
                pl.any_horizontal(
                    pl.col(
                        "financeiro_ofertado_1v", "financeiro_ofertado_2v"
                    ).is_not_null()
                )
            ).then(
                pl.sum_horizontal("financeiro_ofertado_1v", "financeiro_ofertado_2v")
            ),
        )
    )


def leiloes_bc(
    *,
    data: DateLike | DatesLike | None = None,
    inicio: DateLike | None = None,
    fim: DateLike | None = None,
) -> pl.DataFrame:
    """Busca resultados de leilões de TPFs no BCB.

    Exposta por ``yd.tpf.leiloes(fonte="bc")``, cuja docstring documenta o
    contrato e as diferenças em relação ao Tesouro Nacional.
    """
    _validar_periodo(data, inicio, fim)

    datas = None
    if data is not None:
        if any_is_empty(data):
            return pl.DataFrame()
        convertidas = cv.converter_datas(data)
        if convertidas is None:
            return pl.DataFrame()
        if isinstance(convertidas, pl.Series):
            if convertidas.null_count() > 0:
                msg = "data deve conter apenas datas válidas."
                raise ValueError(msg)
            datas = sorted(convertidas.to_list())
        else:
            datas = [convertidas]
        data_inicio, data_fim = datas[0], datas[-1]
    else:
        data_inicio = cv.converter_datas(inicio) if inicio is not None else None
        data_fim = cv.converter_datas(fim) if fim is not None else None

    if data_inicio is not None and data_fim is not None and data_inicio > data_fim:
        msg = "inicio deve ser menor ou igual a fim."
        raise ValueError(msg)

    df = parsear_csv(_buscar_csv(data_inicio, data_fim))
    if df.is_empty():
        return pl.DataFrame()

    df = _processar_df(df)
    if datas is not None:
        df = df.filter(pl.col("data_1v").is_in(datas))
    if df.is_empty():
        return pl.DataFrame()

    return (
        _enriquecer(df)
        .select(ORDEM_FINAL_COLUNAS)
        .sort("data_1v", "titulo", "data_vencimento")
    )
