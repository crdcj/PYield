"""Parâmetros disponíveis para a vigência mensal do VNA."""

from decimal import Decimal
from typing import Literal

import polars as pl

from pyield import igpm, ipca
from pyield._internal.converters import converter_datas
from pyield._internal.numbers import para_decimal
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf.titulos._utils import converter_taxa
from pyield.vna._consulta import historico, vigencia

_ESQUEMA = {
    "titulo": pl.String,
    "data": pl.Date,
    "inicio_vigencia": pl.Date,
    "fim_vigencia": pl.Date,
    "competencia": pl.Date,
    "vna_base": pl.Decimal(18, 6),
    "vna_fim": pl.Decimal(18, 6),
    "inflacao": pl.Decimal(38, 18),
    "situacao_inflacao": pl.String,
    "origem_inflacao": pl.String,
    "pendencias": pl.List(pl.String),
}


def contexto(
    titulo: Literal["NTN-B", "NTN-C"],
    data: DateLike,
    vencimento: DateLike | None = None,
    *,
    inflacao_projetada: float | Decimal | str | None = None,
    atualizar: bool = False,
) -> pl.DataFrame:
    """Consulta os parâmetros disponíveis para a vigência mensal do VNA.

    Args:
        titulo: NTN-B ou NTN-C.
        data: Data contida na vigência, nos formatos aceitos pela biblioteca.
        vencimento: Obrigatório para selecionar a série da NTN-C; não se
            aplica à NTN-B.
        inflacao_projetada: Projeção mensal opcional em unidade decimal:
            0.0045 representa 0,45%. Aceita também percentual explícito, como
            ``"0.45%"`` ou ``"0,45%"``. Convertida para Decimal sem arredondar;
            usada somente se a taxa realizada estiver indisponível.
            Não busca projeções automaticamente.
        atualizar: Renova apenas o cache do histórico de VNA, como em historico.
            Não renova os caches de inflação.

    Returns:
        DataFrame com uma linha, inclusive quando faltam parâmetros. Dados
        numéricos indisponíveis são nulos, com pendências explícitas.

    Output Columns:
        titulo (String): Título consultado.
        data (Date): Data para a qual os parâmetros foram selecionados.
        inicio_vigencia (Date): Início inclusivo, sem ajuste útil.
        fim_vigencia (Date): Fim exclusivo, sem ajuste útil.
        competencia (Date): Primeiro dia do mês da inflação da vigência.
        vna_base (Decimal(18, 6)): VNA publicado no início; nulo se ausente.
        vna_fim (Decimal(18, 6)): VNA publicado no fim; nulo se ausente.
        inflacao (Decimal(38, 18)): Variação mensal decimal, realizada ou
            explicitamente projetada. Pode ser passada diretamente a projetado.
        situacao_inflacao (String): "realizado", "projetado" ou "indisponivel".
        origem_inflacao (String): "IBGE", "SGS/BCB" ou "informada"; nula sem taxa.
        pendencias (List[String]): "vna_base_indisponivel" e/ou
            "inflacao_indisponivel". Lista vazia se ambos estiverem disponíveis.

    Notes:
        Fontes: histórico do Tesouro Nacional; taxa do IPCA do IBGE e taxa do
        IGP-M do SGS/BCB. A competência é o mês do início da vigência: dia 15
        para NTN-B e dia 1 para NTN-C. As consultas refletem os dados disponíveis
        agora, sem reconstruir o que era conhecido na data solicitada.
        A inflação realizada não comprova publicação do VNA final. Este contexto
        não calcula VNA, não infere inflação da razão entre VNAs e não estima
        uma base ausente. Projeções e dados oficiais permanecem identificados.
        Erros operacionais são propagados, sem tratá-los como falta de dados.
        Para detectar transição, compare inicio_vigencia dos contextos de
        operação e liquidação; cada data seleciona sua própria base.

    Raises:
        ValueError: Título ou data inválidos, vencimento incompatível/ausente,
            projeção não finita ou menor ou igual a -1, ou percentual textual inválido.
    """
    inicio, fim = vigencia(titulo, data)
    if titulo == "NTN-C" and (vencimento is None or any_is_empty(vencimento)):
        raise ValueError("Informe o vencimento para selecionar a série da NTN-C.")
    if titulo == "NTN-B" and vencimento is not None:
        raise ValueError("Vencimento aplica-se apenas à NTN-C.")
    projecao = None
    if inflacao_projetada is not None:
        projecao = para_decimal(converter_taxa(inflacao_projetada))
        if not projecao.is_finite() or projecao <= -1:
            raise ValueError("A projeção deve ser finita e maior que -1.")

    dados = historico(titulo, vencimento, atualizar=atualizar)
    base = dados.filter(pl.col("data") == inicio)
    final = dados.filter(pl.col("data") == fim)
    vna_base = None if base.is_empty() else base.item(0, "vna")
    vna_fim = None if final.is_empty() else final.item(0, "vna")
    competencia = inicio.replace(day=1)
    inflacao = ipca.taxa(competencia) if titulo == "NTN-B" else igpm.taxa(competencia)
    if inflacao.is_finite():
        situacao = "realizado"
        origem = "IBGE" if titulo == "NTN-B" else "SGS/BCB"
    else:
        inflacao = projecao
        situacao = "projetado" if projecao is not None else "indisponivel"
        origem = "informada" if projecao is not None else None
    pendencias = []
    if vna_base is None:
        pendencias.append("vna_base_indisponivel")
    if inflacao is None:
        pendencias.append("inflacao_indisponivel")
    return pl.DataFrame(
        [
            {
                "titulo": titulo,
                "data": converter_datas(data),
                "inicio_vigencia": inicio,
                "fim_vigencia": fim,
                "competencia": competencia,
                "vna_base": vna_base,
                "vna_fim": vna_fim,
                "inflacao": inflacao,
                "situacao_inflacao": situacao,
                "origem_inflacao": origem,
                "pendencias": pendencias,
            }
        ],
        schema=_ESQUEMA,
    )
