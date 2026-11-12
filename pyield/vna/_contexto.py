"""Parâmetros disponíveis para a vigência mensal do VNA."""

import datetime as dt
from decimal import Decimal
from typing import Literal

import polars as pl

from pyield import igpm, ipca
from pyield._internal.converters import converter_datas
from pyield._internal.numbers import converter_taxa, para_decimal
from pyield._internal.types import DateLike, any_is_empty
from pyield.vna._consulta import historico, vigencia

_ESQUEMA = {
    "titulo": pl.String,
    "data": pl.Date,
    "inicio_vigencia": pl.Date,
    "fim_vigencia": pl.Date,
    "competencia": pl.Date,
    "competencia_anterior": pl.Date,
    "vna_base": pl.Decimal(18, 6),
    "vna_fim": pl.Decimal(18, 6),
    "indice_anterior": pl.Decimal(18, 2),
    "indice_atual": pl.Decimal(18, 2),
    "origem_indice": pl.String,
    "inflacao": pl.Decimal(38, 4),
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

    Returns:
        DataFrame com uma linha, inclusive quando faltam parâmetros. Dados
        numéricos indisponíveis são nulos, com pendências explícitas de base
        e inflação para projeção por taxa.

    Output Columns:
        * titulo (String): Título consultado.
        * data (Date): Data para a qual os parâmetros foram selecionados.
        * inicio_vigencia (Date): Início inclusivo, sem ajuste útil.
        * fim_vigencia (Date): Fim exclusivo, sem ajuste útil.
        * competencia (Date): Primeiro dia do mês da inflação da vigência.
        * competencia_anterior (Date): Primeiro dia do mês anterior à competência.
        * vna_base (Decimal(18, 6)): VNA publicado no início; nulo se ausente.
        * vna_fim (Decimal(18, 6)): VNA publicado no fim; nulo se ausente.
        * indice_anterior (Decimal(18, escala)): Número-índice da competência
            anterior; escala 2 para IPCA e 3 para IGP-M. Nulo se ausente.
        * indice_atual (Decimal(18, escala)): Número-índice da competência da
            vigência, com a mesma escala de indice_anterior. Nulo se ausente.
        * origem_indice (String): "IBGE" ou "FGV/Ipeadata"; nula se ambos os
            índices estiverem ausentes.
        * inflacao (Decimal(38, escala)): Variação mensal decimal, com escala 4
            quando realizada ou indisponível. Para projeções informadas, preserva
            a escala da entrada. Pode ser passada diretamente a projetado,
            que aplica as regras de projeção, mesmo para uma taxa realizada.
        * situacao_inflacao (String): "realizado", "projetado" ou "indisponivel".
        * origem_inflacao (String): "IBGE", "SGS/BCB" ou "informada"; nula sem taxa.
        * pendencias (List[String]): "vna_base_indisponivel" e/ou
            "inflacao_indisponivel". Lista vazia se ambos estiverem disponíveis.
            Refere-se à projeção por taxa; não atesta disponibilidade dos índices.

    Notes:
        Fontes: histórico do Tesouro Nacional; taxa do IPCA do IBGE e taxa do
        IGP-M do SGS/BCB; números-índice do IPCA do IBGE e do IGP-M da FGV
        disponibilizados pelo Ipeadata. A competência é o mês do início da
        vigência: dia 15 para NTN-B e dia 1 para NTN-C. As consultas refletem
        os dados disponíveis agora, sem reconstruir o que era conhecido na
        data solicitada.
        A inflação realizada não comprova publicação do VNA final. Este contexto
        não calcula VNA, não infere inflação da razão entre VNAs e não estima
        uma base ausente. Projeções e dados oficiais permanecem identificados.
        Os números-índice são consultados independentemente da taxa mensal;
        não são estimados por projeção nem pela razão entre VNAs. Para IGP-M,
        a cobertura começa em agosto de 1994. Competências anteriores ao corte
        ficam nulas, inclusive a anterior de agosto/1994. A razão
        indice_atual / indice_anterior preserva informação que a taxa mensal
        arredondada pode perder. Passar essa taxa a projetado não reproduz
        necessariamente o pró-rata realizado com os índices. Para NTN-B,
        valor usa os índices entre duas referências publicadas; para NTN-C,
        usa a razão entre os VNAs publicados. Índices disponíveis não comprovam
        a publicação do VNA final; consulte os campos nulos individualmente.
        Erros operacionais são propagados, sem tratá-los como falta de dados.
        Para detectar transição, compare inicio_vigencia dos contextos de
        operação e liquidação; cada data seleciona sua própria base.

    Raises:
        ValueError: Título ou data inválidos, vencimento incompatível/ausente,
            projeção não finita ou menor ou igual a -1, ou percentual textual inválido.

    Examples:
        Consulta uma vigência histórica da NTN-B e verifica a competência
        da inflação, a base publicada e a disponibilidade dos parâmetros:

        >>> parametros = yd.vna.contexto("NTN-B", "30-06-2026")
        >>> parametros.select(
        ...     "competencia", "vna_base", "inflacao", "situacao_inflacao", "pendencias"
        ... ).row(0)
        (datetime.date(2026, 6, 1), Decimal('4731.856412'),
         Decimal('0.0016'), 'realizado', [])

        Os números-índice correspondem à competência anterior e à da vigência,
        independentemente da disponibilidade da taxa mensal:

        >>> parametros.select(
        ...     "competencia_anterior", "indice_anterior", "indice_atual"
        ... ).row(0)
        (datetime.date(2026, 5, 1), Decimal('7640.15'), Decimal('7652.37'))

        Para a vigência atual da NTN-C, informa o vencimento e uma projeção
        de 0,45%, usada somente se a inflação realizada não estiver disponível.
        A situação e as pendências dependem dos dados publicados na consulta:

        >>> parametros = yd.vna.contexto(
        ...     "NTN-C",
        ...     yd.hoje(),
        ...     vencimento="01-01-2031",
        ...     inflacao_projetada="0,45%",
        ... )
        >>> situacao = parametros.item(0, "situacao_inflacao")
        >>> pendencias = parametros.item(0, "pendencias")
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

    dados = dict(historico(titulo, vencimento).select("data", "vna").iter_rows())
    vna_base = dados.get(inicio)
    competencia = inicio.replace(day=1)
    competencia_anterior = (competencia - dt.timedelta(days=1)).replace(day=1)
    indexador = ipca if titulo == "NTN-B" else igpm
    inflacao = indexador.taxa(competencia)
    if inflacao.is_finite():
        situacao = "realizado"
        origem = "IBGE" if titulo == "NTN-B" else "SGS/BCB"
    else:
        inflacao = projecao
        situacao = "projetado" if projecao is not None else "indisponivel"
        origem = "informada" if projecao is not None else None
    indices = indexador.indice_serie(competencia_anterior, competencia).select(
        indice_anterior=pl.col("indice")
        .filter(pl.col("competencia") == competencia_anterior)
        .first(),
        indice_atual=pl.col("indice")
        .filter(pl.col("competencia") == competencia)
        .first(),
    )
    origem_indice = None
    if indices.row(0) != (None, None):
        origem_indice = "IBGE" if titulo == "NTN-B" else "FGV/Ipeadata"
    pendencias = []
    if vna_base is None:
        pendencias.append("vna_base_indisponivel")
    if inflacao is None:
        pendencias.append("inflacao_indisponivel")
    escala = 4 if inflacao is None else max(0, -int(inflacao.as_tuple().exponent))
    return pl.DataFrame(
        [
            {
                "titulo": titulo,
                "data": converter_datas(data),
                "inicio_vigencia": inicio,
                "fim_vigencia": fim,
                "competencia": competencia,
                "competencia_anterior": competencia_anterior,
                "vna_base": vna_base,
                "vna_fim": dados.get(fim),
                **indices.row(0, named=True),
                "origem_indice": origem_indice,
                "inflacao": inflacao,
                "situacao_inflacao": situacao,
                "origem_inflacao": origem,
                "pendencias": pendencias,
            }
        ],
        schema=_ESQUEMA | indices.schema | {"inflacao": pl.Decimal(38, escala)},
    )
