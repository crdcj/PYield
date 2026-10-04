"""Cálculos e dados de LTN.

Convenções de precificação (STN, tabela 3):
    - Valor de face: 1000 reais.
    - Prazo de desconto: dias úteis / 252, truncado a 14 casas.
    - PU: truncado a 6 casas.
"""

from decimal import Decimal

import polars as pl

from pyield import du, fwd
from pyield._internal.numbers import truncar_decimal
from pyield._internal.types import DateLike, any_is_empty

from . import _utils

VALOR_FACE = 1000

_SCHEMA_DADOS = {
    "data_referencia": pl.Date,
    "titulo": pl.String,
    "codigo_selic": pl.Int64,
    "data_base": pl.Date,
    "data_vencimento": pl.Date,
    "dias_uteis": pl.Int64,
    "duration": pl.Float64,
    "prazo_medio": pl.Float64,
    "dv01": pl.Float64,
    "pu": pl.Float64,
    "taxa_compra": pl.Float64,
    "taxa_venda": pl.Float64,
    "taxa_indicativa": pl.Float64,
    "taxa_di": pl.Float64,
    "premio": pl.Float64,
    "rentabilidade": pl.Float64,
}


def dados(data: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de LTN na ANBIMA para a data de referência.

    Args:
        data: Data da consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de LTN. Na ausência de dados,
            retorna vazio com as mesmas colunas e tipos.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "LTN").
        - codigo_selic (Int64): Código do título no SELIC.
        - data_base (Date): Data base de emissão do título.
        - data_vencimento (Date): Data de vencimento do título.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - duration (Float64): Macaulay Duration do título (anos).
        - prazo_medio (Float64): Prazo médio do título (anos).
        - dv01 (Float64): Variação no preço para 1bp de taxa.
        - pu (Float64): Preço unitário (PU).
        - taxa_compra (Float64): Taxa de compra (decimal).
        - taxa_venda (Float64): Taxa de venda (decimal).
        - taxa_indicativa (Float64): Taxa indicativa (decimal).
        - taxa_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo
            método flat forward.
        - premio (Float64): prêmio sobre o DI, isto é, o spread sobre a
            taxa DI.
        - rentabilidade (Float64): Razão entre as taxas diárias equivalentes
            da LTN e do DI.

    Examples:
        >>> from pyield import ltn
        >>> df_ltn = ltn.dados("23-08-2024")  # doctest: +SKIP
    """
    df = _utils.obter_tpf(data, "LTN")
    if df.is_empty():
        return pl.DataFrame(schema=_SCHEMA_DADOS)

    df = df.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
        duration=duration_expr("data_referencia", "data_vencimento"),
    ).with_columns(
        prazo_medio=pl.col("duration"),
        dv01=dv01_expr("data_referencia", "data_vencimento", "taxa_indicativa"),
    )
    df = _utils.adicionar_taxa_di(df, data)

    df = df.with_columns(
        premio=pl.col("taxa_indicativa") - pl.col("taxa_di"),
        rentabilidade=rentabilidade_expr("taxa_indicativa", "taxa_di"),
    )

    return df.select(*_SCHEMA_DADOS)


def vencimentos(data: DateLike) -> pl.Series:
    """
    Busca os vencimentos disponíveis para a data de referência.

    Args:
        data: Data da consulta.

    Returns:
        pl.Series: Série de datas de vencimento disponíveis.

    Examples:
        >>> from pyield import ltn
        >>> ltn.vencimentos("22-08-2024")
        shape: (13,)
        Series: 'data_vencimento' [date]
        [
            2024-10-01
            2025-01-01
            2025-04-01
            2025-07-01
            2025-10-01
            …
            2026-10-01
            2027-07-01
            2028-01-01
            2028-07-01
            2030-01-01
        ]
    """
    return dados(data)["data_vencimento"]


def pu(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float | Decimal | str,
) -> Decimal:
    r"""
    Calcula o PU da LTN pela metodologia da STN para leilões primários.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        taxa: Taxa anual efetiva, em decimal, na base de 252 dias úteis.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
            Antes do cálculo, é truncada em oito casas decimais (seis na
            forma percentual), descartando as casas excedentes sem arredondar.

    Returns:
        Decimal: PU da LTN truncado em seis casas decimais. Retorna
            ``Decimal("NaN")`` quando o prazo até o vencimento não é positivo.

    Notes:
        O PU teórico é:

        \[
        \mathrm{PU} = \frac{\mathrm{FC}}{(1+y)^t}
        \]

        onde:

        - \(\mathrm{FC}\): pagamento único no vencimento, de R$ 1.000.
        - \(\mathrm{DU}\): dias úteis entre liquidação e vencimento.
        - \(t = \mathrm{DU}/252\): prazo até o vencimento, em anos de
          252 dias úteis.
        - \(y\): taxa anual efetiva informada em `taxa`, em decimal, na
          base de 252 dias úteis.

        Como a LTN possui um único pagamento, sua taxa anual efetiva coincide
        com a taxa zero e com a TIR para o prazo do título. O fluxo e o prazo
        são determinados pelo título e pela liquidação. No cálculo oficial,
        além do truncamento da taxa
        descrito em `Args`, o prazo é truncado em 14 casas decimais e o PU
        é truncado em seis.

    References:
        - Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos
          Públicos Federais Ofertados nos Leilões Primários.
          https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

    Examples:
        >>> from pyield import ltn
        >>> ltn.pu("05-07-2024", "01-01-2030", "12.145%")
        Decimal('535.279902')
        >>> ltn.pu("21-05-2008", "01-07-2010", "14.36%")
        Decimal('753.315323')
    """
    # Valida e normaliza entradas
    taxa = _utils.converter_taxa(taxa)
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return Decimal("NaN")
    taxa = _utils.normalizar_taxa_precificacao(taxa)
    # Calcula dias úteis entre liquidação e vencimento
    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return Decimal("NaN")

    # Calcula anos úteis truncados conforme a STN
    anos_truncados = _utils.truncar(dias_uteis / 252, 14)

    fator_desconto = (1 + taxa) ** anos_truncados

    # Trunca o preço em 6 casas conforme a STN
    return truncar_decimal(VALOR_FACE / fator_desconto, 6)


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    preco_unitario: float | Decimal,
) -> float:
    r"""
    Calcula a taxa implícita da LTN a partir do preço unitário (PU).

    Inverte algebricamente o valor presente, sem truncamentos da STN:

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        preco_unitario: PU do título.

    Returns:
        float: Taxa implícita em formato decimal, sem arredondamento.
            Retorna NaN para entradas ausentes, PU não positivo ou prazo útil
            não positivo.

    Notes:
        A taxa implícita \(y\), anual efetiva, em decimal e na base de
        252 dias úteis, é:

        \[
        y = \left(\frac{\mathrm{FC}}{\mathrm{PU}}\right)^{1/t}-1
        \]

        onde:

        - \(\mathrm{FC}\): pagamento único no vencimento, de R$ 1.000.
        - \(\mathrm{PU}\): preço recebido em `preco_unitario`.
        - \(\mathrm{DU}\): dias úteis entre liquidação e vencimento.
        - \(t = \mathrm{DU}/252\): prazo até o vencimento, em anos de
          252 dias úteis.

        Como há apenas um pagamento, a taxa anual efetiva coincide com a taxa
        zero e com a TIR para o prazo do título. A inversão usa o preço e o
        prazo sem os truncamentos do PU oficial; por isso, pode não recuperar
        exatamente a taxa usada
        para gerar um PU por `pu`.

    Examples:
        Exibe as taxas em formato decimal:

        >>> from pyield import ltn
        >>> ltn.taxa("05-07-2024", "01-01-2030", 535.279902)
        0.12145000037780962
        >>> ltn.taxa("13-03-2026", "01-01-2027", 895.563913) * 100
        14.830700071776182
        >>> ltn.taxa("21-05-2008", "01-07-2010", 753.3) * 100
        14.361101890993865
    """
    if any_is_empty(data_liquidacao, data_vencimento, preco_unitario):
        return float("nan")

    preco_float = float(preco_unitario)
    if preco_float <= 0:
        return float("nan")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return float("nan")
    return (VALOR_FACE / preco_float) ** (252 / dias_uteis) - 1


def rentabilidade(taxa_ltn: float | str, taxa_di: float | str) -> float:
    r"""
    Calcula a rentabilidade da LTN sobre a taxa de DI Futuro.

    Args:
        taxa_ltn: Taxa anual efetiva da LTN, em decimal, na base de 252 dias úteis.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
        taxa_di: Taxa DI anual efetiva, em decimal, na base de 252 dias úteis,
            para o mesmo prazo da LTN.
            Aceita também percentual explícito: "5.75%" ou "5,75%".

    Returns:
        float: Razão entre as taxas diárias equivalentes da LTN e do DI.
            Por exemplo, 1.01 representa 101% da taxa diária equivalente DI.

    Notes:
        A rentabilidade relativa \(q\) é a razão entre as taxas diárias
        equivalentes:

        \[
        q = \frac{(1+y_{\mathrm{LTN}})^{1/252}-1}
        {(1+\mathrm{DI})^{1/252}-1}
        \]

        onde:

        - \(y_{\mathrm{LTN}}\): taxa informada em `taxa_ltn`.
        - \(\mathrm{DI}\): taxa DI informada em `taxa_di`, para o mesmo
          prazo da LTN.

        Como a LTN possui um único pagamento, a TIR equivalente da referência
        coincide com a taxa DI para o prazo do título. O indicador compara
        taxas implícitas; não representa o retorno realizado entre compra e venda.

    Examples:
        Data de referência: 22-08-2024.
        Taxa da LTN com vencimento em 01-01-2030: 0.118746.
        Taxa de ajuste do DI (JAN30): 0.11725.
        >>> from pyield import ltn
        >>> ltn.rentabilidade("11.8746%", "11.725%")
        1.0120718007994287
    """
    if isinstance(taxa_ltn, str):
        taxa_ltn = float(_utils.converter_taxa(taxa_ltn))
    if isinstance(taxa_di, str):
        taxa_di = float(_utils.converter_taxa(taxa_di))
    if any_is_empty(taxa_ltn, taxa_di):
        return float("nan")
    # Cálculo das taxas diárias
    taxa_diaria_ltn = (1 + taxa_ltn) ** (1 / 252) - 1
    taxa_diaria_di = (1 + taxa_di) ** (1 / 252) - 1

    # Retorno do cálculo da rentabilidade
    return taxa_diaria_ltn / taxa_diaria_di


def rentabilidade_expr(
    taxa_ltn: pl.Expr | str,
    taxa_di: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para a rentabilidade da LTN sobre o DI.

    Args:
        taxa_ltn: Nome de coluna ou expressão Polars com a taxa anual efetiva
            da LTN, em decimal, na base de 252 dias úteis.
        taxa_di: Nome de coluna ou expressão Polars com a taxa DI anual efetiva,
            em decimal, na base de 252 dias úteis, para o mesmo prazo da LTN.

    Returns:
        pl.Expr: Expressão sem alias com a rentabilidade da LTN sobre o DI.
    """
    expr_ltn = taxa_ltn if isinstance(taxa_ltn, pl.Expr) else pl.col(taxa_ltn)
    expr_di = taxa_di if isinstance(taxa_di, pl.Expr) else pl.col(taxa_di)
    taxa_diaria_ltn = (1 + expr_ltn) ** (1 / 252) - 1
    taxa_diaria_di = (1 + expr_di) ** (1 / 252) - 1
    return taxa_diaria_ltn / taxa_diaria_di


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float | Decimal | str,
) -> float:
    r"""
    Calcula o DV01 (Dollar Value of 01) da LTN em R$.

    Representa a redução do PU teórico para um aumento de 1 bp (0,01 ponto
    percentual) na taxa, calculada pela diferença entre os preços antes e depois
    do aumento.

    Args:
        data_liquidacao: Data de liquidação.
        data_vencimento: Data de vencimento.
        taxa: Taxa anual efetiva, em decimal, na base de 252 dias úteis.
            Aceita também percentual explícito: "5.75%" ou "5,75%".

    Returns:
        float: DV01, variação de preço para 1 bp. Retorna ``NaN`` quando o
            prazo até o vencimento não é positivo.

    Notes:
        Mantendo liquidação e vencimento fixos, o pagamento e seu prazo
        também ficam fixos. Nesse contexto, o preço teórico em função da taxa é:

        \[
        \mathrm{PU}(y) = \frac{\mathrm{FC}}{(1+y)^t}
        \]

        O \(\mathrm{DV01}\) é a redução do preço, em R$, para um aumento
        de 1 bp na taxa:

        \[
        \mathrm{DV01} = \mathrm{PU}(y)-\mathrm{PU}(y+0.0001)
        \]

        onde:

        - \(\mathrm{FC}\): pagamento único no vencimento, de R$ 1.000.
        - \(\mathrm{DU}\): dias úteis entre liquidação e vencimento.
        - \(t = \mathrm{DU}/252\): prazo até o vencimento, em anos de
          252 dias úteis.
        - \(y\): taxa anual efetiva informada em `taxa`, em decimal, na
          base de 252 dias úteis.
        - \(0.0001\): aumento de 1 bp na taxa em decimal.

        Os preços não aplicam os truncamentos do PU oficial.

    Examples:
        >>> from pyield import ltn
        >>> ltn.dv01("26-03-2025", "01-01-2032", "15.097%")
        0.2269055067940826
    """
    taxa = _utils.converter_taxa(taxa)
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    dias_uteis = du.contar(data_liquidacao, data_vencimento)
    if dias_uteis <= 0:
        return float("nan")
    anos_uteis = dias_uteis / 252
    pu1 = VALOR_FACE / (1 + float(taxa)) ** anos_uteis
    pu2 = VALOR_FACE / (1 + float(taxa) + 0.0001) ** anos_uteis
    return pu1 - pu2


def duration_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
) -> pl.Expr:
    r"""Cria expressão Polars para a duration da LTN em anos úteis.

    Args:
        data_liquidacao: Nome de coluna ou expressão Polars com a data de
            liquidação.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento.

    Returns:
        pl.Expr: Expressão sem alias com a duration em anos úteis.

    Notes:
        Como a LTN possui um único pagamento, a Macaulay duration \(D\)
        coincide com o prazo até o vencimento:

        \[
        D = t = \frac{\mathrm{DU}}{252}
        \]

        onde \(\mathrm{DU}\) é o número de dias úteis entre liquidação e
        vencimento. O resultado é expresso em anos de 252 dias úteis.
    """
    dias_uteis = du.contar_expr(data_liquidacao, data_vencimento)
    return pl.when(dias_uteis > 0).then(dias_uteis / 252).otherwise(float("nan"))


def dv01_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
    taxa: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para o DV01 da LTN.

    O cálculo é aplicado linha a linha e reprifica o PU teórico para um
    aumento de 1 bp na taxa.

    Args:
        data_liquidacao: Nome de coluna ou expressão Polars com a data de
            liquidação.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento.
        taxa: Nome de coluna ou expressão Polars com a taxa anual efetiva,
            em decimal, na base de 252 dias úteis.

    Returns:
        pl.Expr: Expressão sem alias com o DV01.
    """
    return pl.struct(
        _utils.coluna_ou_expr(data_liquidacao, "data_liquidacao"),
        _utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        _utils.coluna_ou_expr(taxa, "taxa"),
    ).map_elements(
        lambda s: dv01(
            s["data_liquidacao"],
            s["data_vencimento"],
            s["taxa"],
        ),
        return_dtype=pl.Float64,
    )


def taxas_forward(data: DateLike) -> pl.DataFrame:
    r"""Calcula as taxas forward da LTN para uma data de referência.

    As taxas indicativas da LTN já são taxas zero por construção, pois o
    título não paga cupons. Portanto o cálculo de forward é direto usando a
    estrutura de vencimentos e suas taxas.

    Args:
        data: Data das taxas indicativas.

    Returns:
        pl.DataFrame: DataFrame com as taxas forward.

    Output Columns:
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - taxa_indicativa (Float64): Taxa spot (zero cupom), em formato decimal.
        - taxa_forward (Float64): Taxa forward, em formato decimal.

    Notes:
        A taxa forward \(f(t_1,t_2)\), anual efetiva, em decimal e na base
        de 252 dias úteis, entre dois vencimentos consecutivos é:

        \[
        f(t_1,t_2) = \left(
        \frac{(1+r_2)^{t_2}}{(1+r_1)^{t_1}}
        \right)^{1/(t_2-t_1)}-1
        \]

        onde:

        - \(\mathrm{DU}_1\), \(\mathrm{DU}_2\): dias úteis entre a data
          de referência e cada vencimento, com \(\mathrm{DU}_1<\mathrm{DU}_2\).
        - \(t_1 = \mathrm{DU}_1/252\), \(t_2 = \mathrm{DU}_2/252\):
          prazos em anos de 252 dias úteis.
        - \(r_1\), \(r_2\): taxas zero anuais efetivas, em decimal,
          correspondentes aos prazos. São as taxas indicativas das LTNs.

        No primeiro vencimento, a coluna `taxa_forward` recebe a própria
        taxa zero, pois não há um vértice anterior.

    Examples:
        >>> from pyield import ltn
        >>> import polars.selectors as cs
        >>> curva = ltn.taxas_forward("17-10-2025")
        >>> curva.with_columns(cs.starts_with("taxa_") * 100)
        shape: (13, 4)
        ┌─────────────────┬────────────┬─────────────────┬──────────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_indicativa ┆ taxa_forward │
        │ ---             ┆ ---        ┆ ---             ┆ ---          │
        │ date            ┆ i64        ┆ f64             ┆ f64          │
        ╞═════════════════╪════════════╪═════════════════╪══════════════╡
        │ 2026-01-01      ┆ 52         ┆ 14.8307         ┆ 14.8307      │
        │ 2026-04-01      ┆ 113        ┆ 14.7173         ┆ 14.62072     │
        │ 2026-07-01      ┆ 174        ┆ 14.5206         ┆ 14.157112    │
        │ 2026-10-01      ┆ 239        ┆ 14.2424         ┆ 13.501001    │
        │ 2027-04-01      ┆ 361        ┆ 13.8155         ┆ 12.983814    │
        │ …               ┆ …          ┆ …               ┆ …            │
        │ 2028-07-01      ┆ 676        ┆ 13.3411         ┆ 13.165428    │
        │ 2029-01-01      ┆ 800        ┆ 13.4254         ┆ 13.886075    │
        │ 2029-07-01      ┆ 924        ┆ 13.5264         ┆ 14.180178    │
        │ 2030-01-01      ┆ 1049       ┆ 13.5967         ┆ 14.11771     │
        │ 2032-01-01      ┆ 1553       ┆ 13.883          ┆ 14.481206    │
        └─────────────────┴────────────┴─────────────────┴──────────────┘
    """
    if any_is_empty(data):
        return pl.DataFrame()
    return (
        _utils.obter_tpf(data, "LTN")
        .select(
            "data_vencimento",
            dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
            taxa_indicativa="taxa_indicativa",
        )
        .with_columns(taxa_forward=fwd.forwards_expr("dias_uteis", "taxa_indicativa"))
        .sort("data_vencimento")
    )


__all__ = [
    "dados",
    "duration_expr",
    "dv01",
    "dv01_expr",
    "pu",
    "rentabilidade",
    "rentabilidade_expr",
    "taxa",
    "taxas_forward",
    "vencimentos",
]


def __dir__() -> list[str]:
    return __all__
