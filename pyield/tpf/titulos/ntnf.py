"""Cálculos e dados de NTN-F.

Convenções de precificação (STN, tabela 3):
    - Prazo de desconto: dias úteis / 252, truncado a 14 casas.
    - PU: truncado a 6 casas.
    - Cada fluxo descontado: arredondado a 9 casas.

Valores de referência derivados conforme as regras da STN:
    - Valor de face: 1000 reais.
    - Taxa do cupom: (0.10 + 1) ** 0.5 - 1, isto é, 10% a.a. com capitalização
      semestral.
    - Valor do cupom: valor de face × taxa do cupom, arredondado a 5 casas
      (48.80885).
    - Pagamento final: valor de face + valor do cupom (1048.80885).

A NTN-F paga dois cupons por ano (semestrais). As datas de cupom são derivadas
do vencimento (retrocedendo 6 em 6 meses), sem depender de meses fixos.
    Ex.: vencimento 01-01-2027 gera cupons em 01-07-2026, 01-01-2026, ...
"""

import datetime as dt
import math
from decimal import Decimal

import polars as pl

import pyield._internal.converters as conversores
from pyield import du, interpolador
from pyield._internal.numbers import converter_taxa, truncar, truncar_decimal
from pyield._internal.types import ArrayLike, DateLike, DatesLike, any_is_empty
from pyield.futuro import di1
from pyield.tpf.titulos._ntnf_bootstrap import (
    taxas_zero_forwards as taxas_zero_forwards,  # noqa: PLC0414
)

from . import _utils

VALOR_CUPOM = 48.80885
VALOR_FINAL = 1048.80885

_SCHEMA_TAXAS_ZERO = {
    "data_vencimento": pl.Date,
    "dias_uteis": pl.Int64,
    "taxa_zero": pl.Float64,
}

_SCHEMA_DADOS = {
    "data_referencia": pl.Date,
    "titulo": pl.String,
    "codigo_selic": pl.Int64,
    "data_base": pl.Date,
    "data_vencimento": pl.Date,
    "dias_uteis": pl.Int64,
    "prazo_medio": pl.Float64,
    "duration": pl.Float64,
    "dv01": pl.Float64,
    "pu": pl.Float64,
    "taxa_compra": pl.Float64,
    "taxa_venda": pl.Float64,
    "taxa_indicativa": pl.Float64,
    "taxa_di": pl.Float64,
    "taxa_zero": pl.Float64,
    "premio": pl.Float64,
    "premio_limpo": pl.Float64,
    "rentabilidade": pl.Float64,
}


def dados(data: DateLike) -> pl.DataFrame:
    """
    Busca as taxas indicativas de NTN-F na ANBIMA para a data de referência.

    Args:
        data (DateLike): Data da consulta.

    Returns:
        pl.DataFrame: DataFrame Polars com os dados de NTN-F. Na ausência de
            dados, retorna vazio com as mesmas colunas e tipos.

    Output Columns:
        - data_referencia (Date): Data de referência dos dados.
        - titulo (String): Tipo do título (ex.: "NTN-F").
        - codigo_selic (Int64): Código do título no SELIC.
        - data_base (Date): Data base de emissão do título.
        - data_vencimento (Date): Data de vencimento do título.
        - dias_uteis (Int64): Dias úteis entre referência e vencimento.
        - prazo_medio (Float64): Prazo médio do título (anos).
        - duration (Float64): Macaulay Duration do título (anos).
        - dv01 (Float64): Variação no preço para 1bp de taxa.
        - pu (Float64): Preço unitário (PU).
        - taxa_compra (Float64): Taxa de compra (decimal).
        - taxa_venda (Float64): Taxa de venda (decimal).
        - taxa_indicativa (Float64): Taxa indicativa (decimal).
        - taxa_di (Float64): Taxa de ajuste do DI Futuro interpolada pelo
            método flat forward (decimal).
        - taxa_zero (Float64): Taxa zero (zero cupom via bootstrap).
        - premio (Float64): prêmio, isto é, o spread sobre o DI.
        - premio_limpo (Float64): prêmio limpo sobre a curva DI.
        - rentabilidade (Float64): Rentabilidade da NTN-F sobre a curva DI.

    Examples:
        >>> from pyield import ntnf
        >>> df_ntnf = ntnf.dados("23-08-2024")  # doctest: +SKIP
    """
    df = _utils.obter_tpf(data, "NTN-F")
    if df.is_empty():
        return pl.DataFrame(schema=_SCHEMA_DADOS)

    # Adiciona duration, prazo_medio, dv01 e taxa_di
    df = df.with_columns(
        dias_uteis=du.contar_expr("data_referencia", "data_vencimento"),
        duration=duration_expr("data_referencia", "data_vencimento", "taxa_indicativa"),
    ).with_columns(
        prazo_medio=pl.col("duration"),
        dv01=dv01_expr("data_referencia", "data_vencimento", "taxa_indicativa"),
    )
    df = _utils.adicionar_taxa_di(df, data)

    # Busca dados de LTN para bootstrap das taxas zero
    df_ltn = _utils.obter_tpf(data, "LTN").select("data_vencimento", "taxa_indicativa")
    df_zero = taxas_zero(
        data_liquidacao=data,
        vencimentos_ltn=df_ltn["data_vencimento"],
        taxas_ltn=df_ltn["taxa_indicativa"],
        vencimentos_ntnf=df["data_vencimento"],
        taxas_ntnf=df["taxa_indicativa"],
    ).select("data_vencimento", "taxa_zero")
    df = df.join(df_zero, on="data_vencimento", how="left")

    # Busca curva DI para cálculo da rentabilidade
    df_di = di1.dados(data, inicio_mes=True)

    # Calcula prêmios e rentabilidade para cada vencimento
    df = df.with_columns(
        premio=pl.col("taxa_indicativa") - pl.col("taxa_di"),
        premio_limpo=premio_limpo_expr(
            data_liquidacao=data,
            data_vencimento="data_vencimento",
            taxa_ntnf="taxa_indicativa",
            vencimentos_di=df_di["data_vencimento"],
            taxas_di=df_di["taxa_ajuste"],
        ),
        rentabilidade=rentabilidade_expr(
            data_liquidacao=data,
            data_vencimento="data_vencimento",
            taxa_ntnf="taxa_indicativa",
            vencimentos_di=df_di["data_vencimento"],
            taxas_di=df_di["taxa_ajuste"],
        ),
    )

    return df.select(*_SCHEMA_DADOS)


def vencimentos(data: DateLike) -> pl.Series:
    """
    Busca os vencimentos de NTN-F disponíveis para a data de referência.

    Args:
        data (DateLike): Data da consulta.

    Returns:
        pl.Series: Série de datas de vencimento de NTN-F.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.vencimentos("23-08-2024")
        shape: (6,)
        Series: 'data_vencimento' [date]
        [
            2025-01-01
            2027-01-01
            2029-01-01
            2031-01-01
            2033-01-01
            2035-01-01
        ]
    """
    return _utils.obter_tpf(data, "NTN-F")["data_vencimento"]


def datas_pagamento(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
) -> pl.Series:
    """
    Gera todas as datas de pagamento entre liquidação e vencimento.

    As datas são exclusivas para a liquidação e inclusivas para o vencimento.
    Os pagamentos são semestrais, contados a partir do vencimento (1º de
    janeiro e 1º de julho nas NTN-F já emitidas). No vencimento, o fluxo inclui
    o último cupom e a amortização do principal. O título NTN-F é determinado
    pela data de vencimento. As datas são contratuais e não são ajustadas para
    dias úteis.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.

    Returns:
        pl.Series: Série com as datas de pagamento entre a liquidação (exclusiva)
            e o vencimento (inclusivo). Retorna série vazia se o vencimento
            for menor ou igual à liquidação.

    Notes:
        Para obter as datas efetivas de processamento, use
        ``yd.du.deslocar(..., 0)``.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.datas_pagamento("15-05-2024", "01-01-2027")
        shape: (6,)
        Series: 'datas_pagamento' [date]
        [
            2024-07-01
            2025-01-01
            2025-07-01
            2026-01-01
            2026-07-01
            2027-01-01
        ]

        A data de liquidação coincidente com um pagamento é exclusiva:

        >>> ntnf.datas_pagamento("01-07-2024", "01-01-2027")
        shape: (5,)
        Series: 'datas_pagamento' [date]
        [
            2025-01-01
            2025-07-01
            2026-01-01
            2026-07-01
            2027-01-01
        ]
    """
    return _utils.gerar_datas_pagamento(data_liquidacao, data_vencimento)


def fluxos_caixa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
) -> pl.DataFrame:
    """
    Gera os fluxos de caixa da NTN-F entre liquidação (exclusiva) e vencimento
    (inclusivo). Os fluxos incluem cupons e o pagamento final no vencimento.

    Args:
        data_liquidacao (DateLike): Data de liquidação (exclusiva).
        data_vencimento (DateLike): Data de vencimento do título.

    Returns:
        pl.DataFrame: DataFrame com as colunas `data_pagamento` e
            `valor_pagamento`.

    Output Columns:
        - data_pagamento (Date): Data contratual do pagamento, sem ajuste para
            dia útil.
        - valor_pagamento (Float64): Valor do pagamento.

    Notes:
        Para obter as datas efetivas de processamento, use
        ``yd.du.deslocar(..., 0)``.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.fluxos_caixa("15-05-2024", "01-01-2027")
        shape: (6, 2)
        ┌────────────────┬─────────────────┐
        │ data_pagamento ┆ valor_pagamento │
        │ ---            ┆ ---             │
        │ date           ┆ f64             │
        ╞════════════════╪═════════════════╡
        │ 2024-07-01     ┆ 48.80885        │
        │ 2025-01-01     ┆ 48.80885        │
        │ 2025-07-01     ┆ 48.80885        │
        │ 2026-01-01     ┆ 48.80885        │
        │ 2026-07-01     ┆ 48.80885        │
        │ 2027-01-01     ┆ 1048.80885      │
        └────────────────┴─────────────────┘
    """
    if any_is_empty(data_liquidacao, data_vencimento):
        return pl.DataFrame(
            schema={"data_pagamento": pl.Date, "valor_pagamento": pl.Float64}
        )
    vencimento = conversores.converter_datas(data_vencimento)
    # O fluxo final ocorre no vencimento; os demais são cupons. Sem pagamentos
    # (liquidação >= vencimento), o resultado é vazio com o mesmo esquema.
    return pl.DataFrame(
        {"data_pagamento": datas_pagamento(data_liquidacao, vencimento)}
    ).with_columns(
        valor_pagamento=pl.when(pl.col("data_pagamento") == vencimento)
        .then(VALOR_FINAL)
        .otherwise(VALOR_CUPOM)
    )


def _valor_presente(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float | Decimal,
) -> float:
    """Valor presente dos fluxos pela TIR, sem os arredondamentos do PU oficial."""
    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    anos_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"]) / 252
    return float((df_fluxos["valor_pagamento"] / (1 + float(taxa)) ** anos_uteis).sum())


def pu(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float | Decimal | str,
) -> Decimal:
    r"""
    Calcula o PU da NTN-F pela metodologia da STN para leilões primários.

    O preço equivale ao valor presente dos fluxos descontados pela TIR
    informada.

    Args:
        data_liquidacao (DateLike): Data de liquidação para cálculo do preço.
        data_vencimento (DateLike): Data de vencimento do título.
        taxa: TIR anual efetiva, em decimal, na base de 252 dias úteis.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
            Antes do cálculo, é truncada em oito casas decimais (seis na
            forma percentual), descartando as casas excedentes sem arredondar.

    Returns:
        Decimal: Preço da NTN-F truncado em seis casas decimais. Retorna
            ``Decimal("NaN")`` para entradas nulas ou quando não há pagamentos
            após a liquidação.

    Notes:
        O PU teórico é:

        \[
        \mathrm{PU} = \sum_{i=1}^{n}
        \frac{\mathrm{FC}_i}{(1+y)^{t_i}}
        \]

        onde:

        - \(\mathrm{FC}_i\): valor do pagamento \(i\), incluindo cupom e
          amortização quando aplicável.
        - \(\mathrm{DU}_i\): dias úteis entre liquidação e pagamento \(i\).
        - \(t_i = \mathrm{DU}_i/252\): prazo até o pagamento \(i\), em anos
          de 252 dias úteis.
        - \(n\): número de pagamentos restantes.
        - \(y\): TIR anual efetiva informada em `taxa`, em decimal, na
          base de 252 dias úteis.

        Os fluxos e os prazos são determinados pela liquidação e pelo vencimento.
        No cálculo oficial, além do truncamento da TIR descrito em `Args`,
        cada prazo \(t_i\) é truncado em 14 casas decimais, cada fluxo
        descontado é arredondado em nove casas e a soma é truncada em seis.

        O cupom semestral é 48,80885, que representa 10% a.a. com
        capitalização semestral e arredondamento para 5 casas, conforme a STN.

    References:
        - Secretaria do Tesouro Nacional. Metodologia de Cálculo dos Títulos
          Públicos Federais Ofertados nos Leilões Primários.
          https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.pu("05-07-2024", "01-01-2035", "11.921%")
        Decimal('895.359254')
        >>> ntnf.pu("01-07-2024", "01-01-2027", "10%")
        Decimal('999.931303')
    """
    taxa = converter_taxa(taxa)
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return Decimal("NaN")
    taxa = _utils.normalizar_taxa_precificacao(taxa)

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return Decimal("NaN")

    dias_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"])
    anos_uteis = truncar(dias_uteis / 252, 14)
    vp = (df_fluxos["valor_pagamento"] / (1 + taxa) ** anos_uteis).round(9)
    return truncar_decimal(vp.sum(), 6)


def taxas_zero(  # noqa: PLR0913, PLR0914, PLR0917
    data_liquidacao: DateLike,
    vencimentos_ltn: DatesLike,
    taxas_ltn: ArrayLike,
    vencimentos_ntnf: DatesLike,
    taxas_ntnf: ArrayLike,
    incluir_cupons: bool = False,
) -> pl.DataFrame:
    r"""
    Calcula as taxas zero cupom (spot) para NTN-F usando bootstrap.

    O bootstrap determina as taxas zero a partir das TIRs dos títulos.
    O método resolve iterativamente as taxas que descontam os fluxos ao preço.
    Usa as LTNs (zero cupom) até o último vencimento LTN disponível. Após
    isso, calcula as taxas zero a partir das NTN-F.

    Args:
        data_liquidacao (DateLike): Data de liquidação para o cálculo.
        vencimentos_ltn (DatesLike): Datas de vencimento das LTNs usadas como
            vértices prefixados zero cupom.
        taxas_ltn (ArrayLike): Taxas das LTNs. Como a LTN é zero cupom, essas
            taxas são usadas diretamente como taxas zero no bootstrap.
        vencimentos_ntnf (DatesLike): Datas de vencimento das NTN-F usadas no
            bootstrap.
        taxas_ntnf (ArrayLike): TIRs das NTN-F correspondentes aos vencimentos
            informados.
        incluir_cupons (bool): Se True, inclui todas as datas de pagamento
            semestrais até o último vencimento, e não só os vencimentos
            informados. Padrão False.

    Returns:
        pl.DataFrame: DataFrame com colunas `data_vencimento`, `dias_uteis`
            e `taxa_zero`.

    Raises:
        ValueError: Se vencimentos e taxas tiverem tamanhos diferentes ou se
            um mesmo vencimento aparecer com taxas diferentes.

    Output Columns:
        - data_vencimento (Date): Data de vencimento.
        - dias_uteis (Int64): Dias úteis entre liquidação e vencimento.
        - taxa_zero (Float64): Taxa zero (zero cupom), em formato decimal.

    Notes:
        Para cada NTN-F considerada no bootstrap, o preço teórico é:

        \[
        \mathrm{PU} = \sum_{i=1}^{n}
        \frac{\mathrm{FC}_i}{(1+y)^{t_i}}
        \]

        onde:

        - \(\mathrm{FC}_i\): valor do pagamento \(i\), incluindo cupom e
          amortização quando aplicável.
        - \(\mathrm{DU}_i\): dias úteis entre liquidação e pagamento \(i\).
        - \(t_i = \mathrm{DU}_i/252\): prazo até o pagamento \(i\), em anos
          de 252 dias úteis.
        - \(n\): número de pagamentos restantes.
        - \(y\): TIR obtida por interpolação `flat_forward` das TIRs
          informadas para NTN-F.

        Após o último vencimento LTN, o bootstrap encontra a taxa zero do
        pagamento final usando as taxas já determinadas para os pagamentos
        anteriores:

        \[
        r_n = \left(
        \frac{\mathrm{FC}_n}
        {\mathrm{PU}-\sum_{i=1}^{n-1}\mathrm{FC}_i/(1+r_i)^{t_i}}
        \right)^{1/t_n}-1
        \]

        onde \(r_i := r(t_i)\) é a taxa zero para o prazo do pagamento \(i\).

        As taxas de entrada e de saída são anuais efetivas, em decimal, na
        base de 252 dias úteis. Para a LTN, a TIR coincide com a taxa zero,
        pois há apenas um pagamento. O preço teórico não aplica os
        arredondamentos e truncamentos do PU oficial.

        Vértices com taxa nula ou NaN e vencimentos menores ou iguais à
        liquidação são ignorados antes da construção dos interpoladores e
        não aparecem no resultado.

    Examples:
        >>> from pyield import ntnf, ltn
        >>> import polars.selectors as cs
        >>> df_ltn = ltn.dados("03-09-2024")
        >>> df_ntnf = ntnf.dados("03-09-2024")
        >>> curva = ntnf.taxas_zero(
        ...     data_liquidacao="03-09-2024",
        ...     vencimentos_ltn=df_ltn["data_vencimento"],
        ...     taxas_ltn=df_ltn["taxa_indicativa"],
        ...     vencimentos_ntnf=df_ntnf["data_vencimento"],
        ...     taxas_ntnf=df_ntnf["taxa_indicativa"],
        ... )
        >>> curva.with_columns(cs.starts_with("taxa_") * 100)
        shape: (6, 3)
        ┌─────────────────┬────────────┬───────────┐
        │ data_vencimento ┆ dias_uteis ┆ taxa_zero │
        │ ---             ┆ ---        ┆ ---       │
        │ date            ┆ i64        ┆ f64       │
        ╞═════════════════╪════════════╪═══════════╡
        │ 2025-01-01      ┆ 83         ┆ 10.8837   │
        │ 2027-01-01      ┆ 584        ┆ 11.998105 │
        │ 2029-01-01      ┆ 1083       ┆ 12.211302 │
        │ 2031-01-01      ┆ 1584       ┆ 12.223069 │
        │ 2033-01-01      ┆ 2088       ┆ 12.135544 │
        │ 2035-01-01      ┆ 2587       ┆ 12.139796 │
        └─────────────────┴────────────┴───────────┘
    """
    if any_is_empty(
        data_liquidacao,
        vencimentos_ltn,
        taxas_ltn,
        vencimentos_ntnf,
        taxas_ntnf,
    ):
        return pl.DataFrame(schema=_SCHEMA_TAXAS_ZERO)
    # 1. Converter e normalizar inputs para Polars
    liquidacao = conversores.converter_datas(data_liquidacao)
    vencimentos_ltn = conversores.converter_datas(vencimentos_ltn)
    vencimentos_ntnf = conversores.converter_datas(vencimentos_ntnf)
    vencimentos_ltn, taxas_ltn = _utils.filtrar_vertices_futuros(
        liquidacao, vencimentos_ltn, taxas_ltn, "LTN"
    )
    vencimentos_ntnf, taxas_ntnf = _utils.filtrar_vertices_futuros(
        liquidacao, vencimentos_ntnf, taxas_ntnf, "NTN-F"
    )
    if vencimentos_ltn.is_empty() or vencimentos_ntnf.is_empty():
        return pl.DataFrame(schema=_SCHEMA_TAXAS_ZERO)

    # 2. Criar interpoladores (aceitam pl.Series diretamente)
    interpolador_ltn = interpolador.Interpolador(
        dias_uteis=du.contar(liquidacao, vencimentos_ltn),
        taxas=taxas_ltn,
        metodo="flat_forward",
    )
    interpolador_ntnf = interpolador.Interpolador(
        dias_uteis=du.contar(liquidacao, vencimentos_ntnf),
        taxas=taxas_ntnf,
        metodo="flat_forward",
    )

    # 3. Gerar todas as datas de pagamento até o último vencimento NTN-F
    ultimo_vencimento = vencimentos_ntnf.max()
    assert isinstance(ultimo_vencimento, dt.date)
    todas_datas_pagamento = datas_pagamento(liquidacao, ultimo_vencimento)

    # 4. Construir DataFrame inicial
    df = (
        pl.DataFrame({"data_vencimento": todas_datas_pagamento})
        .with_columns(dias_uteis=du.contar_expr(liquidacao, "data_vencimento"))
        .with_columns(
            anos_uteis=pl.col("dias_uteis") / 252,
            taxa_tir=interpolador_ntnf.interpolar_expr("dias_uteis"),
        )
    )

    # 5. Loop de bootstrap (iterativo por dependência sequencial)
    ultimo_vencimento_ltn = vencimentos_ltn.max()
    assert isinstance(ultimo_vencimento_ltn, dt.date)

    # As linhas vêm em ordem crescente de data, e o dicionário preserva essa
    # ordem: cada vértice só depende das taxas zero dos anteriores.
    taxas_zero_por_data: dict[dt.date, float] = {}

    for data_vertice, dias_uteis, anos_uteis, tir in df.iter_rows():
        # Até o último vencimento LTN (inclusive), a taxa zero vem das LTNs
        if data_vertice <= ultimo_vencimento_ltn:
            taxas_zero_por_data[data_vertice] = interpolador_ltn(dias_uteis)
            continue

        # Cupons anteriores ao pagamento final, com taxas zero já resolvidas.
        # Sem cupons anteriores, o valor presente é zero e a taxa zero é a TIR.
        datas_cupom = datas_pagamento(liquidacao, data_vertice)[:-1]
        valor_presente_cupons = _utils.calcular_pv(
            fluxos_caixa=[VALOR_CUPOM] * len(datas_cupom),
            taxas=[taxas_zero_por_data[d] for d in datas_cupom],
            prazos=du.contar(liquidacao, datas_cupom) / 252,
        )
        preco_titulo = _valor_presente(liquidacao, data_vertice, tir)
        fator_preco = VALOR_FINAL / (preco_titulo - valor_presente_cupons)
        taxas_zero_por_data[data_vertice] = fator_preco ** (1 / anos_uteis) - 1

    # 6. Selecionar colunas finais com a taxa zero de cada vértice
    df = df.select(
        "data_vencimento",
        "dias_uteis",
        taxa_zero=pl.Series(list(taxas_zero_por_data.values()), dtype=pl.Float64),
    )

    # 7. Manter apenas os vencimentos informados se os cupons não forem pedidos
    if not incluir_cupons:
        df = df.filter(pl.col("data_vencimento").is_in(vencimentos_ntnf.implode()))

    return df


def _fluxos_com_taxa_di(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    vencimentos_di: DatesLike,
    taxas_di: ArrayLike,
) -> pl.DataFrame:
    """Fluxos da NTN-F com prazo em anos úteis e taxa DI de cada pagamento.

    A taxa DI é interpolada por flat forward e fica nula nos pagamentos além
    do último vértice da curva. Sem pagamentos, retorna o DataFrame vazio.
    """
    df = fluxos_caixa(data_liquidacao, data_vencimento)
    if df.is_empty():
        return df

    interpolador_di = interpolador.Interpolador(
        du.contar(data_liquidacao, vencimentos_di), taxas_di, "flat_forward"
    )
    return df.with_columns(
        dias_uteis=du.contar(data_liquidacao, df["data_pagamento"]),
    ).with_columns(
        anos_uteis=pl.col("dias_uteis") / 252,
        taxa_di=interpolador_di.interpolar_expr("dias_uteis"),
    )


def rentabilidade(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa_ntnf: float | Decimal | str,
    vencimentos_di: DatesLike,
    taxas_di: ArrayLike,
) -> float:
    r"""
    Calcula a rentabilidade de uma NTN-F sobre a curva DI.

    Compara as taxas diárias equivalentes da TIR da NTN-F e da TIR de referência
    obtida pela curva DI, conforme o Anexo 2 de *Dívida Pública: a experiência
    brasileira*, do Tesouro Nacional. Interpola as taxas DI nas datas de pagamento,
    calcula o valor presente dos fluxos contratuais usando essas taxas e encontra
    a TIR única que reproduz esse preço de referência.

    Args:
        data_liquidacao (DateLike): Data de liquidação para o cálculo.
        data_vencimento (DateLike): Data de vencimento da NTN-F.
        taxa_ntnf (float | Decimal | str): TIR anual efetiva da NTN-F, em
            formato decimal.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
        vencimentos_di (DatesLike): Datas de vencimento da curva DI.
        taxas_di (ArrayLike): Taxas DI anuais efetivas, em formato decimal,
            correspondentes aos vencimentos.

    Returns:
        float: Razão entre as taxas diárias equivalentes. Por exemplo, 1.01
            representa 101% da taxa diária equivalente de referência DI.
            Retorna NaN para entradas ausentes, fluxos vazios ou preço calculado
            NaN. Também retorna NaN se a resolução numérica falhar. Se a TIR
            de referência DI for zero, retorna ``inf`` para TIR positiva da
            NTN-F e ``0.0`` nos demais casos.

    Notes:
        **Preço de referência pela curva DI**

        O preço de referência é:

        \[
        P_{\mathrm{DI}} = \sum_{i=1}^{n}
        \frac{\mathrm{FC}_i}{(1+\mathrm{DI}_i)^{t_i}}
        \]

        onde:

        - \(\mathrm{FC}_i\): valor do pagamento \(i\), incluindo cupom e
          amortização quando aplicável.
        - \(\mathrm{DU}_i\): dias úteis entre liquidação e pagamento \(i\).
        - \(t_i = \mathrm{DU}_i/252\): prazo até o pagamento \(i\), em anos
          de 252 dias úteis.
        - \(n\): número de pagamentos restantes.
        - \(\mathrm{DI}(t)\): taxa DI para o prazo \(t\), anual efetiva e
          em decimal, na base de 252 dias úteis.
        - \(\mathrm{DI}_i := \mathrm{DI}(t_i)\): taxa DI para o prazo do
          pagamento \(i\), obtida por interpolação `flat_forward`.

        **TIR equivalente da referência**

        A função resolve numericamente a TIR anual efetiva
        \(y_{\mathrm{DI}}\), em decimal, que desconta os mesmos fluxos ao
        preço de referência:

        \[
        \sum_{i=1}^{n}
        \frac{\mathrm{FC}_i}{(1+y_{\mathrm{DI}})^{t_i}}
        = P_{\mathrm{DI}}
        \]

        Essa TIR é uma taxa única para todos os pagamentos, enquanto cada
        \(\mathrm{DI}_i\) corresponde
        ao prazo do pagamento \(i\).

        **Rentabilidade relativa**

        A rentabilidade relativa \(q\) é a razão entre as taxas diárias
        equivalentes:

        \[
        q = \frac{(1+y_{\mathrm{NTNF}})^{1/252}-1}
        {(1+y_{\mathrm{DI}})^{1/252}-1}
        \]

        onde \(y_{\mathrm{NTNF}}\) é a TIR informada em `taxa_ntnf`.

        O indicador compara taxas implícitas; não representa o retorno realizado
        entre compra e venda. Também não calibra um spread ou um multiplicador
        uniforme sobre os vértices da curva DI.

        Os fluxos mantêm as datas contratuais, excluem pagamentos na liquidação
        e incluem o vencimento. O preço de referência não aplica os arredondamentos
        e truncamentos do PU oficial. Não há extrapolação na ponta longa da curva;
        pagamentos além do último vértice DI resultam em NaN.

        ??? note "Comparação com a LTN"

            A LTN possui um único pagamento. Nesse caso, a TIR equivalente da
            referência coincide com a taxa zero DI do vencimento, e a fórmula
            se reduz à utilizada por `yd.ltn.rentabilidade`. Na NTN-F, a TIR
            equivalente considera também os cupons intermediários.

        Referência: Tesouro Nacional, *Dívida Pública: a experiência brasileira*,
        Parte 3, capítulo 2, Anexo 2, item 2, páginas 329–330:
        [Prêmio das NTN-Fs](https://thot-arquivos.tesouro.gov.br/publicacao-anexo/4710).

    Examples:
        >>> from pyield import ntnf
        >>> # Obs: apenas algumas taxas DI serão usadas no exemplo.
        >>> vencimentos_di = ["2025-01-01", "2030-01-01", "2035-01-01"]
        >>> taxas_di = [0.10823, 0.11594, 0.11531]
        >>> ntnf.rentabilidade(
        ...     data_liquidacao="23-08-2024",
        ...     data_vencimento="01-01-2035",
        ...     taxa_ntnf="11.6586%",
        ...     vencimentos_di=vencimentos_di,
        ...     taxas_di=taxas_di,
        ... )
        1.0099602280683393
    """
    taxa_ntnf = converter_taxa(taxa_ntnf)
    if any_is_empty(
        data_liquidacao,
        data_vencimento,
        taxa_ntnf,
        vencimentos_di,
        taxas_di,
    ):
        return float("nan")
    taxa_ntnf = float(taxa_ntnf)

    df = _fluxos_com_taxa_di(data_liquidacao, data_vencimento, vencimentos_di, taxas_di)
    if df.is_empty():
        return float("nan")

    # Taxa DI nula (pagamento além do último vértice) resulta em preço NaN
    preco_referencia = _utils.calcular_pv(
        fluxos_caixa=df["valor_pagamento"],
        taxas=df["taxa_di"],
        prazos=df["anos_uteis"],
    )

    if math.isnan(preco_referencia):
        return float("nan")

    def diferenca_preco(taxa: float) -> float:
        fluxos_descontados = df["valor_pagamento"] / (1 + taxa) ** df["anos_uteis"]
        return float(fluxos_descontados.sum()) - preco_referencia

    tir_di = _utils.encontrar_raiz(diferenca_preco)

    fator_ntnf = (1 + taxa_ntnf) ** (1 / 252)
    fator_di = (1 + tir_di) ** (1 / 252)
    if fator_di == 1:
        return float("inf") if fator_ntnf > 1 else 0.0

    return (fator_ntnf - 1) / (fator_di - 1)


def rentabilidade_expr(
    data_liquidacao: DateLike,
    data_vencimento: pl.Expr | str,
    taxa_ntnf: pl.Expr | str,
    vencimentos_di: DatesLike,
    taxas_di: ArrayLike,
) -> pl.Expr:
    """Cria expressão Polars para a rentabilidade da NTN-F sobre a curva DI.

    O cálculo é aplicado linha a linha porque a rentabilidade depende dos fluxos
    de caixa do título, da interpolação da curva DI e da resolução de raiz.
    Falhas de resolução retornam NaN apenas na linha afetada. Outros erros
    são propagados.

    Args:
        data_liquidacao: Data de liquidação para o cálculo.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento da NTN-F.
        taxa_ntnf: Nome de coluna ou expressão Polars com a TIR anual efetiva
            da NTN-F, em decimal, na base de 252 dias úteis.
        vencimentos_di: Datas de vencimento da curva DI.
        taxas_di: Taxas DI anuais efetivas, em decimal, na base de 252 dias
            úteis, correspondentes aos vencimentos.

    Returns:
        pl.Expr: Expressão sem alias com a rentabilidade da NTN-F sobre a curva
        DI.
    """
    return pl.struct(
        _utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        _utils.coluna_ou_expr(taxa_ntnf, "taxa_ntnf"),
    ).map_elements(
        lambda row: rentabilidade(
            data_liquidacao=data_liquidacao,
            data_vencimento=row["data_vencimento"],
            taxa_ntnf=row["taxa_ntnf"],
            vencimentos_di=vencimentos_di,
            taxas_di=taxas_di,
        ),
        return_dtype=pl.Float64,
    )


def premio_limpo(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa_ntnf: float | Decimal | str,
    vencimentos_di: DatesLike,
    taxas_di: ArrayLike,
) -> float:
    r"""
    Calcula o spread líquido (prêmio limpo no jargão de mercado) da NTN-F sobre a
    curva DI.

    A função determina o spread que iguala o valor presente dos fluxos ao preço
    do título. Interpola as taxas DI nas datas de pagamento e encontra o spread
    que zera a diferença de preços, retornado em decimal e conversível para bps.

    Args:
        data_liquidacao (DateLike): Data de liquidação para o cálculo.
        data_vencimento (DateLike): Data de vencimento do título.
        taxa_ntnf (float | Decimal | str): TIR anual efetiva, em decimal, na base de
            252 dias úteis.
            Aceita também percentual explícito: "5.75%" ou "5,75%".
        vencimentos_di (DatesLike): Vencimentos da curva DI.
        taxas_di (ArrayLike): Taxas DI anuais efetivas, em decimal, na base
            de 252 dias úteis, correspondentes aos vencimentos.

    Returns:
        float: Spread líquido em formato decimal (ex.: 0.0012 = 12 bps).
            Retorna NaN para entradas ausentes ou fluxos vazios.
            Também retorna NaN se a resolução numérica falhar ou se houver
            pagamento além do último vértice da curva DI, pois não há
            extrapolação na ponta longa.

    Notes:
        O spread \(s\), adicionado a cada taxa DI anual em decimal,
        é a solução de:

        \[
        \sum_{i=1}^{n}
        \frac{\mathrm{FC}_i}{(1+\mathrm{DI}_i+s)^{t_i}}
        = \mathrm{PU} = \sum_{i=1}^{n}
        \frac{\mathrm{FC}_i}{(1+y)^{t_i}}
        \]

        onde:

        - \(\mathrm{FC}_i\): valor do pagamento \(i\), incluindo cupom e
          amortização quando aplicável.
        - \(\mathrm{DU}_i\): dias úteis entre liquidação e pagamento \(i\).
        - \(t_i = \mathrm{DU}_i/252\): prazo até o pagamento \(i\), em anos
          de 252 dias úteis.
        - \(n\): número de pagamentos restantes.
        - \(y\): TIR anual efetiva informada em `taxa_ntnf`, em decimal,
          na base de 252 dias úteis.
        - \(\mathrm{DI}_i := \mathrm{DI}(t_i)\): taxa DI anual efetiva,
          em decimal, para o prazo do pagamento \(i\), obtida por
          interpolação `flat_forward`, na base de 252 dias úteis.

        Para expressar o spread em pontos-base, multiplique por 10.000. Os preços
        são teóricos, sem os arredondamentos e truncamentos do PU oficial.

    Examples:
        >>> from pyield import ntnf
        >>> # Obs: apenas algumas taxas DI serão usadas no exemplo.
        >>> vencimentos_di = ["2025-01-01", "2030-01-01", "2035-01-01"]
        >>> taxas_di = [0.10823, 0.11594, 0.11531]
        >>> spread = ntnf.premio_limpo(
        ...     data_liquidacao="23-08-2024",
        ...     data_vencimento="01-01-2035",
        ...     taxa_ntnf=0.116586,
        ...     vencimentos_di=vencimentos_di,
        ...     taxas_di=taxas_di,
        ... )
        >>> round(spread * 10_000, 2)  # Converte para bps para exibição
        12.13
    """
    taxa_ntnf = converter_taxa(taxa_ntnf)
    if any_is_empty(
        data_liquidacao,
        data_vencimento,
        taxa_ntnf,
        vencimentos_di,
        taxas_di,
    ):
        return float("nan")
    taxa_ntnf = float(taxa_ntnf)

    df = _fluxos_com_taxa_di(data_liquidacao, data_vencimento, vencimentos_di, taxas_di)
    # A soma ignoraria fluxos com taxa DI nula e igualaria só parte deles ao preço
    if df.is_empty() or df["taxa_di"].has_nulls():
        return float("nan")

    preco_titulo = _valor_presente(data_liquidacao, data_vencimento, taxa_ntnf)

    def diferenca_preco(spread: float) -> float:
        fluxos_descontados = (
            df["valor_pagamento"] / (1 + df["taxa_di"] + spread) ** df["anos_uteis"]
        )
        return float(fluxos_descontados.sum()) - preco_titulo

    return _utils.encontrar_raiz(diferenca_preco)


def premio_limpo_expr(
    data_liquidacao: DateLike,
    data_vencimento: pl.Expr | str,
    taxa_ntnf: pl.Expr | str,
    vencimentos_di: DatesLike,
    taxas_di: ArrayLike,
) -> pl.Expr:
    """Cria expressão Polars para o prêmio limpo da NTN-F sobre a curva DI.

    O cálculo é aplicado linha a linha porque o prêmio limpo depende dos fluxos
    de caixa do título, da interpolação da curva DI e da resolução de raiz.
    Falhas de resolução retornam NaN apenas na linha afetada. Outros erros
    são propagados.

    Args:
        data_liquidacao: Data de liquidação para o cálculo.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento da NTN-F.
        taxa_ntnf: Nome de coluna ou expressão Polars com a TIR anual efetiva
            da NTN-F, em decimal, na base de 252 dias úteis.
        vencimentos_di: Datas de vencimento da curva DI.
        taxas_di: Taxas DI anuais efetivas, em decimal, na base de 252 dias
            úteis, correspondentes aos vencimentos.

    Returns:
        pl.Expr: Expressão sem alias com o prêmio limpo da NTN-F sobre a curva
        DI.
    """
    return pl.struct(
        _utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        _utils.coluna_ou_expr(taxa_ntnf, "taxa_ntnf"),
    ).map_elements(
        lambda row: premio_limpo(
            data_liquidacao=data_liquidacao,
            data_vencimento=row["data_vencimento"],
            taxa_ntnf=row["taxa_ntnf"],
            vencimentos_di=vencimentos_di,
            taxas_di=taxas_di,
        ),
        return_dtype=pl.Float64,
    )


def duration(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float | Decimal | str,
) -> float:
    r"""
    Calcula a Macaulay duration de uma NTN-F em anos úteis.

    Args:
        data_liquidacao (DateLike): Data de liquidação para o cálculo.
        data_vencimento (DateLike): Data de vencimento do título.
        taxa (float | Decimal | str): TIR anual efetiva, em decimal, na base de 252 dias
            úteis, usada para descontar os fluxos.
            Aceita também percentual explícito: "5.75%" ou "5,75%".

    Returns:
        float: Macaulay duration em anos úteis. Retorna NaN para entradas
            ausentes ou quando não há pagamentos após a liquidação.

    Notes:
        A Macaulay duration \(D\), em anos de 252 dias úteis, é:

        \[
        D = \frac{
        \sum_{i=1}^{n}t_i\,\mathrm{FC}_i/(1+y)^{t_i}}
        {\mathrm{PU}}
        \]

        onde:

        - \(\mathrm{FC}_i\): valor do pagamento \(i\), incluindo cupom e
          amortização quando aplicável.
        - \(\mathrm{DU}_i\): dias úteis entre liquidação e pagamento \(i\).
        - \(t_i = \mathrm{DU}_i/252\): prazo até o pagamento \(i\), em anos
          de 252 dias úteis.
        - \(n\): número de pagamentos restantes.
        - \(y\): TIR anual efetiva informada em `taxa`, em decimal, na
          base de 252 dias úteis.
        - \(\mathrm{PU}\): soma dos fluxos descontados pela TIR \(y\).

        Os fluxos descontados não aplicam os arredondamentos e truncamentos
        do PU oficial.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.duration("02-09-2024", "01-01-2035", "12.1785%")
        6.32854218039796
    """
    taxa = converter_taxa(taxa)
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")
    taxa = float(taxa)

    df_fluxos = fluxos_caixa(data_liquidacao, data_vencimento)
    if df_fluxos.is_empty():
        return float("nan")

    anos_uteis = du.contar(data_liquidacao, df_fluxos["data_pagamento"]) / 252
    vp = df_fluxos["valor_pagamento"] / (1 + taxa) ** anos_uteis
    return float((vp * anos_uteis).sum()) / float(vp.sum())


def duration_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
    taxa: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para a duration da NTN-F.

    O cálculo é aplicado linha a linha porque a duration depende dos fluxos de
    caixa do título.

    Args:
        data_liquidacao: Nome de coluna ou expressão Polars com a data de
            liquidação.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento.
        taxa: Nome de coluna ou expressão Polars com a TIR anual efetiva,
            em decimal, na base de 252 dias úteis.

    Returns:
        pl.Expr: Expressão sem alias com a Macaulay duration em anos úteis.
    """
    return pl.struct(
        _utils.coluna_ou_expr(data_liquidacao, "data_liquidacao"),
        _utils.coluna_ou_expr(data_vencimento, "data_vencimento"),
        _utils.coluna_ou_expr(taxa, "taxa"),
    ).map_elements(
        lambda s: duration(
            s["data_liquidacao"],
            s["data_vencimento"],
            s["taxa"],
        ),
        return_dtype=pl.Float64,
    )


def dv01(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    taxa: float | Decimal | str,
) -> float:
    r"""
    Calcula o DV01 (Dollar Value of 01) de uma NTN-F em R$.

    Representa a redução do PU teórico para um aumento de 1 bp (0,01 ponto
    percentual) na taxa, calculada pela diferença entre os preços antes e depois
    do aumento.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        taxa (float | Decimal | str): TIR anual efetiva, em decimal, na base
            de 252 dias úteis.
            Aceita também percentual explícito: "5.75%" ou "5,75%".

    Returns:
        float: DV01, variação de preço para 1 bp. Retorna NaN para entradas
            ausentes ou quando não há pagamentos após a liquidação.

    Notes:
        Mantendo liquidação e vencimento fixos, os fluxos e seus prazos
        também ficam fixos. Nesse contexto, o preço teórico em função da TIR é:

        \[
        \mathrm{PU}(y) = \sum_{i=1}^{n}\frac{\mathrm{FC}_i}{(1+y)^{t_i}}
        \]

        O \(\mathrm{DV01}\) é a redução do preço, em R$, para um aumento
        de 1 bp na TIR:

        \[
        \mathrm{DV01} = \mathrm{PU}(y) - \mathrm{PU}(y+0.0001)
        \]

        onde:

        - \(\mathrm{FC}_i\): valor do pagamento \(i\), incluindo cupom e
          amortização quando aplicável.
        - \(\mathrm{DU}_i\): dias úteis entre liquidação e pagamento \(i\).
        - \(t_i = \mathrm{DU}_i/252\): prazo até o pagamento \(i\), em anos
          de 252 dias úteis.
        - \(n\): número de pagamentos restantes.
        - \(y\): TIR anual efetiva informada em `taxa`, em decimal, na
          base de 252 dias úteis.
        - \(0.0001\): aumento de 1 bp na TIR em decimal.

        Os preços não aplicam os arredondamentos e truncamentos do PU oficial.

    Examples:
        >>> from pyield import ntnf
        >>> ntnf.dv01("26-03-2025", "01-01-2035", "15.1375%")
        0.39025157587195736
    """
    taxa = converter_taxa(taxa)
    if any_is_empty(data_liquidacao, data_vencimento, taxa):
        return float("nan")

    pu1 = _valor_presente(data_liquidacao, data_vencimento, taxa)
    pu2 = _valor_presente(data_liquidacao, data_vencimento, float(taxa) + 0.0001)
    return pu1 - pu2


def dv01_expr(
    data_liquidacao: pl.Expr | str,
    data_vencimento: pl.Expr | str,
    taxa: pl.Expr | str,
) -> pl.Expr:
    """Cria expressão Polars para o DV01 da NTN-F.

    O cálculo é aplicado linha a linha e reprecifica o PU teórico para um
    aumento de 1 bp na taxa.

    Args:
        data_liquidacao: Nome de coluna ou expressão Polars com a data de
            liquidação.
        data_vencimento: Nome de coluna ou expressão Polars com a data de
            vencimento.
        taxa: Nome de coluna ou expressão Polars com a TIR anual efetiva,
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


def taxa(
    data_liquidacao: DateLike,
    data_vencimento: DateLike,
    pu: float | Decimal,
) -> float:
    r"""
    Calcula a TIR implícita de uma NTN-F a partir de um PU informado.

    A função resolve numericamente a taxa que aproxima o PU informado pelo
    valor presente bruto dos fluxos. Como o valor presente não é arredondado
    nem truncado, o resultado não pretende reproduzir exatamente a taxa usada
    para gerar um PU canônico por ``pu()``.

    Args:
        data_liquidacao (DateLike): Data de liquidação.
        data_vencimento (DateLike): Data de vencimento.
        pu: Preço unitário (PU) do título.

    Returns:
        float: TIR implícita em formato decimal, sem arredondamento. Retorna
            NaN para entradas ausentes, PU não positivo ou falha na resolução
            numérica.

    Notes:
        A TIR implícita \(y\), anual efetiva, em decimal e na base de
        252 dias úteis, é a solução de:

        \[
        \sum_{i=1}^{n}
        \frac{\mathrm{FC}_i}{(1+y)^{t_i}}
        = \mathrm{PU}_{\mathrm{informado}}
        \]

        onde:

        - \(\mathrm{FC}_i\): valor do pagamento \(i\), incluindo cupom e
          amortização quando aplicável.
        - \(\mathrm{DU}_i\): dias úteis entre liquidação e pagamento \(i\).
        - \(t_i = \mathrm{DU}_i/252\): prazo até o pagamento \(i\), em anos
          de 252 dias úteis.
        - \(n\): número de pagamentos restantes.
        - \(\mathrm{PU}_{\mathrm{informado}}\): preço recebido em `pu`.

        A soma representa o preço teórico, sem os arredondamentos e truncamentos
        do PU oficial.

    Examples:
        A taxa é retornada em formato decimal; multiplique por 100 para exibi-la
        em percentual:

        >>> from pyield import ntnf
        >>> ntnf.taxa("13-03-2026", "01-01-2035", 820.995125)
        0.1427430001649857
        >>> ntnf.taxa("21-05-2008", "01-01-2014", 903.039091) * 100
        13.661101023197176
    """
    if any_is_empty(data_liquidacao, data_vencimento, pu):
        return float("nan")

    pu_float = float(pu)
    if pu_float <= 0:
        return float("nan")

    def diferenca_preco(taxa_candidata: float) -> float:
        return (
            _valor_presente(data_liquidacao, data_vencimento, taxa_candidata) - pu_float
        )

    return _utils.encontrar_raiz(diferenca_preco)


__all__ = [
    "dados",
    "datas_pagamento",
    "duration",
    "duration_expr",
    "dv01",
    "dv01_expr",
    "fluxos_caixa",
    "premio_limpo",
    "premio_limpo_expr",
    "pu",
    "rentabilidade",
    "rentabilidade_expr",
    "taxa",
    "taxas_zero",
    "taxas_zero_forwards",
    "vencimentos",
]


def __dir__() -> list[str]:
    return __all__
