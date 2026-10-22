"""Operações de VNA selecionadas pelo título."""

import datetime as dt
from decimal import ROUND_HALF_UP, Decimal, localcontext
from typing import Literal

import polars as pl

from pyield import du
from pyield._internal.converters import converter_datas
from pyield._internal.numbers import para_decimal, truncar_decimal
from pyield._internal.types import DateLike, any_is_empty
from pyield.vna import _calculo, _lft, _ntnb, _ntnc

TipoTitulo = Literal["LFT", "NTN-B", "NTN-C"]

_VNA_ORIGINAL = Decimal("1000")
_INDICE_BASE_IPCA = Decimal("1614.62")  # Junho de 2000; base em 15/07/2000.
_INDICE_BASE_IGPM = Decimal("183.745")  # Junho de 2000; base em 01/07/2000.
_IGPM_NOVEMBRO_1999 = Decimal("174.939")
_IGPM_DEZEMBRO_1999 = Decimal("178.099")


def _modulo_mensal(titulo: TipoTitulo):
    if titulo == "NTN-B":
        return _ntnb
    if titulo == "NTN-C":
        return _ntnc
    raise ValueError("Esta operação aceita apenas NTN-B e NTN-C.")


def calcular_base(
    titulo: TipoTitulo,
    indice_atual: float | Decimal,
    vencimento: DateLike | None = None,
) -> Decimal:
    """Calcula o VNA realizado da LFT, NTN-B ou NTN-C desde sua base original.

    Permite calcular a referência mensal após a divulgação do índice pelo
    IBGE (IPCA) ou pela FGV (IGP-M), mesmo antes da publicação do VNA pelo
    Tesouro Nacional. Não consulta a rede e não calcula pró-rata entre
    datas-base. As bases históricas são selecionadas internamente pelo título
    e, para NTN-C, pela série de vencimento.
    Para LFT, recebe o fator Selic já acumulado desde 01/07/2000.

    Args:
        titulo: LFT, NTN-B ou NTN-C.
        indice_atual: Número-índice realizado do IPCA (NTN-B) ou IGP-M
            (NTN-C), do mês anterior à nova data-base. Por exemplo, julho
            de 2026 para o VNA de 15/08/2026 da NTN-B ou 01/08/2026 da NTN-C.
            Aceita float ou Decimal; não recebe taxa percentual de inflação.
            Para LFT, é o fator multiplicativo Selic acumulado desde
            01/07/2000 até a data final exclusiva de vigência. Não é taxa
            diária, taxa anual ou fator de apenas um dia.
        vencimento: Obrigatório para selecionar a série da NTN-C; aceita
            os formatos de data da biblioteca. A seleção usa o ano de
            vencimento. Deve ser omitido para LFT e NTN-B.

    Returns:
        Decimal: VNA-base calculado, truncado em seis casas decimais. Retorna
            ``Decimal('NaN')`` se não houver série de NTN-C para o vencimento.

    Notes:
        Para NTN-B e NTN-C, conforme a metodologia da STN, calcula
        ``T6(vna_base * T16(indice_atual / indice_base))``. Primeiro trunca
        o fator acumulado em dezesseis casas; depois multiplica pela base
        e trunca o VNA em seis casas. Não arredonda os números-índice nem
        usa a taxa mensal percentual arredondada. O cálculo usa 28 dígitos
        de precisão, independentemente do contexto Decimal do chamador.
        Entradas float são convertidas pela representação textual.

        Para LFT, calcula ``T6(1000 * A16(indice_atual))``: arredonda o
        fator acumulado em dezesseis casas (meio para cima) e trunca o VNA
        em seis casas, com precisão local de 28 dígitos. A base de R$ 1.000
        em 01/07/2000 é constante interna. O fator resulta do produtório
        de ``1 + taxa_diaria``; a série SGS 11 publica a taxa em percentual
        ao dia, que deve ser dividida por 100. Preserve a precisão do
        produtório; truncar o VNA diariamente acumula perda.

        O relatório APC238 gerado pelo BCB em um dia informa o VNA para o
        próximo dia útil. Por exemplo, o arquivo de 31/05/2024 informa o VNA
        válido para 03/06/2024 e inclui a taxa Selic de 31/05/2024. A consulta
        ``yd.vna.valor("LFT", data)`` seleciona a data do arquivo. Esta função
        não recebe datas nem acumula a série diária automaticamente.

        A NTN-B usa R$ 1.000 em 15/07/2000 e IPCA de junho de 2000
        (1614,62). A NTN-C dos vencimentos 2005, 2008, 2011, 2017, 2021 e
        2031 usa R$ 1.000 em 01/07/2000 e IGP-M de junho de 2000 (183,745).
        São constantes internas; o usuário informa apenas o índice final.

        Para NTN-C de 2002/2006, a origem é a emissão de 03/12/1999. A base
        ajustada é ``1000 * (178.099 / 174.939) ** T14(29 / 31)``, mantida
        sem truncamento intermediário, com índice-base de dezembro de 1999
        (178,099). As constantes de IGP-M foram verificadas com a série
        FGV/Ipeadata ``IGP12_IGPM12``. Esse ajuste não é uma projeção mensal.

        Usar o VNA mensal anterior já truncado como base perde precisão e
        pode diferir da série oficial na sexta casa. Encadear esses resultados
        acumula essa perda. Para reconstruir a série, a função mantém a base
        original e seu número-índice fixos. O resultado é calculado, não consultado na
        publicação do Tesouro; eventuais divergências devem ser investigadas.

    References:
        - https://crdcj.github.io/PYield/referencias/metodologia-calculo-tpf-stn/
        - https://crdcj.github.io/PYield/referencias/metodologia-calculo-letras-notas-stn/
        - https://sidra.ibge.gov.br/tabela/1737

    Raises:
        ValueError: Se o título for inválido, o índice não for positivo e
            finito, o vencimento da NTN-C estiver ausente ou malformado, ou
            houver vencimento informado para LFT ou NTN-B.

    Examples:
        Usa o número-índice do IBGE para calcular a base de 15/08/2026:

        >>> indice_atual = 7657.73  # yd.ipca.indice("01-07-2026")
        >>> calculado = yd.vna.calcular_base("NTN-B", indice_atual)
        >>> calculado
        Decimal('4742.744422')
        >>> calculado == yd.vna.valor("NTN-B", "15-08-2026")
        True

        Calcula a base de 01/10/2026 da NTN-C da série com vencimento em 2031:

        >>> indice_atual = 1225.846  # yd.igpm.indice("01-09-2026")
        >>> calculado = yd.vna.calcular_base(
        ...     "NTN-C", indice_atual, vencimento="01-01-2031"
        ... )
        >>> calculado
        Decimal('6671.452284')
        >>> calculado == yd.vna.valor("NTN-C", "01-10-2026", "01-01-2031")
        True

        Usa o fator acumulado da Selic até 31/05/2024 (inclusive), conferido
        com a série SGS 11, para obter o VNA válido em 03/06/2024:

        >>> fator_acumulado = 14.903011480282196
        >>> calculado = yd.vna.calcular_base("LFT", fator_acumulado)
        >>> calculado
        Decimal('14903.011480')
        >>> calculado == yd.vna.valor("LFT", "31-05-2024")
        True
    """
    if titulo not in {"LFT", "NTN-B", "NTN-C"}:
        raise ValueError("Título deve ser LFT, NTN-B ou NTN-C.")
    if titulo != "NTN-C" and vencimento is not None:
        raise ValueError("Vencimento aplica-se apenas à NTN-C.")
    if titulo == "LFT":
        fator = para_decimal(indice_atual)
        if not fator.is_finite() or fator <= 0:
            raise ValueError("O fator acumulado deve ser positivo e finito.")
        with localcontext(prec=_calculo.PRECISAO_CALCULO):
            fator = fator.quantize(Decimal("1e-16"), rounding=ROUND_HALF_UP)
            return truncar_decimal(_VNA_ORIGINAL * fator, 6)
    if titulo == "NTN-B":
        return _calculo.calcular_base(_VNA_ORIGINAL, _INDICE_BASE_IPCA, indice_atual)

    data_vencimento = converter_datas(vencimento)
    if data_vencimento is None:
        raise ValueError("Informe o vencimento para selecionar a série da NTN-C.")
    ano = data_vencimento.year
    with localcontext(prec=_calculo.PRECISAO_CALCULO):
        base = _VNA_ORIGINAL
        indice_base = _INDICE_BASE_IGPM
        if ano in _ntnc._ANOS_VENCIMENTO["column_3"]:
            expoente = truncar_decimal(Decimal(29) / 31, 14)
            base *= (_IGPM_DEZEMBRO_1999 / _IGPM_NOVEMBRO_1999) ** expoente
            indice_base = _IGPM_DEZEMBRO_1999
        elif ano not in _ntnc._ANOS_VENCIMENTO["column_2"]:
            return Decimal("NaN")
        return _calculo.calcular_base(base, indice_base, indice_atual)


def valor(
    titulo: TipoTitulo,
    data: DateLike | None = None,
    vencimento: DateLike | None = None,
    *,
    atualizar: bool = False,
) -> Decimal:
    """Consulta o VNA do título na data informada.

    Args:
        titulo: LFT, NTN-B ou NTN-C.
        data: Data de referência. Se nula, retorna ``Decimal('NaN')``.
        vencimento: Necessário para selecionar a série da NTN-C. Não se
            aplica aos demais títulos.
        atualizar: Se True, baixa novamente o arquivo de VNA e renova seu
            cache de 60 segundos. Não atualiza o cache dos índices do IPCA.

    Returns:
        Decimal: VNA com seis casas ou ``Decimal('NaN')`` na ausência de dados,
            conforme o contrato da consulta específica do título.

    Notes:
        Fontes: Banco Central (arquivo diário SELIC) para LFT; Tesouro
        Nacional para NTN-B e NTN-C. Entre referências, NTN-B usa os
        números-índice do IPCA do IBGE, e NTN-C usa a razão entre os VNAs.
        Não realiza projeção automática. NTN-C sem vencimento retorna NaN.

    Raises:
        ValueError: Se o título for inválido ou o vencimento for informado
            para um título diferente de NTN-C.

    Examples:
        Consulta uma referência mensal publicada da NTN-B:

        >>> yd.vna.valor("NTN-B", "15-12-2025")
        Decimal('4570.078408')

        Entre referências da NTN-C, calcula o pró-rata da série selecionada:

        >>> yd.vna.valor("NTN-C", "16-12-2025", vencimento="01-01-2031")
        Decimal('6449.641358')

        Para LFT, informa a data do arquivo diário do BCB. O arquivo de
        31/05/2024 contém o VNA válido no próximo dia útil, 03/06/2024:

        >>> yd.vna.valor("LFT", "31-05-2024")
        Decimal('14903.011480')
    """
    if titulo not in {"LFT", "NTN-B", "NTN-C"}:
        raise ValueError("Título deve ser LFT, NTN-B ou NTN-C.")
    if titulo == "NTN-C":
        return _ntnc.vna(data, vencimento, atualizar=atualizar)
    if vencimento is not None:
        raise ValueError("Vencimento aplica-se apenas à NTN-C.")
    if titulo == "LFT":
        return _lft.vna(data, atualizar=atualizar)
    return _ntnb.vna(data, atualizar=atualizar)


def historico(
    titulo: TipoTitulo,
    vencimento: DateLike | None = None,
    *,
    atualizar: bool = False,
) -> pl.DataFrame:
    """Busca os VNAs mensais publicados pelo Tesouro Nacional.

    Args:
        titulo: NTN-B ou NTN-C. Histórico de LFT não está disponível.
        vencimento: Filtro opcional de série para NTN-C. Se omitido, retorna
            todas as séries. Não se aplica à NTN-B.
        atualizar: Se True, baixa novamente o arquivo de VNA e renova seu
            cache de 60 segundos. Não atualiza o cache dos índices do IPCA.

    Returns:
        DataFrame Polars ordenado por data. Sem série correspondente ao
            vencimento, retorna DataFrame vazio com o mesmo schema.

    Output Columns:
        - data (Date): Data de referência publicada.
        - anos_vencimento (List[Int64]): Apenas NTN-C; anos da série.
        - vna (Decimal(18, 6)): Valor nominal atualizado publicado.

    Notes:
        Fonte: planilhas de Valor Nominal de NTN-B e NTN-C do Tesouro
        Transparente. Preserva o schema das consultas específicas por título.

    Raises:
        ValueError: Se o título não for NTN-B ou NTN-C, ou se houver filtro
            de vencimento para NTN-B.

    Examples:
        Seleciona duas referências mensais da NTN-B no histórico publicado:

        >>> referencias = yd.vna.historico("NTN-B")
        >>> referencias.filter(
        ...     pl.col("data").is_between(pl.date(2025, 12, 15), pl.date(2026, 1, 15))
        ... ).to_dicts()
        [{'data': datetime.date(2025, 12, 15), 'vna': Decimal('4570.078408')},
         {'data': datetime.date(2026, 1, 15), 'vna': Decimal('4585.159356')}]

        Para NTN-C, filtra o vencimento antes de interpolar para manter
        apenas uma série de VNA:

        >>> referencias = yd.vna.historico("NTN-C", vencimento="01-01-2031")
        >>> yd.vna.calcular_vna(referencias, "16-12-2025")
        Decimal('6449.641358')
    """
    modulo = _modulo_mensal(titulo)
    if vencimento is not None and titulo != "NTN-C":
        raise ValueError("Vencimento aplica-se apenas à NTN-C.")
    ano = None
    if vencimento is not None and not any_is_empty(vencimento):
        ano = converter_datas(vencimento).year
    df = modulo.vnas(atualizar=atualizar)
    if ano is not None:
        df = df.filter(pl.col("anos_vencimento").list.contains(ano))
    return df.sort("data", maintain_order=True)


def ultimo(
    titulo: TipoTitulo,
    vencimento: DateLike | None = None,
    *,
    atualizar: bool = False,
) -> pl.DataFrame:
    """Busca a última referência publicada de cada série de VNA.

    Args:
        titulo: NTN-B ou NTN-C. Última publicação de LFT não está disponível.
        vencimento: Filtro opcional da NTN-C. Sem filtro, retorna a última
            referência de cada série. Não se aplica à NTN-B.
        atualizar: Se True, baixa novamente o arquivo de VNA e renova seu
            cache de 60 segundos. Não atualiza o cache dos índices do IPCA.

    Returns:
        DataFrame Polars com data e valor da última publicação de cada série.
            Retorna vazio quando o histórico consultado estiver vazio.

    Output Columns:
        - data (Date): Data de referência da última publicação da série.
        - anos_vencimento (List[Int64]): Apenas NTN-C; anos da série.
        - vna (Decimal(18, 6)): VNA publicado nessa referência.

    Notes:
        Fonte: históricos do Tesouro Nacional obtidos por ``historico``.
        O resultado é publicado, sem projeção até a data atual. Preserva
        as colunas e os tipos do histórico.

    Examples:
        Extrai a data e o VNA da última referência da NTN-B. Os valores
        dependem da publicação mais recente do Tesouro Nacional:

        >>> referencia = yd.vna.ultimo("NTN-B")
        >>> data_publicada = referencia.item(0, "data")
        >>> vna_publicado = referencia.item(0, "vna")

        Para NTN-C, seleciona uma série pelo vencimento. Sem esse filtro,
        o resultado pode conter mais de uma linha:

        >>> referencia = yd.vna.ultimo("NTN-C", vencimento="01-01-2031")
    """
    df = historico(titulo, vencimento, atualizar=atualizar)
    if titulo == "NTN-C":
        return df.unique(subset="anos_vencimento", keep="last", maintain_order=True)
    return df.tail(1)


def vigencia(titulo: TipoTitulo, data: DateLike) -> tuple[dt.date, dt.date]:
    """Obtém o intervalo mensal de atualização do VNA, sem acesso à rede.

    Args:
        titulo: NTN-B ou NTN-C; LFT não possui essa vigência mensal.
        data: Data contida na vigência.

    Returns:
        tuple[date, date]: Início inclusivo e fim exclusivo, sem ajuste útil.
            NTN-B usa dia 15 a dia 15; NTN-C usa primeiro dia a primeiro dia.

    Notes:
        Segue os calendários de atualização do Tesouro Nacional implementados
        nas funções específicas de cada título.

    Raises:
        ValueError: Se o título não for NTN-B ou NTN-C ou a data for inválida.

    Examples:
        A NTN-B muda de vigência no dia 15, inclusive quando não é dia útil:

        >>> yd.vna.vigencia("NTN-B", "14-07-2026")
        (datetime.date(2026, 6, 15), datetime.date(2026, 7, 15))
        >>> yd.vna.vigencia("NTN-B", "15-07-2026")
        (datetime.date(2026, 7, 15), datetime.date(2026, 8, 15))

        A NTN-C muda no primeiro dia do mês:

        >>> yd.vna.vigencia("NTN-C", "15-07-2026")
        (datetime.date(2026, 7, 1), datetime.date(2026, 8, 1))
    """
    return _modulo_mensal(titulo).vigencia(data)


def projetado(
    titulo: TipoTitulo,
    data: DateLike,
    vna_base: float | Decimal,
    inflacao: float | Decimal | str | None = None,
    *,
    selic: float | Decimal | str | None = None,
) -> Decimal:
    """Projeta o VNA do título conforme sua metodologia.

    Args:
        titulo: LFT, NTN-B ou NTN-C.
        data: Data para a qual projetar o VNA.
        vna_base: VNA-base correspondente ao início da vigência ou ao último
            dia útil anterior à data, conforme o título.
        inflacao: Para NTN-B e NTN-C, variação mensal decimal; ``0.0045``
            representa 0,45%. Aceita também percentual explícito, como
            ``"0.45%"`` ou ``"0,45%"``. Para LFT, informe a taxa anual
            em ``selic``.
        selic: Obrigatória apenas para LFT. Aceita taxa decimal ou
            percentual explícito, como ``"13.65%"``. Para reproduzir o
            acruamento oficial da LFT, use a taxa Selic diária publicada pelo
            SGS 11. Como ela é publicada em percentual, ``0,050788%`` equivale
            à taxa decimal ``0,00050788`` e ao fator derivado ``1,00050788``.
            Converta essa taxa diária para uma taxa anual equivalente. A Selic
            Over do SGS 1178 é anualizada e serve apenas como aproximação. Para
            NTN-B e NTN-C, deve ser omitida.

    Returns:
        Decimal: VNA projetado com seis casas; NaN para entradas nulas.

    Notes:
        Entradas numéricas são convertidas para Decimal pela representação
        textual; percentuais explícitos são convertidos para a unidade decimal
        sem arredondamento antes da aplicação das regras do cálculo.
        NTN-B e NTN-C seguem a metodologia STN: base truncada em seis casas,
        inflação percentual arredondada em duas casas (quatro na unidade
        decimal) e expoente em dias corridos truncado em
        catorze. LFT usa dias úteis, base 252 e capitalização pela taxa anual
        informada. A taxa Selic diária publicada pelo SGS 11 é a referência
        exata para o acruamento; o fator usado no cálculo é ``1 + taxa`` em
        formato decimal. A função não busca a taxa automaticamente nem faz
        projeções externas.

    Raises:
        ValueError: Se o título for inválido, a taxa for informada para o
            título errado, ou houver violação dos limites da metodologia
            específica, ou percentual textual inválido.

    Examples:
        >>> import pyield as yd
        >>> yd.vna.projetado("NTN-B", "30-06-2026", 4731.856412, "0.45%")
        Decimal('4742.491138')
        >>> yd.vna.projetado(
        ...     "LFT",
        ...     "18-09-2026",
        ...     19905.773236,
        ...     selic="13.65%",
        ... )
        Decimal('19915.882987')

        Projeta a NTN-C a partir da base de 01/12/2025 da série de 2031,
        em um cenário de inflação mensal de 0,45%. A taxa é uma hipótese
        explícita, não a inflação realizada desse mês:

        >>> yd.vna.projetado("NTN-C", "16-12-2025", 6450.107485, "0,45%")
        Decimal('6464.135801')
    """
    if titulo not in {"LFT", "NTN-B", "NTN-C"}:
        raise ValueError("Título deve ser LFT, NTN-B ou NTN-C.")
    if titulo == "LFT":
        if inflacao is not None:
            raise ValueError("Use selic para a projeção da LFT.")
        if selic is None:
            return Decimal("NaN")
        if any_is_empty(selic):
            return Decimal("NaN")
        data_convertida = converter_datas(data)
        if du.eh_dia_util(data_convertida):
            data_base = du.deslocar(data_convertida, -1, ajuste="anterior")
        else:
            data_base = du.deslocar(data_convertida, 0, ajuste="anterior")
        return _lft.projetado_lft(data_base, data, vna_base, selic)
    if selic is not None:
        raise ValueError("selic aplica-se apenas à projeção da LFT.")
    return _modulo_mensal(titulo).vna_projetado(data, vna_base, inflacao)
