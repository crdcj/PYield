"""VNA (Valor Nominal Atualizado) da LFT no site do BCB.

Exemplo de chamada à API:
    https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/20240531APC238

Trecho relevante da resposta (tabela VNA):
    EMISSAO     VENCIMENTO   DATA BASE    TITULO        INDICE
    03/02/2021  01/09/2024   01/07/2000   210100       14903,011480
    30/03/2022  01/03/2025   01/07/2000   210100       14903,011480
    23/08/2019  01/09/2025   01/07/2000   210100       14903,011480
    28/06/2023  01/03/2026   01/07/2000   210100       14903,011480

A resposta bruta do BCB é uma tabela diária com títulos remunerados pela Selic.
A função pública filtra as LFTs (título 210100), valida a consistência dos
valores e retorna o VNA único.
"""

import datetime as dt
import logging
from decimal import Decimal, localcontext

import requests

from pyield import du
from pyield._internal.cache import ttl_cache
from pyield._internal.converters import converter_datas, data_referencia_valida
from pyield._internal.numbers import truncar_decimal
from pyield._internal.retry import retry_padrao
from pyield._internal.types import DateLike, any_is_empty
from pyield.tpf.titulos._utils import converter_taxa

CODIGO_LFT = "210100"
_LOGGER = logging.getLogger(__name__)


def projetado_lft(
    data_base: DateLike,
    data: DateLike,
    vna_base: float | Decimal,
    taxa_anual: float | Decimal | str,
) -> Decimal:
    """Projeta o VNA da LFT com uma taxa anual constante fornecida pelo consumidor.

    Args:
        data_base: Data à qual corresponde o VNA-base.
        data: Data do próximo dia útil de acruamento após a data-base.
        vna_base: VNA-base em reais, positivo e finito.
        taxa_anual: Taxa anual decimal, finita e maior que -1. Aceita também
            percentual explícito, como ``"13.65%"`` ou ``"13,65%"``.

    Returns:
        Decimal: VNA projetado, truncado em seis casas decimais. Retorna
            ``Decimal('NaN')`` para entradas ausentes ou NaN.

    Notes:
        Calcula ``vna_base * (1 + taxa_anual) ** (dias_uteis / 252)``.
        Usa ``yd.du.contar`` com calendário automático de feriados brasileiros:
        data-base inclusiva e data final exclusiva, sem deslocar as datas.
        Com zero dias úteis, retorna o VNA-base truncado em seis casas.

        O cálculo usa Decimal com 28 dígitos de precisão. Entradas float são
        convertidas pela representação decimal textual. Percentuais explícitos
        são convertidos para decimal sem truncamento. Não há truncamentos
        intermediários nem arredondamento comercial da taxa.

        Não consulta fontes externas nem escolhe automaticamente uma taxa. Para
        reproduzir o acruamento oficial da LFT, use a taxa Selic diária
        publicada pela série SGS 11. Como ela é publicada em percentual,
        ``0,050788%`` corresponde à taxa decimal ``0,00050788`` e ao fator
        derivado ``1,00050788``. Converta essa taxa diária para uma taxa anual
        equivalente antes de informar ``taxa_anual``. A Selic Over da série
        SGS 1178 é anualizada e serve apenas como aproximação. O resultado é
        uma projeção sob a taxa informada, não um VNA oficial realizado. A
        taxa pode ser negativa, desde que maior que -100%.

    Raises:
        ValueError: Data final anterior à base, VNA não positivo ou não finito,
            taxa não finita ou menor ou igual a -100%, ou data malformada.

    Examples:
        >>> from decimal import Decimal
        >>> projetado_lft(
        ...     "17-09-2026",
        ...     "18-09-2026",
        ...     Decimal("19905.773236"),
        ...     "13.65%",
        ... )
        Decimal('19915.882987')
    """
    if any_is_empty(data_base, data, vna_base, taxa_anual):
        return Decimal("NaN")
    inicio = converter_datas(data_base)
    fim = converter_datas(data)
    if fim < inicio:
        raise ValueError("A data de projeção não pode ser anterior à data-base.")
    dias_uteis = du.contar(inicio, fim)
    if dias_uteis != 1:
        _LOGGER.warning(
            "A projeção da LFT avançou uma quantidade diferente de um dia útil.",
        )
    base = vna_base if isinstance(vna_base, Decimal) else Decimal(str(vna_base))
    taxa_anual = converter_taxa(taxa_anual)
    taxa = taxa_anual if isinstance(taxa_anual, Decimal) else Decimal(str(taxa_anual))
    if not base.is_finite() or base <= 0:
        raise ValueError("O VNA-base deve ser positivo e finito.")
    if not taxa.is_finite() or taxa <= -1:
        raise ValueError("A taxa anual deve ser finita e maior que -100%.")
    with localcontext() as contexto:
        contexto.prec = 28
        expoente = Decimal(dias_uteis) / 252
        return truncar_decimal(base * (1 + taxa) ** expoente, 6)


@ttl_cache()
@retry_padrao
def _baixar_texto(data_referencia: dt.date) -> str:
    """Baixa o arquivo diário do SELIC no site do BCB."""
    # Exemplo: https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/20240418APC238
    url_base = "https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/3/"
    url_file = f"{data_referencia.strftime('%Y%m%d')}APC238"
    url = url_base + url_file

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text


def _recortar_tabela(texto: str) -> str:
    """Recorta o trecho da tabela VNA do texto bruto."""
    inicio = texto.find("EMISSAO")
    fim = texto.find("99999999*")
    return texto[inicio:fim].strip()


def _obter_linhas(texto_tabela: str) -> list[str]:
    """Retorna as linhas de dados (sem cabeçalho) da tabela VNA."""
    todas = texto_tabela.splitlines()
    linhas = [linha.strip() for linha in todas if linha.strip()]
    return linhas[1:]


def _filtrar_linhas_lft(linhas: list[str]) -> list[str]:
    """Filtra apenas as linhas de LFT."""
    return [linha for linha in linhas if linha.split()[3] == CODIGO_LFT]


def _extrair_valores_lft(linhas: list[str]) -> list[Decimal]:
    """Extrai os valores VNA numéricos das linhas de LFT."""
    valores = []
    for linha in _filtrar_linhas_lft(linhas):
        vna_str = linha.split()[-1].replace(",", ".")
        valores.append(Decimal(vna_str))
    return valores


def _validar_valores(valores: list[Decimal]) -> Decimal:
    """Valida se todos os valores VNA são iguais e retorna o valor único."""
    valor = valores[0]
    if any(valor != v for v in valores):
        bcb_url = "https://www.bcb.gov.br/estabilidadefinanceira/selicbaixar"
        msg = f"Valores VNA divergentes. Verifique os dados em {bcb_url}"
        raise ValueError(msg)
    return valor


def vna(data: DateLike | None = None, *, atualizar: bool = False) -> Decimal:
    """Busca o Valor Nominal Atualizado (VNA) da LFT.

    Fonte: Banco Central do Brasil, arquivo diário do SELIC. A resposta bruta
    contém uma tabela de títulos remunerados pela Selic; esta função filtra as
    LFTs (título 210100), valida que todas as linhas têm o mesmo VNA e retorna
    esse valor único.

    Args:
        data: Data de referência. Se omitida ou nula, retorna
            ``Decimal('NaN')``.

    Returns:
        Decimal: VNA da LFT com seis casas decimais. Retorna
            ``Decimal('NaN')`` se a entrada for nula, vazia ou uma data de
            referência inválida.

    Raises:
        ValueError: Se os valores VNA extraídos da fonte forem divergentes.
        requests.exceptions.HTTPError: Se a requisição ao BCB falhar.

    Examples:
        >>> import pyield as yd
        >>> yd.vna.valor("LFT", "31-05-2024")
        Decimal('14903.011480')
    """
    if any_is_empty(data):
        return Decimal("NaN")
    data = converter_datas(data)
    if data is None or not data_referencia_valida(data):
        return Decimal("NaN")

    texto = _baixar_texto(data, _atualizar=atualizar)
    tabela = _recortar_tabela(texto)
    linhas = _obter_linhas(tabela)
    valores = _extrair_valores_lft(linhas)
    return _validar_valores(valores)
