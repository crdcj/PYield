"""Relatório Mensal da Dívida (RMD) do Tesouro Nacional."""

import logging

import polars as pl

from . import _aba_1_3, _aba_2_1
from ._download import baixar_planilha_rmd as _carregar_planilha_rmd

logger = logging.getLogger(__name__)

_IMPLEMENTACOES = {
    "1.3": _aba_1_3.estruturar_dados,
    "2.1": _aba_2_1.estruturar_dados,
}


def rmd(aba: str) -> pl.DataFrame:
    """Retorna dados do Relatório Mensal da Dívida (RMD) do Tesouro Nacional.

    Baixa e processa a planilha do RMD, extraindo dados da aba solicitada. A
    publicação mais recente é descoberta automaticamente via parse HTML da página
    oficial do Tesouro Transparente.

    Fonte: Tesouro Nacional (Tesouro Transparente).

    Args:
        aba: Número da aba a processar. Abas implementadas: ``"1.3"`` e ``"2.1"``.

    Returns:
        DataFrame Polars no schema específico da aba solicitada.

    Output Columns:
        Aba ``"1.3"``:
            * periodo (Date): primeiro dia do mês de referência.
            * grupo (String): seção principal — ``"Emissões"`` ou ``"Resgates"``.
            * subgrupo (String): categoria dentro do grupo.
            * titulo (String): tipo de título ou ``null`` para subgrupos sem
                detalhamento por título.
            * valor (Float64): valor em R$.
        Aba ``"2.1"``:
            * periodo (Date): primeiro dia do mês de referência.
            * detentor (String): quem detém o estoque — ``"Público"`` ou
                ``"Banco Central"``.
            * tipo (String): classificação da dívida — ``"DPMFi"`` (interna) ou
                ``"DPFe"`` (externa).
            * categoria (String): subdivisão dentro do tipo, quando houver —
                ``"Tesouro Nacional"``, ``"Banco Central"`` (emitente dentro da
                DPMFi pública), ``"Mobiliária"``, ``"Contratual"``; ``null``
                quando não há subdivisão (ex.: DPMFi em poder do Banco Central).
            * titulo (String): título ou instrumento de dívida.
            * valor (Float64): valor em R$. Somente registros folha; subtotais
                devem ser calculados pelo usuário via agregação.

    Raises:
        ValueError: Se ``aba`` não estiver entre as abas implementadas.

    Notes:
        - A aba ``"1.3"`` traz emissões e resgates da DPMFi; valores iguais a
          zero são descartados.
        - A aba ``"2.1"`` traz a série histórica de estoque da DPF; valores
          iguais a zero são mantidos.

        Erros de download e processamento são propagados ao chamador; não
        são convertidos em DataFrame vazio.

    Examples:
        >>> df = yd.rmd(aba="1.3")  # doctest: +SKIP
        >>> df = yd.rmd(aba="2.1")  # doctest: +SKIP
    """
    if aba not in _IMPLEMENTACOES:
        disponiveis = ", ".join(f'"{t}"' for t in sorted(_IMPLEMENTACOES))
        raise ValueError(
            f"Aba '{aba}' não disponível. Abas implementadas: {disponiveis}."
        )

    conteudo_excel = _carregar_planilha_rmd()
    df = _IMPLEMENTACOES[aba](conteudo_excel)

    logger.info("Dados do RMD (aba %r) processados. Shape: %s.", aba, df.shape)
    return df
