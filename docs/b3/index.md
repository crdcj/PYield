# B3 - Bolsa de Valores do Brasil

Este módulo contém funções para acessar dados de mercado da B3 (Brasil, Bolsa, Balcão).

Convenção de nomenclatura:
- Nas APIs enriquecidas da biblioteca, `contrato` representa o identificador-base
	do futuro (ex.: `DI1`, `DAP`, `WDO`) e `codigo_negociacao` representa o código
	completo negociado na B3 (ex.: `DI1F25`).
- No módulo `boletim`, que opera sobre o XML bruto da B3, os filtros usam
	`prefixo_ticker` e `comprimento_ticker` porque atuam diretamente sobre o
	campo original `TckrSymb`.

::: pyield.b3
