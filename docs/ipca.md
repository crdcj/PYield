# IPCA

O número-índice é retornado como `Decimal` em `yd.ipca.indice()` e como
`Decimal(18, 2)` na coluna `indice` de `yd.ipca.indice_serie()`. Os valores
preservam as duas casas publicadas pelo IBGE, sem conversão intermediária para
`float`. Quando indisponível, a consulta escalar retorna `Decimal("NaN")`;
use `.is_nan()` para verificar.

As taxas históricas são retornadas como `Decimal` em `yd.ipca.taxa()` e como
`Decimal(18, 4)` na coluna `taxa` de `yd.ipca.taxa_serie()`. A projeção da
ANBIMA, em `yd.ipca.taxa_projetada().valor_projetado`, também retorna `Decimal`
com quatro casas. A unidade é decimal: `Decimal("0.0045")` representa 0,45%.
A taxa histórica indisponível retorna `Decimal("NaN")` na consulta escalar.

## Calendário de divulgação

`yd.ipca.calendario(ano, atualizar=False)` consulta o calendário oficial do
IBGE e retorna as colunas `competencia` e `data_divulgacao`, ambas `Date`.
O ano seleciona a competência: dezembro pode ter divulgação no ano seguinte.
A data prevista não comprova publicação do índice e datas ausentes não são
estimadas. `atualizar=True` renova o cache de 60 segundos da consulta.

```python
calendario = yd.ipca.calendario(2026)
```

::: pyield.ipca
