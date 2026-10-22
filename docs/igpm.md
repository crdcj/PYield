# IGP-M

As taxas mensais são retornadas como `Decimal` em `yd.igpm.taxa()` e como
`Decimal(18, 4)` na coluna `taxa` de `yd.igpm.taxa_serie()`. A unidade é decimal:
`Decimal("0.0027")` representa 0,27%. Os valores são convertidos diretamente
do texto do SGS/BCB, sem passagem por `float`.

Quando indisponível, a consulta escalar retorna `Decimal("NaN")`; use
`.is_nan()` para verificar. A consulta de série retorna um DataFrame vazio
com o mesmo esquema. A série de taxas representa a variação mensal.

Os números-índice são obtidos com `yd.igpm.indice(data)` e
`yd.igpm.indice_serie(inicio, fim)` ou `yd.igpm.indice_serie(ultimos=n)`.
A fonte é a FGV/Conjuntura Econômica, disponibilizada pelo
[Ipeadata](https://www.ipeadata.gov.br/api/) na série `IGP12_IGPM12`.
O retorno escalar usa `Decimal` e a coluna `indice` usa `Decimal(18, 10)`:
as dez casas preservam os valores antigos, anteriores a 1994. A coluna
`periodo` identifica a competência no formato `YYYYMM`. O índice indisponível
retorna `Decimal("NaN")` na consulta escalar.

Nas séries de índices, qualquer dia identifica o mês inteiro; os dois meses
extremos são incluídos. `fim=None` usa a data atual no Brasil. Informe `inicio`
ou `ultimos`; quando informado, `ultimos` tem prioridade sobre as datas.

## Calendário de divulgação

`yd.igpm.calendario(ano, atualizar=False)` consulta as divulgações mensais na
FGV IBRE, excluindo as prévias decendiais e os demais índices. Retorna
`competencia` e `data_divulgacao`, ambas `Date`. A competência corresponde ao
mês da divulgação mensal. Datas ausentes não são estimadas, e o calendário
não comprova publicação. `atualizar=True` renova o cache de 60 segundos do ano.

```python
calendario = yd.igpm.calendario(2026)
```

::: pyield.igpm
