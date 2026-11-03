# IGP-M

As escalas e a cobertura dos números-índice foram ajustadas na
[0.63.0](release-0.63.0.md#cobertura-e-organizacao-do-igp-m).

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
O retorno escalar usa `Decimal` com três casas e a coluna `indice` usa
`Decimal(18, 3)`. A cobertura começa em **agosto de 1994**, base 100 da série.
A coluna `periodo` identifica a competência no formato `YYYYMM`. O índice indisponível
retorna `Decimal("NaN")` na consulta escalar.

O corte acompanha a base do índice: julho de 1994 ainda tem quatro casas
decimais na fonte (`92.9684`); de agosto de 1994 em diante, três casas preservam
os valores. A biblioteca cobre esse período para manter um esquema simples e
coerente com seu uso em cálculos financeiros em reais. Competências anteriores
ao corte são omitidas nas séries e retornam `Decimal("NaN")` na consulta
escalar. Um intervalo que atravesse o corte retorna somente agosto de 1994 em
diante; um intervalo inteiramente anterior retorna DataFrame vazio, com o mesmo
esquema. A seleção por `ultimos` também respeita esse limite.

Para consultar os valores anteriores com sua precisão original, acesse a
[série completa na API do Ipeadata](https://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='IGP12_IGPM12')).
Essa consulta é feita diretamente na fonte, fora da API de índices do PYield.
A biblioteca omite esses índices, em vez de retorná-los com precisão reduzida.
A cobertura das taxas mensais segue a série SGS 189.

Nas séries de índices, qualquer dia identifica o mês inteiro; os dois meses
extremos são incluídos. `fim=None` usa a data atual no Brasil. Informe `inicio`
ou `ultimos`; quando informado, `ultimos` tem prioridade sobre as datas.

## Calendário de divulgação

`yd.igpm.calendario(ano)` consulta as divulgações mensais na
FGV IBRE, excluindo as prévias decendiais e os demais índices. Retorna
`competencia` e `data_divulgacao`, ambas `Date`. A competência corresponde ao
mês da divulgação mensal. Datas ausentes não são estimadas, e o calendário
não comprova publicação.

```python
calendario = yd.igpm.calendario(2026)
```

::: pyield.igpm
