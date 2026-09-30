# CPM

O CPM é o contrato de opção digital da B3 relacionado às decisões do COPOM.
Ele é exposto como um produto independente na raiz da API:

```python
import pyield as yd

contratos = yd.cpm.contratos("29-01-2025")
```

O resultado contém os contratos negociados, a reunião correspondente, a data
de expiração, a variação do strike em pontos-base e o preço de ajuste de
referência da B3.

::: pyield.cpm.contratos

## Probabilidades implícitas

As probabilidades derivadas dos preços dos contratos ficam em
`yd.cpm.probabilidades`:

```python
import polars as pl

todas = yd.cpm.probabilidades("29-01-2025")
proxima = todas.filter(pl.col("ranking_reuniao") == 1)
```

::: pyield.cpm.probabilidades

## Migração

Na versão atual, use:

```python
yd.cpm.contratos(data)
yd.cpm.probabilidades(data, tipo_opcao="call")
```

em vez de `yd.cpm.data`, `yd.cpm.probabilidades.all_meetings`,
`yd.cpm.probabilidades.meeting` e dos caminhos antigos `yd.selic.cpm` e
`yd.selic.probabilities`.
