# CPM

O CPM é o contrato de opção digital da B3 relacionado às decisões do COPOM.
Ele é exposto como um produto independente na raiz da API:

```python
import pyield as yd

contratos = yd.cpm.data("29-01-2025")
```

O resultado contém os contratos negociados, a reunião correspondente, a data
de expiração, a variação do strike em pontos-base e o preço de ajuste de
referência da B3.

::: pyield.cpm

## Probabilidades implícitas

As probabilidades derivadas dos preços dos contratos ficam no namespace
`yd.cpm.probabilidades`:

```python
probabilidades = yd.cpm.probabilidades.meeting("29-01-2025")
todas = yd.cpm.probabilidades.all_meetings("29-01-2025")
```

::: pyield.cpm.probabilidades

## Migração

Na versão atual, use:

```python
yd.cpm.data(data)
yd.cpm.probabilidades.meeting(data)
```

em vez dos caminhos antigos `yd.selic.cpm` e
`yd.selic.probabilities`.
