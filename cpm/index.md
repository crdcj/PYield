# CPM

O CPM é o contrato de opção digital da B3 relacionado às decisões do COPOM. Ele é exposto como um produto independente na raiz da API:

```python
import pyield as yd

contratos = yd.cpm.contratos("29-01-2025")
```

O resultado contém os contratos negociados, a reunião correspondente, a data de expiração, a variação do strike em pontos-base e o preço de ajuste de referência da B3.

Busca os contratos CPM negociados em uma data.

Fonte: B3. A lista de contratos vem do boletim de preços (SPR) e o preço de ajuste vem do "Preço de Referência" do boletim de derivativos consolidados, o mesmo exibido no painel "Probabilidades da Taxa Selic Meta" da B3. As datas da reunião e de expiração vêm do calendário do COPOM.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de negociação. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars ordenado por `data_expiracao` e |
| `DataFrame` | `variacao_strike_bps`. Retorna DataFrame vazio, com o schema abaixo, |
| `DataFrame` | se não houver contratos CPM na data. |

**Output Columns**

- data\_referencia (Date): data de negociação.
- codigo\_negociacao (String): código do contrato (ex.: `CPMZ25C099500`).
- data\_fim\_reuniao (Date): último dia da reunião do COPOM.
- data\_expiracao (Date): primeiro dia útil após `data_fim_reuniao`, data de liquidação do contrato.
- tipo\_opcao (String): `"call"` ou `"put"`.
- variacao\_strike\_bps (Int32): variação da Selic Meta, em pontos-base, associada ao strike.
- preco\_ajuste (Float64): Preço de Referência da B3, em pontos (0–100).
- dias\_uteis (Int32): dias úteis de `data_referencia` até `data_expiracao`.

**Notes**

A B3 só publica o Preço de Referência de datas recentes (cerca de um mês). Para datas mais antigas, `preco_ajuste` é nulo.

Examples:

```pycon
>>> df = yd.cpm.contratos("29-01-2025")
```

## Probabilidades implícitas

As probabilidades derivadas dos preços dos contratos ficam em `yd.cpm.probabilidades`:

```python
import polars as pl

todas = yd.cpm.probabilidades("29-01-2025")
proxima = todas.filter(pl.col("ranking_reuniao") == 1)
```

Calcula as probabilidades implícitas das reuniões do COPOM.

Fonte: B3, a partir dos preços dos contratos CPM (`yd.cpm.contratos`), descontados pela curva de DI1. Inclui todas as reuniões com contratos negociados na data. Somente strikes com `preco_ajuste` não nulo são incluídos; reuniões em que todos os strikes têm preço nulo são excluídas.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de negociação. | *required* |
| `tipo_opcao` | `Literal['call', 'put']` | Tipo de opção, `"call"` ou `"put"`. O padrão é `"call"`, que na prática é o lado mais líquido. | `'call'` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars ordenado por `ranking_reuniao` e |
| `DataFrame` | `variacao_strike_bps`. Retorna DataFrame vazio, com o schema abaixo, |
| `DataFrame` | se não houver dados. |

**Output Columns**

- data\_referencia (Date): data de negociação.
- data\_fim\_reuniao (Date): último dia da reunião do COPOM.
- data\_expiracao (Date): data de expiração do contrato CPM.
- ranking\_reuniao (Int32): 1 para a reunião mais próxima com preços, 2 para a seguinte, e assim por diante.
- variacao\_strike\_bps (Int32): variação do strike em pontos-base, crescente dentro de cada reunião.
- dias\_uteis (Int32): dias úteis até a expiração.
- preco\_ajuste (Float64): Preço de Referência da B3 em pontos (0–100).
- taxa\_di1 (Float64): taxa DI1 interpolada até a expiração.
- fator\_desconto (Float64): fator aplicado na inversão da fórmula da B3, `exp(dias_uteis / 252 * ln(1 + taxa_di1))`.
- prob\_bruta (Float64): probabilidade antes da normalização, `preco_ajuste * fator_desconto / 100`.
- prob (Float64): probabilidade normalizada, `prob_bruta / soma(prob_bruta)` por `data_expiracao`, com soma 1,0 por reunião.
- prob\_acumulada (Float64): probabilidade acumulada em ordem crescente de strike.

**Notes**

Para uma única reunião, filtre o resultado, por exemplo `df.filter(pl.col("ranking_reuniao") == 1)` para a mais próxima.

Examples:

```pycon
>>> df = yd.cpm.probabilidades("29-01-2025")
>>> proxima = df.filter(pl.col("ranking_reuniao") == 1)
```

## Migração

Na versão atual, use:

```python
yd.cpm.contratos(data)
yd.cpm.probabilidades(data, tipo_opcao="call")
```

em vez de `yd.cpm.data`, `yd.cpm.probabilidades.all_meetings`, `yd.cpm.probabilidades.meeting` e dos caminhos antigos `yd.selic.cpm` e `yd.selic.probabilities`.
