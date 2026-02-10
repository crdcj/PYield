[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)

# PYield: Toolkit de Renda Fixa Brasileira

Português | [English](https://github.com/crdcj/PYield/blob/main/README.en.md)

PYield é uma biblioteca Python voltada para análise de títulos públicos brasileiros. Ela busca e processa dados da ANBIMA, BCB, IBGE, B3 e **Tesouro Nacional**, retornando DataFrames do Polars para pipelines rápidos e com tipagem consistente.

Embora inclua dados e ferramentas de outros mercados (como DI1, DAP e PTAX), esses recursos são auxiliares para o objetivo central: análise, precificação e acompanhamento de títulos públicos.

## Instalação

```sh
pip install pyield
```

## Início Rápido

```python
import pyield as yd

# Dias úteis (base de todos os cálculos)
yd.bday.count("02-01-2025", "15-01-2025")  # -> 9
yd.bday.offset("29-12-2023", 1)            # -> datetime.date(2024, 1, 2)

# Curva de DI Futuro
df = yd.futures("31-05-2024", "DI1")
# Columns: TradeDate, TickerSymbol, ExpirationDate, BDaysToExp, SettlementRate, ...

# Interpolação de taxas (flat forward, convenção 252 dias úteis/ano)
interp = yd.Interpolator("flat_forward", df["BDaysToExp"], df["SettlementRate"])
interp(45)       # -> 0.04833...
interp([30, 60]) # -> pl.Series with interpolated rates

# Precificação de títulos públicos
yd.ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651

# Indicadores do BCB
yd.bc.selic_over("31-05-2024")  # -> 0.000414...
```

Um notebook no Colab com mais exemplos:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)

## Blocos Principais

### Dias Úteis (`bday`)

O módulo `bday` é a base do PYield. Todos os cálculos com datas (preço, duration, taxas a termo) dependem da contagem correta de dias úteis com feriados brasileiros.

```python
from pyield import bday

# Conta dias úteis (início inclusivo, fim exclusivo)
bday.count("29-12-2023", "02-01-2024")  # -> 1

# Avança N dias úteis
bday.offset("29-12-2023", 1)  # -> datetime.date(2024, 1, 2)

# Ajusta dia não útil para o próximo dia útil
bday.offset("30-12-2023", 0)  # -> datetime.date(2024, 1, 2)

# Gera intervalo de dias úteis
bday.generate("22-12-2023", "02-01-2024")
# -> Series: [2023-12-22, 2023-12-26, 2023-12-27, 2023-12-28, 2023-12-29, 2024-01-02]

# Verifica se a data é dia útil
bday.is_business_day("25-12-2023")  # -> False (Christmas)
```

Todas as funções suportam operações vetorizadas com listas, Series ou arrays.

### Interpolação de Taxas (`Interpolator`)

A classe `Interpolator` interpola taxas usando a convenção de 252 dias úteis/ano, padrão no mercado brasileiro.

```python
from pyield import Interpolator

known_bdays = [30, 60, 90]
known_rates = [0.045, 0.05, 0.055]

# Interpolação flat forward (padrão de mercado)
interp = Interpolator("flat_forward", known_bdays, known_rates)
interp(45)  # -> 0.04833...

# Interpolação linear
linear = Interpolator("linear", known_bdays, known_rates)
linear(45)  # -> 0.0475

# Vetorizado
interp([15, 45, 75])  # -> pl.Series with 3 rates

# Extrapolação (desabilitada por padrão, retorna NaN)
interp(100)  # -> nan
Interpolator("flat_forward", known_bdays, known_rates, extrapolate=True)(100)  # -> 0.055
```

### Taxas a Termo (`forward`, `forwards`)

Calcula taxas a termo a partir de curvas spot:

```python
from pyield import forward, forwards

# Taxa a termo única entre dois pontos
forward(bday1=10, bday2=20, rate1=0.05, rate2=0.06)  # -> 0.0700952...

# Curva a termo vetorizada a partir de taxas spot
bdays = [10, 20, 30]
rates = [0.05, 0.06, 0.07]
forwards(bdays, rates)  # -> Series: [0.05, 0.070095, 0.090284]
```

## Visão Geral dos Módulos

| Módulo | Finalidade |
|--------|---------|
| `bday` | Calendário de dias úteis com feriados brasileiros |
| `futures` | Dados de futuros da B3 (DI1, DDI, FRC, DAP, DOL, WDO, IND, WIN) |
| `Interpolator` | Interpolação de taxas (flat_forward, linear) |
| `forward` / `forwards` | Cálculo de taxas a termo |
| `ltn`, `ntnb`, `ntnf`, `lft`, `ntnc` | Precificação e análise de títulos públicos |
| `anbima` | Dados da ANBIMA (preços de TPF, curvas de juros, índices IMA) |
| `bc` | Indicadores do BCB (SELIC, PTAX, repos, VNA) |
| `ipca` | Dados de inflação (histórico e projeções) |
| `today` / `now` | Data/hora atual no Brasil (America/Sao_Paulo) |

## Títulos Públicos

```python
from pyield import ltn, ntnb, ntnf

# Busca taxas indicativas da ANBIMA
ltn.data("23-08-2024")   # -> DataFrame with LTN bonds
ntnb.data("23-08-2024")  # -> DataFrame with NTN-B bonds

# Calcula cotação do título (base 100)
ntnb.quotation("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651
ntnb.quotation("31-05-2024", "15-08-2060", 0.061878)  # -> 99.5341

# Spreads de DI (bps=True multiplica por 10.000)
ntnf.di_spreads("30-05-2025", bps=True)
# -> DataFrame: BondType, MaturityDate, DISpread
```

## Dados de Futuros

```python
from pyield import futures

# DI1 (Futuro de Depósito Interfinanceiro)
futures("31-05-2024", "DI1")

# Outros contratos: DDI, FRC, DAP, DOL, WDO, IND, WIN
futures("31-05-2024", "DAP")

# Dados intradiários (quando o mercado estiver aberto)
futures("16-01-2025", "DI1")  # Retorna dados ao vivo durante o horário de negociação
```

## Tratamento de Datas

PYield aceita entradas de data flexíveis (`DateLike`):
- Strings: `"31-05-2024"`, `"31/05/2024"`, `"2024-05-31"`
- `datetime.date`, `datetime.datetime`
- `pandas.Timestamp`, `numpy.datetime64`

Funções escalares retornam `datetime.date`. Funções vetorizadas retornam `polars.Series`.

O parsing de strings é elemento a elemento entre os formatos aceitos. Strings
inválidas são convertidas para valores nulos (`None` em saídas escalares e `null`
em saídas vetorizadas).

Tratamento de nulos: funções escalares retornam `float('nan')` para entradas ausentes
(propaga nos cálculos). Funções vetorizadas propagam `null` elemento a elemento.

```python
from pyield import ntnb, bday

ntnb.quotation(None, "15-05-2035", 0.06149)  # -> nan
bday.count(["01-01-2024", None], "01-02-2024")  # -> Series: [22, null]
```

## Migração para Polars (v0.40.0+)

Todas as funções retornam **DataFrames/Series do Polars**. Para converter para Pandas:

```python
df_pandas = df.to_pandas(use_pyarrow_extension_array=True)
```

## Documentação

Documentação completa: [crdcj.github.io/PYield](https://crdcj.github.io/PYield/)

## Testes

```sh
pytest pyield --doctest-modules
```
