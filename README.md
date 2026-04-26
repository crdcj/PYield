[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue?logo=readthedocs&logoColor=white)](https://crdcj.github.io/PYield/)

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
yd.du.contar("02-01-2025", "15-01-2025")  # -> 9
yd.du.deslocar("29-12-2023", 1)           # -> datetime.date(2024, 1, 2)

# Curva de DI Futuro
df = yd.futuro.historico("31-05-2024", "DI1")
# Colunas: data_referencia, codigo_negociacao, data_vencimento, dias_uteis, taxa_ajuste, ...

# Interpolação de taxas (flat forward, convenção 252 dias úteis/ano)
interp = yd.Interpolador(df["dias_uteis"], df["taxa_ajuste"], metodo="flat_forward")
interp(45)       # -> 0.04833...
interp([30, 60]) # -> Series do Polars com taxas interpoladas

# Precificação de títulos públicos
yd.ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651

# Indicadores do BCB
yd.selic_over("31-05-2024")  # -> 0.000414...
```

Um notebook no Colab com mais exemplos:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)

## Blocos Principais

### Dias Úteis (`du`)

O módulo `du` é a base do PYield. Todos os cálculos com datas (preço, duration, taxas a termo) dependem da contagem correta de dias úteis com feriados brasileiros.

```python
from pyield import du

# Conta dias úteis (início inclusivo, fim exclusivo)
du.contar("29-12-2023", "02-01-2024")  # -> 1

# Avança N dias úteis
du.deslocar("29-12-2023", 1)  # -> datetime.date(2024, 1, 2)

# Ajusta dia não útil para o próximo dia útil
du.deslocar("30-12-2023", 0)  # -> datetime.date(2024, 1, 2)

# Gera intervalo de dias úteis
du.gerar("22-12-2023", "02-01-2024")
# -> Series: [2023-12-22, 2023-12-26, 2023-12-27, 2023-12-28, 2023-12-29, 2024-01-02]

# Verifica se a data é dia útil
du.eh_dia_util("25-12-2023")  # -> False (Natal)
```

Todas as funções suportam operações vetorizadas com listas, Series ou arrays.

### Interpolação de Taxas (`Interpolador`)

A classe `Interpolador` interpola taxas usando a convenção de 252 dias úteis/ano, padrão no mercado brasileiro.

```python
from pyield import Interpolador

dias_uteis = [30, 60, 90]
taxas = [0.045, 0.05, 0.055]

# Interpolação flat forward (padrão de mercado)
interp = Interpolador(dias_uteis, taxas, metodo="flat_forward")
interp(45)  # -> 0.04833...

# Interpolação linear
linear = Interpolador(dias_uteis, taxas, metodo="linear")
linear(45)  # -> 0.0475

# Vetorizado
interp([15, 45, 75])  # -> pl.Series com 3 taxas

# Extrapolação (desabilitada por padrão, retorna NaN)
interp(100)  # -> nan
Interpolador(dias_uteis, taxas, metodo="flat_forward", extrapolar=True)(100)  # -> 0.055
```

### Taxas a Termo (`forward`, `forwards`)

Calcula taxas a termo a partir de curvas spot:

Convenção utilizada:

- `fwd_k = fwd_{j->k}` (forward do vértice `j` para `k`)
- `f_k = 1 + tx_k` (fator de capitalização no vértice `k`)
- `fwd_k = (f_k^au_k / f_j^au_j)^(1 / (au_k - au_j)) - 1`, com `au = du / 252`

```python
from pyield import forward, forwards

# Taxa a termo única entre dois pontos
forward(10, 20, 0.05, 0.06)  # -> 0.0700952...

# Curva a termo vetorizada a partir de taxas spot
dias_uteis = [10, 20, 30]
taxas = [0.05, 0.06, 0.07]
forwards(dias_uteis, taxas)  # -> Series: [0.05, 0.070095, 0.090284]
```

## Visão Geral dos Módulos

| Módulo | Finalidade |
|--------|---------|
| `du` | Calendário de dias úteis com feriados brasileiros |
| `futuro` | Dados de futuros (DI1, DDI, DAP, DOL, WDO, IND, WIN e outros) |
| `tpf` | Taxas, vencimentos, estoque, leilões, benchmarks, RMD e negociações de TPFs |
| `di1` | Curva DI1 interpolada e datas de negociação disponíveis |
| `Interpolador` | Interpolação de taxas (flat_forward, linear) |
| `forward` / `forwards` | Cálculo de taxas a termo |
| `ltn`, `ntnb`, `ntnf`, `lft`, `ntnc` | Precificação e análise dos títulos públicos principais |
| `ntnb1`, `ntnbprinc` | Títulos adicionais (NTN-B1, NTN-B Principal) |
| `copom` | Calendário de reuniões do COPOM e próxima reunião |
| `compromissadas` | Operações compromissadas do BCB |
| `ipca` | Dados de inflação (histórico e projeções) |
| `selic` | Opções digitais de COPOM e probabilidades implícitas |
| `hoje` / `agora` | Data/hora atual no Brasil (America/Sao_Paulo) |

## Títulos Públicos

```python
from pyield import ltn, ntnb, ntnf

# Busca taxas indicativas da ANBIMA
ltn.dados("23-08-2024")  # -> DataFrame com títulos LTN
ntnb.dados("23-08-2024")  # -> DataFrame com títulos NTN-B

# Calcula cotação do título (base 100)
ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651
ntnb.cotacao("31-05-2024", "15-08-2060", 0.061878)  # -> 99.5341

# Prêmio sobre o DI (pontos_base=True multiplica por 10.000)
ntnf.premio("30-05-2025", pontos_base=True)
# -> DataFrame: titulo, data_vencimento, premio
```

## Dados de Futuros

```python
import pyield as yd

# DI1 (Futuro de Depósito Interfinanceiro)
yd.futuro.historico("31-05-2024", "DI1")

# Outros contratos disponíveis no cache histórico:
# - Juros: DI1, DDI, FRC, FRO, DAP
# - Moedas: DOL, WDO
# - Índices: IND, WIN
yd.futuro.historico("31-05-2024", "DAP")

# Múltiplas datas de uma vez
yd.futuro.historico(["29-05-2024", "31-05-2024"], "DI1")

# Dados intradia (quando o mercado estiver aberto)
yd.futuro.intradia("DI1")  # Retorna dados ao vivo durante o pregão
```

## Tratamento de Datas

PYield aceita entradas de data flexíveis (`DateLike`):
- Strings: `"31-05-2024"`, `"31/05/2024"`, `"2024-05-31"`
- `datetime.date`, `datetime.datetime`

Funções escalares retornam `datetime.date`. Funções vetorizadas retornam `polars.Series`.

O parsing de strings é elemento a elemento entre os formatos aceitos. Strings
inválidas são convertidas para valores nulos (`None` em saídas escalares e `null`
em saídas vetorizadas).

Tratamento de nulos: funções escalares retornam `float('nan')` para entradas ausentes
(propaga nos cálculos). Funções vetorizadas propagam `null` elemento a elemento.

```python
from pyield import ntnb, du

ntnb.cotacao(None, "15-05-2035", 0.06149)  # -> nan
du.contar(["01-01-2024", None], "01-02-2024")  # -> Series: [22, null]
```

Consultas sem dados disponíveis (data futura, feriado, fim de semana ou
fonte indisponível) retornam DataFrame vazio ou `nan`, sem lançar exceção:

```python
import pyield as yd

yd.futuro.historico("01-01-2030", "DI1").is_empty()  # -> True
yd.tpf.secundario_mensal("01-01-2030").is_empty()    # -> True
yd.ptax("25-12-2025")                                # -> nan
```

## Migração da API por Objeto (v0.49.0)

A versão 0.49.0 reorganiza a API pública para privilegiar o objeto de análise,
não a fonte do dado. Mapa de migração:

| Antes | Depois |
|---|---|
| `yd.b3.futuro(data, contrato)` | `yd.futuro.historico(data, contrato)` |
| `yd.b3.futuro_intradia(contrato)` | `yd.futuro.intradia(contrato)` |
| `yd.b3.futuro_datas_disponiveis(contrato)` | `yd.futuro.datas_disponiveis(contrato)` |
| `yd.b3.futuro_enriquecer(df, contrato)` | `yd.futuro.enriquecer(df, contrato)` |
| `yd.b3.di_over(data)` | `yd.di_over(data)` |
| `yd.b3.di1.dados(data)` | `yd.di1.dados(data)` |
| `yd.b3.di1.interpolar_taxa(...)` | `yd.di1.interpolar_taxa(...)` |
| `yd.b3.di1.interpolar_taxas(...)` | `yd.di1.interpolar_taxas(...)` |
| `yd.b3.di1.datas_disponiveis()` | `yd.di1.datas_disponiveis()` |
| `yd.bc.ptax(data)` | `yd.ptax(data)` |
| `yd.bc.ptax_serie(inicio, fim)` | `yd.ptax_serie(inicio, fim)` |
| `yd.bc.selic_over(data)` | `yd.selic_over(data)` |
| `yd.bc.selic_over_serie(...)` | `yd.selic_over_serie(...)` |
| `yd.bc.selic_meta(data)` | `yd.selic_meta(data)` |
| `yd.bc.selic_meta_serie(...)` | `yd.selic_meta_serie(...)` |
| `yd.anbima.tpf(data, titulo)` | `yd.tpf.taxas(data, titulo)` |
| `yd.anbima.tpf_vencimentos(data, titulo)` | `yd.tpf.vencimentos(data, titulo)` |
| `yd.anbima.imaq(data)` | `yd.tpf.estoque(data)` |
| `yd.tn.leilao(data)` | `yd.tpf.leilao(data)` |
| `yd.bc.tpf_intradia()` | `yd.tpf.secundario_intradia()` |
| `yd.bc.tpf_mensal(data, extragrupo=...)` | `yd.tpf.secundario_mensal(data, extragrupo=...)` |
| `yd.bc.vna_lft(data)` | `yd.lft.vna(data)` |
| `yd.tn.benchmarks(...)` | `yd.tpf.benchmarks(...)` |
| `yd.pre.taxas_zero(data)` | `yd.tpf.curva_pre(data)` |

As funções antigas listadas acima foram removidas da API pública de alto nível.

## Documentação

Documentação completa: [crdcj.github.io/PYield](https://crdcj.github.io/PYield/)

## Testes

```sh
pytest
```
