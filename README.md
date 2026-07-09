[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![Powered by Polars](https://img.shields.io/badge/Powered%20by-Polars-blue)](https://pola.rs/)
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue?logo=readthedocs&logoColor=white)](https://crdcj.github.io/PYield/)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)

# PYield: Toolkit de Renda Fixa Brasileira

Português | [English](https://github.com/crdcj/PYield/blob/main/README.en.md)

PYield é uma biblioteca Python voltada para análise de títulos públicos
brasileiros. Ela busca e processa dados da ANBIMA, BCB, IBGE, B3 e **Tesouro
Nacional**.

PYield usa Polars para processar dados em tabelas e coleções. Saídas escalares
retornam tipos nativos do Python, enquanto saídas não escalares retornam
`polars.Series` ou `polars.DataFrame`, conforme a função.

Embora inclua dados e ferramentas de outros mercados (como DI1, DAP e PTAX),
esses recursos são auxiliares para o objetivo central: análise, precificação e
acompanhamento de títulos públicos.

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
interp(45)  # -> 0.04833...

# Preçificar títulos públicos
yd.ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651

# Indicadores do BCB
yd.selic.over("31-05-2024")  # -> 0.000414...
```

Um notebook no Colab com mais exemplos:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)

## Mapa da API

Veja o [mapa completo da API](https://crdcj.github.io/PYield/api-map/) na
documentação.

| Componente | Tipo | Finalidade | Funções públicas |
|---|---|---|---|
| `yd.du` | módulo | Dias úteis e calendário brasileiro | `contar`, `deslocar`, `eh_dia_util`, `gerar`, `ultimo_dia_util`, `contar_expr`, `deslocar_expr`, `eh_dia_util_expr` |
| `yd.Interpolador` | classe | Interpolação escalar e em pipelines Polars | `interpolar`, `interpolar_expr`, `linear`, `flat_forward` |
| `yd.interpolar(...)` | função | Interpolação vetorizada flat-forward, curva única ou multi-curva |  |
| `yd.forward(...)` | função | Taxa a termo entre dois vértices |  |
| `yd.forwards(...)` | função | Curva de taxas a termo |  |
| `yd.futuro` | módulo | Contratos futuros da B3 | `di1`, `historico`, `intradia`, `datas_disponiveis`, `vencimento`, `enriquecer`, `vencimento_expr` |
| `yd.di1` | módulo | Curva DI1 e interpolação | `dados`, `interpolar_taxas`, `interpolar_taxa`, `datas_disponiveis` |
| `yd.tpf` | módulo | Títulos públicos federais | `taxas`, `vencimentos`, `estoque`, `leiloes`, `benchmarks`, `curva_pre`, `premio_pre`, `rmd`, `secundario` |
| `yd.lft` | módulo | LFT | `dados`, `vencimentos`, `cotacao`, `pu`, `taxa`, `vna`, `rentabilidade`, `rentabilidade_expr` |
| `yd.ltn` | módulo | LTN | `dados`, `vencimentos`, `pu`, `taxa`, `duration_expr`, `dv01`, `dv01_expr`, `rentabilidade`, `rentabilidade_expr`, `taxas_forward` |
| `yd.ntnb` | módulo | NTN-B | `dados`, `vencimentos`, `datas_pagamento`, `fluxos_caixa`, `cotacao`, `pu`, `taxa`, `taxas_zero`, `duration`, `duration_expr`, `dv01`, `dv01_expr`, `implicitas`, `curva` |
| `yd.ntnb1` | módulo | NTN-B1 | `datas_pagamento`, `fluxos_caixa`, `cotacao`, `pu`, `duration`, `dv01` |
| `yd.ntnbprinc` | módulo | NTN-B Principal | `pu`, `dv01` |
| `yd.ntnc` | módulo | NTN-C | `dados`, `datas_pagamento`, `fluxos_caixa`, `cotacao`, `pu`, `taxa`, `duration`, `duration_expr`, `dv01`, `dv01_expr` |
| `yd.ntnf` | módulo | NTN-F | `dados`, `vencimentos`, `datas_pagamento`, `fluxos_caixa`, `pu`, `taxa`, `taxas_zero`, `premio`, `premio_limpo`, `premio_limpo_expr`, `rentabilidade`, `rentabilidade_expr`, `duration`, `duration_expr`, `dv01`, `dv01_expr` |
| `yd.selic` | módulo | Selic, COPOM e política monetária | `over`, `over_serie`, `meta`, `meta_serie`, `compromissadas`, `copom`, `cpm`, `probabilities` |
| `yd.ipca` | módulo | IPCA histórico e projetado | `indice`, `indices`, `indices_ultimos`, `taxa`, `taxas`, `taxas_ultimas`, `taxa_projetada` |
| `yd.ptax(data)` | função | PTAX para uma data |  |
| `yd.ptax_serie(inicio, fim)` | função | Série histórica da PTAX |  |
| `yd.di_over(data)` | função | Taxa DI Over |  |
| `yd.hoje()` | função | Data atual no Brasil |  |
| `yd.agora()` | função | Data e hora atual no Brasil |  |

## Blocos Principais

### Dias Úteis (`du`)

O módulo `du` é a base do PYield. Todos os cálculos com datas (preço, duration,
taxas a termo) dependem da contagem correta de dias úteis com feriados
brasileiros.

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

As principais funções de cálculo (`contar`, `deslocar` e `eh_dia_util`)
suportam operações vetorizadas com listas, Series ou arrays.

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

# Extrapolação na ponta longa: desabilitada por padrão (NaN). A ponta
# curta sempre retorna a primeira taxa conhecida.
interp(100)  # -> nan
Interpolador(dias_uteis, taxas, metodo="flat_forward", extrapolar=True)(100)  # -> 0.055
```

Para interpolar uma coluna inteira dentro de um pipeline Polars, use
`interpolar_expr`:

```python
import polars as pl

df = pl.DataFrame({"du": [15, 45, 75]})
df.with_columns(taxa=interp.interpolar_expr("du"))
```

Quando os pontos alvo e a curva vêm de DataFrames diferentes (inclusive com
múltiplas datas de referência), use a função top-level `yd.interpolar`:

```python
import pyield as yd

taxas = yd.interpolar(
    dus_alvo=df_alvo["dias_uteis"],
    dus_curva=df_curva["dias_uteis"],
    taxas_curva=df_curva["taxa"],
    datas_alvo=df_alvo["data_referencia"],   # opcional (multi-curva)
    datas_curva=df_curva["data_referencia"], # opcional (multi-curva)
)
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

## Títulos Públicos

Os módulos `lft`, `ltn`, `ntnb`, `ntnf`, `ntnc`, `ntnb1` e `ntnbprinc`
fazem parte da família de Títulos Públicos Federais (`tpf`). Para uso direto
dos títulos, prefira os atalhos públicos na raiz:

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

Funções escalares de data retornam `datetime.date`. Operações vetorizadas de
data retornam `polars.Series`, enquanto consultas tabulares retornam
`polars.DataFrame`.

O parsing de strings usa expressões vetorizadas do Polars, com fallback entre os
formatos aceitos por linha. Strings inválidas são convertidas para valores nulos
(`None` em saídas escalares e `null` em saídas vetorizadas).

Valores nulos de data são preservados: entradas escalares ausentes retornam
`None`, e operações vetorizadas propagam `null` elemento a elemento.

```python
from pyield import du

du.deslocar(None, 1)  # -> None
du.contar(["01-01-2024", None], "01-02-2024")  # -> Series: [22, null]
```

Consultas sem dados disponíveis (data futura, feriado, fim de semana ou
fonte indisponível) retornam DataFrame vazio ou `nan`, sem lançar exceção:

```python
import pyield as yd

yd.futuro.historico("01-01-2030", "DI1").is_empty()  # -> True
yd.tpf.secundario.mensal("01-01-2030").is_empty()    # -> True
yd.ptax("25-12-2025")                                # -> nan
```

## Documentação

Documentação completa: [crdcj.github.io/PYield](https://crdcj.github.io/PYield/)

## Quebra de API (v0.52.0)

Resumo das quebras desta versão:

- `Interpolador(...)` e `Interpolador.interpolar(...)` agora aceitam **apenas
  inteiro escalar**. Chamadas com lista/`pl.Series` (`interp([30, 60])`),
  antes suportadas via sobrecarga vetorial, agora levantam `TypeError`.
  Substitua por `Interpolador.interpolar_expr` em pipelines Polars ou pela
  função top-level `yd.interpolar(...)` (curva única ou multi-curva).
- `yd.interpolar(...)` passou a usar `extrapolar=False` como padrão, alinhado
  com `Interpolador`. Antes era `True`. Chamadas que dependiam da
  extrapolação implícita na ponta longa precisam passar `extrapolar=True`
  explicitamente.
- As funções `dv01(...)` dos títulos públicos agora recebem `pu` como argumento
  explícito. Chamadas antigas que passavam apenas data, vencimento e taxa, ou
  que passavam VNA no caso da NTN-B, precisam ser atualizadas.
- `ntnf.taxas_zero(...)` mudou os nomes dos parâmetros de curva para o padrão
  `vencimentos_*` / `taxas_*`. Chamadas por posição continuam com a mesma
  ordem; chamadas por keyword precisam usar os novos nomes.

### `Interpolador(...)` não aceita mais lista/Series

O ``__call__`` e o método ``interpolar`` da classe agora são estritamente
escalares. O caminho vetorial foi dividido em duas APIs com semântica clara:

```python
# Antes (v0.51.x)
interp = yd.Interpolador(dus, taxas, metodo="flat_forward")
interp([15, 45, 75])  # pl.Series

# Agora, dentro de um pipeline Polars
df.with_columns(taxa=interp.interpolar_expr("du"))

# Agora, ad-hoc (curva única ou multi-curva)
yd.interpolar(
    dus_alvo=pl.Series([15, 45, 75]),
    dus_curva=pl.Series(dus),
    taxas_curva=pl.Series(taxas),
)
```

### `dv01(...)` agora recebe PU

As funções `dv01(...)` dos títulos públicos recebem o PU usado como base para o
cálculo, não mais o VNA ou apenas a taxa. Se necessário, calcule o PU antes:

```python
pu = yd.ntnb.pu(vna, yd.ntnb.cotacao(data, vencimento, taxa))
dv01 = yd.ntnb.dv01(data, vencimento, taxa, pu)
```

### `ntnf.taxas_zero(...)` usa nomes por conceito

Os parâmetros de curva da NTN-F foram renomeados para manter o padrão
`vencimentos_*` / `taxas_*`:

- `ltn_vencimentos` → `vencimentos_ltn`
- `ltn_taxas` → `taxas_ltn`
- `ntnf_vencimentos` → `vencimentos_ntnf`
- `ntnf_taxas` → `taxas_ntnf`

## Testes

```sh
uv run pytest
```
