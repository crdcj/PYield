---
title: "Quickstart"
description: "Primeiros passos com a biblioteca PYield"
---

# Quickstart

Este guia apresenta o fluxo básico para consultar dados, trabalhar com
`polars.DataFrame` e calcular indicadores de renda fixa com o PYield.

## Instalação

O PYield requer Python 3.12 ou superior:

```sh
pip install pyield
```

Se o projeto já for gerenciado pelo `uv`, adicione o PYield como dependência
do projeto:

```sh
uv add pyield
```

Depois, importe a biblioteca com o alias recomendado:

```python
import pyield as yd
```

As consultas de mercado buscam dados nas fontes correspondentes. Portanto, uma
conexão com a internet pode ser necessária na primeira consulta ou quando os
dados não estiverem disponíveis no cache local.

## Dias úteis

O módulo `du` é a base dos cálculos de datas. Ele considera o calendário de
feriados brasileiros e também aceita operações vetorizadas:

```python
yd.du.contar("29-12-2023", "02-01-2024")
# 1

yd.du.deslocar("29-12-2023", 1)
# datetime.date(2024, 1, 2)

yd.du.gerar("22-12-2023", "02-01-2024")
# Series com os dias úteis do intervalo
```

Por padrão, `calendario="auto"` seleciona a lista de feriados com base na data
de referência. Use `calendario="anterior"` ou `calendario="atual"` para
selecionar explicitamente um regime.

```python
yd.du.contar(
    "20-11-2024",
    "21-11-2024",
    calendario="atual",
)
```

## Consultar dados de mercado

As funções de consulta retornam `polars.DataFrame`. Por exemplo, os contratos
futuros da B3 podem ser consultados por data e código de negociação:

```python
df = yd.futuro.historico("31-05-2024", "DI1")

df.columns
# ['data_referencia', 'codigo_negociacao', 'data_vencimento',
#  'dias_uteis', 'taxa_ajuste', ...]
```

O mesmo módulo atende outros contratos, como `DDI`, `FRC`, `FRO`, `DAP`, `DOL`,
`WDO`, `IND` e `WIN`:

```python
df_dap = yd.futuro.historico("31-05-2024", "DAP")
```

Também é possível consultar várias datas de uma vez:

```python
df = yd.futuro.historico(
    ["29-05-2024", "31-05-2024"],
    "DI1",
)
```

Para dados durante o pregão, use a consulta intradia:

```python
df = yd.futuro.intradia("DI1")
```

## Títulos públicos

Os módulos de títulos públicos acessam dados indicativos da ANBIMA e expõem
funções de precificação e análise. Acesse os módulos pela raiz do pacote:

```python
from pyield import ltn, ntnb

df_ltn = ltn.dados("23-08-2024")
df_ntnb = ntnb.dados("23-08-2024")
```

Para consultar taxas indicativas de forma agregada, use `tpf`:

```python
df = yd.tpf.taxas("23-08-2024", titulo="PRE")
df_historico = yd.tpf.taxas_historicas(
    inicio="01-08-2024",
    fim="31-08-2024",
    titulo="PRE",
)
```

Uma cotação de NTN-B usa data de referência, vencimento e taxa em formato
decimal:

```python
cotacao = ntnb.cotacao(
    "31-05-2024",
    "15-05-2035",
    0.061490,
)
# 0.993651
```

Consulte as páginas de [LFT](lft.md), [LTN](ltn.md), [NTN-B](ntnb.md) e dos
demais títulos para fórmulas, convenções e colunas retornadas.

## Interpolação de taxas

`Interpolador` usa a convenção de 252 dias úteis por ano e oferece os métodos
`flat_forward` e `linear`:

```python
interp = yd.Interpolador(
    [30, 60, 90],
    [0.045, 0.050, 0.055],
    metodo="flat_forward",
)

interp(45)
# 0.04833...
```

Para usar o interpolador em um pipeline Polars, aplique
`interpolar_expr`:

```python
import polars as pl

df = pl.DataFrame({"dias_uteis": [15, 45, 75]})
df.with_columns(
    taxa=interp.interpolar_expr("dias_uteis"),
)
```

Quando os pontos-alvo e a curva estão em estruturas diferentes, use
`yd.interpolar`. Para calcular uma taxa a termo, use `yd.forward` ou
`yd.forwards`.

## Indicadores auxiliares

Indicadores do BCB, da PTAX e do IPCA ficam disponíveis em namespaces próprios:

```python
yd.selic.over("31-05-2024")
yd.selic.meta("31-05-2024")
yd.ptax("31-05-2024")
yd.ipca.taxas("01-01-2024", "01-03-2024")
```

## Datas e ausência de dados

Datas escalares aceitam `DD-MM-YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD`,
`datetime.date` e `datetime.datetime`. Datas escalares malformadas levantam
`ValueError`; em operações vetorizadas, elementos ausentes ou malformados
tornam-se `null` para preservar o pipeline Polars.

Consultas válidas sem dados — por exemplo, para uma data futura, feriado ou fim
de semana — retornam um DataFrame vazio ou `nan`, conforme o contrato da função:

```python
yd.futuro.historico("01-01-2030", "DI1").is_empty()
# True

yd.ptax("25-12-2025")
# nan
```

Para detalhes de parâmetros, colunas e convenções financeiras, continue pelo
[mapa da API](api-map.md) ou pela página do módulo desejado.
