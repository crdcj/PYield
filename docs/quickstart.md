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

As consultas de mercado precisam de conexão com a internet e buscam novamente
os dados nas fontes a cada chamada. A exceção são os históricos em Parquet de
TPF e futuros, que mantêm cache em memória por processo, renovado diariamente.

Falhas transitórias de comunicação permitem uma única retentativa por download,
com espera entre 0,5 e 1 segundo. Se a falha persistir, a exceção é propagada.
As consultas HTTP usam 5 segundos para conexão e 10, 30 ou 60 segundos para
leitura, conforme o endpoint. Esses limites valem por tentativa; não há garantia
de um prazo total rígido. Consulte a [política de retentativas](release-0.62.0.md#falhas-de-rede-e-retentativas)
para os erros elegíveis e o tratamento de `Retry-After`.

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
# 99.3651
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
yd.ipca.taxa_serie("01-01-2024", "01-03-2024")
```

Os indicadores de inflação, Selic, DI Over e PTAX retornam `Decimal` nas
consultas escalares e colunas Polars `Decimal` nas séries. Para cálculos exatos,
use constantes criadas a partir de texto:

```python
from decimal import Decimal

taxa = yd.ipca.taxa("01-01-2024")
if not taxa.is_nan():
    fator = Decimal("1") + taxa
```

Os [VNAs](vna.md) também usam `Decimal`. As taxas históricas e projetadas do
IPCA e IGP-M são expressas em unidade decimal. As entradas de inflação em
`vna.contexto` e `vna.projetado` aceitam números decimais (`0.0045`) ou
percentuais explícitos (`"0.45%"` ou `"0,45%"`), convertidos para `Decimal`. Consulte as
[notas da 0.60.0](release-0.60.0.md) para os tipos e a migração da API.

Para análises que precisam de ponto flutuante, converta explicitamente:

```python
taxa_float = float(yd.selic.over("31-05-2024"))
serie_float = yd.ptax_serie(ultimos=5).with_columns(
    cotacao=pl.col("cotacao").cast(pl.Float64)
)
```

A conversão pode introduzir aproximação binária. Veja as
[notas da 0.62.0](release-0.62.0.md) para a migração dos indicadores SGS e as
[notas da 0.63.0](release-0.63.0.md) para as escalas decimais atuais.

## Datas e ausência de dados

Datas escalares aceitam `DD-MM-YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD`,
`datetime.date` e `datetime.datetime`. Datas escalares malformadas levantam
`ValueError`; em operações vetorizadas, elementos ausentes ou malformados
tornam-se `null` para preservar o pipeline Polars.

Consultas válidas sem dados — por exemplo, para uma data futura, feriado ou fim
de semana — retornam um DataFrame vazio, `nan` ou `Decimal("NaN")`, conforme
o contrato da função:

```python
yd.futuro.historico("01-01-2030", "DI1").is_empty()
# True

yd.ptax("25-12-2025")
# Decimal('NaN')
```

Para detalhes de parâmetros, colunas e convenções financeiras, continue pelo
[mapa da API](api-map.md) ou pela página do módulo desejado.
