# Novidades na versão 0.66.0

Revisão do interpolador. `yd.Interpolador` passa a ser o caminho para uma
única curva, inclusive com uma sequência de prazos, e `yd.interpolar` fica
dedicada ao cálculo em lote, com uma curva por data de referência. Prazos
negativos ficam sem taxa em todos os caminhos e prazos de tipo não inteiro
geram erro.

Há duas mudanças incompatíveis, ambas com erro imediato na chamada:
`yd.interpolar` exige as datas, o método e parâmetros nomeados, e os prazos
devem ter tipo inteiro. As seções abaixo mostram a migração.

## `yd.interpolar` só para lote, com parâmetros nomeados

`yd.interpolar` aceitava dois usos: uma única curva, sem datas, e várias
curvas, com `datas_alvo` e `datas_curva`. O primeiro repetia o que
`yd.Interpolador` já faz. Agora a função atende apenas ao segundo, que é o seu
propósito: interpolar de uma vez os pontos de muitas datas de referência, como
no reprocessamento de uma base histórica.

As datas passam a ser obrigatórias e todos os parâmetros são nomeados. São
cinco séries, duas de datas e duas de prazos com o mesmo tipo; passadas por
posição, uma troca entre alvo e curva não gerava erro, apenas resultado errado.

O método também passa a ser informado em `metodo`, como em `yd.Interpolador`.
A função só calculava flat-forward, sem que a chamada dissesse isso; agora o
método aparece na chamada, e `"linear"` fica disponível também em lote.

```python
yd.interpolar(
    datas_alvo=df["data_referencia"],
    dus_alvo=df["dias_uteis"],
    datas_curva=df_curva["data_referencia"],
    dus_curva=df_curva["dias_uteis"],
    taxas_curva=df_curva["taxa"],
    metodo="flat_forward",
)
```

Para uma única curva, use `yd.Interpolador`:

```python
# antes
yd.interpolar(dus_alvo, dus_curva, taxas_curva, extrapolar=True)

# agora
flat_forward = yd.Interpolador(dus_curva, taxas_curva, "flat_forward", extrapolar=True)
flat_forward(dus_alvo)
```

## Prazo negativo fica sem taxa

`yd.interpolar` devolvia a primeira taxa da curva quando o prazo era negativo,
enquanto `yd.Interpolador` já devolvia nulo. Agora os dois devolvem nulo: um
prazo negativo não tem taxa.

A mudança alcança `yd.di1.interpolar_taxas` e `yd.di1.interpolar_taxa`, que
usam `yd.interpolar`. Um vencimento anterior à data de negociação passa a
resultar em nulo na primeira e em `nan` na segunda:

```python
yd.di1.interpolar_taxas("25-04-2025", ["01-01-2020", "01-01-2027"])
# antes: [taxa do primeiro vértice, 0.13901]
# agora: [null, 0.13901]
```

Como nas demais funções vetoriais da biblioteca, o prazo inválido anula apenas
a sua linha; os outros prazos da série continuam calculados.

O mesmo vale para a curva: um vértice com prazo negativo é ignorado, como os
vértices nulos. Antes ele entrava no cálculo e alterava as taxas vizinhas; com
o alvo no prazo zero, `yd.Interpolador` gerava `ZeroDivisionError` e
`yd.interpolar` devolvia `inf`.

## Prazo deve ter tipo inteiro

O prazo em dias úteis é inteiro por definição. `yd.Interpolador` e
`yd.interpolar` aceitavam uma coluna float ou de texto e a convertiam em
silêncio, truncando a parte fracionária. Um inteiro com ruído de ponto
flutuante era lido com um dia a menos: `0.57 * 100` resulta em
`56.99999999999999` e virava 56.

Agora os prazos a interpolar e os vértices da curva devem ter tipo inteiro.
Outro tipo gera `TypeError`, que informa o parâmetro e o tipo recebido:

```python
flat_forward = yd.Interpolador([30, 60, 90], [0.045, 0.05, 0.055], "flat_forward")
flat_forward(pl.Series([45.0, 46.0]))
# TypeError: du deve ter tipo inteiro, mas tem Float64. Converta os prazos
# para inteiro antes de interpolar.
```

A regra é a mesma da chamada com um único valor, que já rejeitava float. O
erro aparece antes de qualquer cálculo, e não no meio de um lote. Quem tem
prazos em coluna float decide a conversão, porque conhece a origem dos valores:

```python
df.with_columns(dias_uteis=pl.col("dias_uteis").round().cast(pl.Int64))
```

Prazos nulos continuam aceitos e resultam em taxa nula, inclusive em uma
coluna vazia ou só com nulos. As funções de títulos e `yd.di1` calculam os
prazos internamente e não são afetadas.

## Sequência de prazos no `Interpolador`

A instância de `yd.Interpolador` aceita lista, tupla ou `pl.Series`, além do
inteiro, e devolve uma Series na ordem da entrada. Prazos nulos, negativos ou
além do último vértice sem extrapolação ficam nulos:

```python
flat_forward = yd.Interpolador([30, 60, 90], [0.045, 0.05, 0.055], "flat_forward")
flat_forward(45)
# 0.04833068080970859
flat_forward([15, 45, 100])
# [0.045, 0.048331, null]
```

A chamada direta é o atalho do método `interpolar`, que aceita as mesmas
entradas. Com isso, `yd.Interpolador` cobre todos os usos de uma única curva
com dois métodos: `interpolar` para inteiro ou sequência e `interpolar_expr` em
pipelines Polars.

## Valores retornados

Fora o prazo negativo em `yd.interpolar`, os valores de `yd.Interpolador` e de
`yd.interpolar` são os mesmos da 0.65.0. `interpolar` com sequência e
`interpolar_expr` dão exatamente a mesma taxa da chamada com inteiro.

As funções de curva zero de NTN-B1 e NTN-C passam a interpolar com
`yd.Interpolador`, como as demais funções de títulos. A taxa interpolada de
cada fluxo pode mudar no último dígito, na ordem de 1e-15; os valores cobertos
pelos testes não mudaram.
