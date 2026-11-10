# Novidades na versão 0.64.0

Esta versão reúne as duas fontes de leilões de TPF em `yd.tpf.leiloes`, com a
escolha explícita da fonte pelo novo parâmetro `fonte`. A função
`yd.tpf.leiloes_bcb` deixa de existir.

## Fonte dos leilões de TPF

`yd.tpf.leiloes` passa a aceitar `fonte="tn"` (Tesouro Nacional, padrão) ou
`fonte="bc"` (Banco Central do Brasil). A API pública fica orientada ao
conceito, leilão de TPF, e não à fonte de dados.

Chamadas sem `fonte` continuam consultando o Tesouro Nacional, com os mesmos
parâmetros, colunas e valores da 0.63.0. Com `fonte="bc"`, o resultado é o
mesmo que `yd.tpf.leiloes_bcb` retornava: colunas, tipos e metodologia não
mudam.

Não há troca automática de fonte. As duas fontes diferem em colunas e em
metodologia, por exemplo nos financeiros calculados a partir do PU médio e na
cotação publicada pelo BCB para LFT e NTN-B até 10/06/2024. Se a fonte mudasse
conforme a disponibilidade do Tesouro, a mesma chamada poderia retornar
esquemas e valores diferentes. As diferenças estão documentadas nas notas de
[`yd.tpf.leiloes`](tpf.md#leiloes).

### Migração

Substitua `yd.tpf.leiloes_bcb` por `yd.tpf.leiloes` com `fonte="bc"`:

```python
# 0.63.0
df = yd.tpf.leiloes_bcb(data="19-08-2025")

# 0.64.0
df = yd.tpf.leiloes(data="19-08-2025", fonte="bc")
```

Quem usava o BCB como alternativa ao Tesouro indisponível pode tratar o erro
explicitamente:

```python
try:
    df = yd.tpf.leiloes(data="19-08-2025")
except requests.RequestException:
    df = yd.tpf.leiloes(data="19-08-2025", fonte="bc")
```

## Taxas do secundário mensal em decimal

As colunas `taxa_minima`, `taxa_media` e `taxa_maxima` de
`yd.tpf.secundario.mensal` e `yd.tpf.secundario.ler` passam a ser retornadas
em decimal (0.1215 = 12,15%), como as demais taxas da biblioteca, incluindo
`yd.tpf.secundario.intradia`. Até a 0.63.0 elas vinham em percentual
(12.15). O tipo continua `Float64`, e as demais colunas não mudam.

### Migração

Para manter a escala anterior, multiplique as três colunas por 100:

```python
df = yd.tpf.secundario.mensal("07-01-2025").with_columns(
    pl.col("taxa_minima", "taxa_media", "taxa_maxima") * 100
)
```

## `nome_comercial` obrigatório em `yd.ntnb1.dv01`

`yd.ntnb1.dv01` deixa de assumir Renda+ quando `nome_comercial` não é
informado, como as demais funções de `yd.ntnb1`. Antes, um Educa+ calculado
sem o argumento retornava o DV01 do Renda+ sem erro. Agora a omissão gera
`TypeError`.

### Migração

Informe o nome comercial explicitamente:

```python
r_mais = yd.ntnb1.NomeComercial.RENDA_MAIS

# 0.63.0
dv01 = yd.ntnb1.dv01("23-06-2025", "15-12-2084", "6.86%", pu)

# 0.64.0
dv01 = yd.ntnb1.dv01("23-06-2025", "15-12-2084", "6.86%", pu, r_mais)
```

## Prazos repetidos na curva zero e no interpolador

As funções de NTN-B Principal, NTN-B1 e NTN-C que recebem `curva_zero`
(`yd.ntnbp.taxa`, `yd.ntnb1.cotacao_curva_zero`, `yd.ntnb1.taxa_curva_zero`,
`yd.ntnc.cotacao_curva_zero` e `yd.ntnc.taxa_curva_zero`) passam a tratar
prazos repetidos da mesma forma:

- linhas idênticas são aceitas e consideradas uma única vez;
- o mesmo `dias_uteis` com taxas diferentes gera `ValueError`.

A regra vale também para `yd.Interpolador` e `yd.interpolar`, que estão na
base dessas funções. Em `yd.interpolar` com várias curvas, o conflito é
verificado dentro de cada data: o mesmo DU em datas diferentes é legítimo. A
mensagem de erro informa o DU, as taxas conflitantes e, se houver, a data:

```text
ValueError: Curva tem o mesmo prazo com taxas diferentes; informe uma única
taxa por prazo. Conflitos: 2025-01-03, DU 200: taxas [0.11, 0.13].
```

Antes, o interpolador usava a última linha de cada prazo, e o resultado
dependia da ordem da curva. A NTN-C rejeitava qualquer repetição, mesmo com
taxas iguais. Curvas geradas por `yd.ntnb.taxas_zero` e as curvas internas de
DI1 e TPF não têm prazos repetidos e não são afetadas.

O mesmo critério vale para os vértices informados aos bootstraps
`yd.ntnf.taxas_zero`, `yd.ntnf.taxas_zero_forwards` e `yd.ntnb.taxas_zero`
(também usado por `yd.ntnb.implicitas`). Antes, `yd.ntnf.taxas_zero` aceitava
o mesmo vencimento com taxas diferentes e o resultado dependia da ordem dos
vértices; `yd.ntnb.taxas_zero` repetia o vértice na saída ou falhava com
`RuntimeError`; e `yd.ntnf.taxas_zero_forwards` rejeitava até repetições
idênticas.

## Duration sem truncamento

`yd.ntnb.duration`, `yd.ntnc.duration` e `yd.ntnb1.duration` deixam de truncar
o resultado em 14 casas decimais, como já faziam `yd.ntnf.duration` e as
versões `duration_expr` usadas em `dados()`. A duration é uma medida
analítica em ponto flutuante, e o truncamento não segue a metodologia de
cotação do Tesouro. A diferença fica no último dígito do `float`, por exemplo
`yd.ntnc.duration("21-03-2025", "01-01-2031", "6.7626%")` passa de
`4.405363320448` para `4.405363320448003`. O truncamento dos anos úteis nas
cotações não muda.
