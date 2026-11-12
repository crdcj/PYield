# Novidades na versão 0.64.0

Esta versão reúne as duas fontes de leilões de TPF em `yd.tpf.leiloes`, com a escolha explícita da fonte pelo novo parâmetro `fonte`. A função `yd.tpf.leiloes_bcb` deixa de existir.

## Fonte dos leilões de TPF

`yd.tpf.leiloes` passa a aceitar `fonte="tn"` (Tesouro Nacional, padrão) ou `fonte="bc"` (Banco Central do Brasil). A API pública fica orientada ao conceito, leilão de TPF, e não à fonte de dados.

Chamadas sem `fonte` continuam consultando o Tesouro Nacional, com os mesmos parâmetros, colunas e valores da 0.63.0. Com `fonte="bc"`, o resultado é o mesmo que `yd.tpf.leiloes_bcb` retornava: colunas, tipos e metodologia não mudam.

Não há troca automática de fonte. As duas fontes diferem em colunas e em metodologia, por exemplo nos financeiros calculados a partir do PU médio e na cotação publicada pelo BCB para LFT e NTN-B até 10/06/2024. Se a fonte mudasse conforme a disponibilidade do Tesouro, a mesma chamada poderia retornar esquemas e valores diferentes. As diferenças estão documentadas nas notas de [`yd.tpf.leiloes`](<https://crdcj.github.io/PYield/tpf/index.md#leiloes>).

### Migração

Substitua `yd.tpf.leiloes_bcb` por `yd.tpf.leiloes` com `fonte="bc"`:

```python
# 0.63.0
df = yd.tpf.leiloes_bcb(data="19-08-2025")

# 0.64.0
df = yd.tpf.leiloes(data="19-08-2025", fonte="bc")
```

Quem usava o BCB como alternativa ao Tesouro indisponível pode tratar o erro explicitamente:

```python
try:
    df = yd.tpf.leiloes(data="19-08-2025")
except requests.RequestException:
    df = yd.tpf.leiloes(data="19-08-2025", fonte="bc")
```

## Taxas do secundário mensal em decimal

As colunas `taxa_minima`, `taxa_media` e `taxa_maxima` de `yd.tpf.secundario.mensal` e `yd.tpf.secundario.ler` passam a ser retornadas em decimal (0.1215 = 12,15%), como as demais taxas da biblioteca, incluindo `yd.tpf.secundario.intradia`. Até a 0.63.0 elas vinham em percentual (12.15). O tipo continua `Float64`, e as demais colunas não mudam.

### Migração

Para manter a escala anterior, multiplique as três colunas por 100:

```python
df = yd.tpf.secundario.mensal("07-01-2025").with_columns(
    pl.col("taxa_minima", "taxa_media", "taxa_maxima") * 100
)
```

## `nome_comercial` obrigatório em `yd.ntnb1.dv01`

`yd.ntnb1.dv01` deixa de assumir Renda+ quando `nome_comercial` não é informado, como as demais funções de `yd.ntnb1`. Antes, um Educa+ calculado sem o argumento retornava o DV01 do Renda+ sem erro. Agora a omissão gera `TypeError`.

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

As funções de NTN-B Principal, NTN-B1 e NTN-C que recebem `curva_zero` (`yd.ntnbp.taxa`, `yd.ntnb1.cotacao_curva_zero`, `yd.ntnb1.taxa_curva_zero`, `yd.ntnc.cotacao_curva_zero` e `yd.ntnc.taxa_curva_zero`) passam a tratar prazos repetidos da mesma forma:

- linhas idênticas são aceitas e consideradas uma única vez;
- o mesmo `dias_uteis` com taxas diferentes gera `ValueError`.

A regra vale também para `yd.Interpolador` e `yd.interpolar`, que estão na base dessas funções. Em `yd.interpolar` com várias curvas, o conflito é verificado dentro de cada data: o mesmo DU em datas diferentes é legítimo. A mensagem de erro informa o DU, as taxas conflitantes e, se houver, a data:

```text
ValueError: Curva tem o mesmo prazo com taxas diferentes; informe uma única
taxa por prazo. Conflitos: 2025-01-03, DU 200: taxas [0.11, 0.13].
```

Antes, o interpolador usava a última linha de cada prazo, e o resultado dependia da ordem da curva. A NTN-C rejeitava qualquer repetição, mesmo com taxas iguais. Curvas geradas por `yd.ntnb.taxas_zero` e as curvas internas de DI1 e TPF não têm prazos repetidos e não são afetadas.

O mesmo critério vale para os vértices informados aos bootstraps `yd.ntnf.taxas_zero`, `yd.ntnf.taxas_zero_forwards` e `yd.ntnb.taxas_zero` (também usado por `yd.ntnb.implicitas`). Antes, `yd.ntnf.taxas_zero` aceitava o mesmo vencimento com taxas diferentes e o resultado dependia da ordem dos vértices; `yd.ntnb.taxas_zero` repetia o vértice na saída ou falhava com `RuntimeError`; e `yd.ntnf.taxas_zero_forwards` rejeitava até repetições idênticas.

## Duration sem truncamento

`yd.ntnb.duration`, `yd.ntnc.duration` e `yd.ntnb1.duration` deixam de truncar o resultado em 14 casas decimais, como já faziam `yd.ntnf.duration` e as versões `duration_expr` usadas em `dados()`. A duration é uma medida analítica em ponto flutuante, e o truncamento não segue a metodologia de cotação do Tesouro. A diferença fica no último dígito do `float`, por exemplo `yd.ntnc.duration("21-03-2025", "01-01-2031", "6.7626%")` passa de `4.405363320448` para `4.405363320448003`. O truncamento dos anos úteis nas cotações não muda.

## Taxas em `Decimal`

As funções escalares que recebem uma taxa passam a aceitar `Decimal` de forma uniforme, além de `float` e do percentual textual (`"6.25%"`). Antes, `cotacao`, `pu` e parte dos `dv01` já aceitavam, mas as funções abaixo terminavam em erro do Polars ou em `TypeError`:

- `duration` de `yd.ntnb`, `yd.ntnc`, `yd.ntnb1` e `yd.ntnf`;
- `dv01` de `yd.ntnb1` e `yd.ntnbp`;
- `rentabilidade` de `yd.ltn`, `yd.lft` e `yd.ntnf`, e `yd.ntnf.premio_limpo`.

O `Decimal` é convertido para `float` antes do cálculo, e o resultado é o mesmo obtido com o `float` equivalente. Chamadas existentes não mudam.

## Ofertas de compra e venda em `yd.futuro.historico`

Nos contratos cotados por taxa (DI1, DAP, DDI, FRC e FRO), as colunas `taxa_ultima_oferta_compra` e `taxa_ultima_oferta_venda` de `yd.futuro.historico` e `yd.futuro.enriquecer` estavam trocadas. Até a 0.63.0, a melhor oferta de compra da B3 saía como venda e vice-versa, e a taxa de compra ficava acima da de venda.

As colunas passam a seguir a convenção da B3, a mesma de `yd.futuro.intradia`: comprar um contrato de taxa é comprar taxa, o que equivale a vender PU. Por isso a oferta de compra tem a taxa menor. A ordem das colunas também muda: compra vem antes de venda. Os contratos cotados por preço (DOL, WDO, IND e WIN) não mudam.

### Migração

Para manter os valores da 0.63.0, troque os nomes das duas colunas:

```python
df = yd.futuro.historico("06-11-2026", "DI1").rename(
    {
        "taxa_ultima_oferta_compra": "taxa_ultima_oferta_venda",
        "taxa_ultima_oferta_venda": "taxa_ultima_oferta_compra",
    }
)
```

## Correções

As correções abaixo alteram apenas resultados que antes estavam errados ou terminavam em erros sem relação com a entrada. Os demais valores não mudam.

- `yd.ntnf.premio_limpo` retorna NaN quando algum pagamento fica além do último vértice da curva DI, como `yd.ntnf.rentabilidade` já fazia. Antes, o spread era calculado com apenas parte dos fluxos. O mesmo vale para a coluna `premio_limpo` de `yd.ntnf.dados`.
- `yd.ntnf.taxas_zero` calcula a taxa zero do vértice que tem um único pagamento depois da última LTN; nesse caso ela é a própria TIR. Antes, o vértice ficava nulo e os seguintes, NaN. A correção alcança `yd.ntnf.dados` e `yd.tpf.curva_pre`.
- `yd.ntnb1.cotacao_curva_zero` e `yd.ntnb1.taxa_curva_zero` retornam NaN quando não há pagamentos após a liquidação e geram `ValueError` para curva sem vértice válido, como `yd.ntnbp.taxa` e as funções de curva da NTN-C. Antes, a cotação saía `0.0` e a taxa terminava em `IndexError` ou `TypeError`.
- `yd.ntnb.taxas_zero`, `yd.ntnb.implicitas`, `yd.ntnf.taxas_zero` e `yd.ntnf.taxas_zero_forwards` ignoram vértices com taxa nula ou NaN, que representam dado ausente: o vértice sai do cálculo e do resultado, e a remoção é registrada no log. Antes, a NTN-B terminava em `TypeError` ou em um erro do solver; em `yd.ntnf.taxas_zero`, a taxa zero do vértice era calculada com uma TIR interpolada dos vizinhos, ou a função terminava em `TypeError` quando o vértice sem taxa era o último; e `yd.ntnf.taxas_zero_forwards` gerava `ValueError`.
- `yd.ntnb.taxas_zero` e `yd.ntnb.implicitas` geram `ValueError` para taxas infinitas ou menores ou iguais a -1, que antes terminavam em `ZeroDivisionError` ou em um erro do solver.
- `yd.tpf.leiloes` com `fonte="tn"` e várias datas retorna o resultado ordenado e sem repetir linhas quando a mesma data é informada mais de uma vez, como já ocorria com `fonte="bc"`. Sem PTAX disponível, as colunas `ptax` e `dv01_*_usd` ficam nulas nas duas fontes, em vez de ausentes na fonte TN e de erro na fonte BC.
- As funções de `yd.ntnb1` deixam de gerar `ValueError` quando o vencimento é menor ou igual à liquidação. Título vencido ou que vence na liquidação não tem mais fluxos: `datas_pagamento` e `fluxos_caixa` retornam vazio, e `cotacao`, `duration`, `dv01`, `cotacao_curva_zero` e `taxa_curva_zero` retornam NaN, como nos demais títulos.
- `rentabilidade` de `yd.ltn`, `yd.lft` e `yd.ntnf` retorna NaN quando a razão sobre o DI não é definida: taxa DI igual a zero ou alguma taxa menor ou igual a -1. As versões `rentabilidade_expr` seguem a mesma regra. Antes, os escalares de LTN e LFT terminavam em `ZeroDivisionError` com DI zero e devolviam um número complexo para taxa abaixo de -1; as expressões devolviam `inf`, `-inf` ou NaN; e a NTN-F devolvia `inf` ou `0.0`.
- `yd.tpf.leiloes` com `fonte="bc"`, em leilão sem aceite: `financeiro_ofertado_total` fica nulo, como já ocorria com `financeiro_ofertado_1v` e `financeiro_ofertado_2v`, pois sem PU o financeiro ofertado é desconhecido; antes, o total saía `0.0`. Já `financeiro_aceito_1v` e `financeiro_aceito_2v` passam a valer `0.0`, em vez de nulos, pois quantidade aceita zero significa financeiro aceito zero; `financeiro_aceito_total` continua `0.0`.
- `yd.ltn.taxas_forward` sem data retorna DataFrame vazio com as colunas documentadas.
- `yd.futuro.vencimento` aceita código nulo e retorna `None`, como a assinatura já indicava. `yd.futuro.historico` e `yd.futuro.enriquecer` exigem o contrato completo de três caracteres; um prefixo parcial, como `"D"`, retorna vazio em vez de misturar contratos.
- Datas informadas como string em branco deixam de gerar `AttributeError` ou `TypeError` em `yd.vna.vigencia` (`ValueError`), `yd.vna.historico`, `yd.ipca.taxa_serie`, `yd.ipca.indice_serie`, `yd.futuro.historico` e nas funções `datas_pagamento` dos títulos (resultado vazio).
- Consultas externas que recebem HTTP 304 (Not Modified) sem terem feito requisição condicional geram `requests.HTTPError` com a URL consultada. O SGS do Banco Central passou a responder assim, com corpo vazio, para algumas séries e quantidades de `ultimos`. Antes, a resposta terminava em `JSONDecodeError`, que não indicava a falha da fonte.
