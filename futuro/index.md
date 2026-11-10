# Futuro

## Compra e venda em contratos de taxa

Contratos como DI1 e DAP são negociados em taxa. Na convenção da B3, o que se compra e se vende é a própria taxa:

- **comprar DI1 é comprar taxa**;
- **vender DI1 é vender taxa**.

É por isso que as ofertas de compra e venda têm a ordem descrita abaixo, o inverso da intuição de quem vem dos títulos públicos.

| Operação | Jargão | Equivale a | Ganha quando a taxa |
| --- | --- | --- | --- |
| Comprar DI1 (comprar taxa) | ficar **tomado**, tomar taxa | vender PU | sobe |
| Vender DI1 (vender taxa) | ficar **aplicado**, dar taxa | comprar PU | cai |

Quem toma taxa quer pagar barato, por isso oferta a taxa mais baixa. Quem dá taxa quer receber caro, por isso oferta a taxa mais alta. Assim, no livro de ofertas de um contrato de taxa:

- a **oferta de compra** (bid) é a de quem quer ficar tomado e tem a taxa **menor**;
- a **oferta de venda** (ask) é a de quem quer ficar aplicado e tem a taxa **maior**.

Quem toma taxa a mercado fecha negócio na oferta de venda, pagando a taxa maior. Quem dá taxa a mercado fecha na oferta de compra.

Nos títulos públicos a lógica é a oposta, porque o título é negociado em PU. Comprar uma LTN é comprar PU: quem compra quer PU menor, ou seja, taxa maior. A posição comprada no título é aplicada, como a posição vendida no DI1. Por isso, quem tem LTN em carteira se protege da alta de juros comprando DI1 (ficando tomado).

O ajuste diário segue a mesma equivalência: para calcular o ajuste, a B3 converte a taxa em PU, e a posição comprada em taxa é tratada como vendida em PU.

Na biblioteca, as colunas mantêm os nomes da B3. Em `yd.futuro.historico` (`taxa_ultima_oferta_compra` e `taxa_ultima_oferta_venda`) e em `yd.futuro.intradia` (`taxa_oferta_compra` e `taxa_oferta_venda`), compra é compra de taxa. Logo, a taxa de compra fica abaixo da de venda. Os contratos cotados por preço, como DOL, WDO, IND e WIN, seguem a convenção usual: compra abaixo de venda, em preço.

Contratos futuros negociados na B3.

## `datas_disponiveis(contrato)`

Retorna as datas disponíveis no dataset histórico cacheado.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `contrato` | `str` | Contrato futuro (ex.: `DI1`, `DOL`). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Series` | Series ordenada de datas para as quais há dados históricos. |

Examples:

```pycon
>>> yd.futuro.datas_disponiveis("DI1").head(3)
shape: (3,)
Series: 'data_referencia' [date]
[
    2018-01-02
    2018-01-03
    2018-01-04
]
```

## `dv01_expr(coluna_dias_uteis, coluna_taxa, coluna_preco)`

Retorna a expressão Polars para cálculo de DV01.

Fórmula: DV01 = (Duration / (1 + taxa)) \* preço \* 0,0001

Onde: - Duration = dias\_uteis / 252 - taxa deve estar em formato decimal (ex.: 0.145) - preço é o PU do contrato.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `coluna_dias_uteis` | `str` | Coluna com os dias úteis até o vencimento. | *required* |
| `coluna_taxa` | `str` | Coluna com a taxa em formato decimal. | *required* |
| `coluna_preco` | `str` | Coluna com o PU do contrato. | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | Uma `pl.Expr` com o DV01 na unidade de `coluna_preco`. |

Examples:

```pycon
>>> df = pl.DataFrame({"du": [252], "taxa": [0.10], "pu": [90_909.09]})
>>> df.select(dv01=yd.futuro.dv01_expr("du", "taxa", "pu"))
shape: (1, 1)
┌──────────┐
│ dv01     │
│ ---      │
│ f64      │
╞══════════╡
│ 8.264463 │
└──────────┘
```

## `enriquecer(df, contrato)`

Enriquece DataFrame bruto do Price Report (PR) da B3.

Aceita um DataFrame com colunas no schema original da B3 (ex.: `TradDt`, `TckrSymb`). Filtra pelo contrato informado, adiciona data de vencimento, dias úteis/corridos e colunas derivadas (dv01, taxa\_forward) conforme o contrato.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `df` | `DataFrame` | DataFrame com dados brutos do PR da B3. | *required* |
| `contrato` | `str` | Contrato futuro (ex.: `DI1`, `DOL`). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame Polars enriquecido e ordenado. |

## `vencimento(codigo, contrato)`

Calcula a data de vencimento de contratos futuros da B3.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `codigo` | `str \| ArrayLike \| None` | Código de negociação ou coleção de códigos. | *required* |
| `contrato` | `str` | Contrato futuro na B3 (ex.: `DI1` ou `DAP`). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `date \| Series \| None` | Data de vencimento para código escalar, ou `pl.Series` para coleção. |

Examples:

```pycon
>>> yd.futuro.vencimento("DI1F25", "DI1")
datetime.date(2025, 1, 2)
```

```pycon
>>> yd.futuro.vencimento(["DI1F25", "di1g25", "DI1E27"], "DI1")
shape: (3,)
Series: 'vencimento' [date]
[
    2025-01-02
    2025-02-03
    null
]
```

## `vencimento_expr(coluna_codigo, contrato)`

Cria expressão Polars para a data de vencimento de futuros da B3.

Assume códigos de negociação no formato padrão de futuros da B3 (ex.: `DI1F25`). A caixa dos caracteres é ignorada e o vencimento é ajustado para dia útil.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `coluna_codigo` | `str` | Nome da coluna com o código de negociação. | *required* |
| `contrato` | `str` | Contrato futuro na B3 (ex.: `DI1` ou `DAP`). | *required* |

Returns:

| Type | Description |
| --- | --- |
| `Expr` | Uma `pl.Expr` que resulta em Date. |

Examples:

```pycon
>>> df = pl.DataFrame({"codigo_negociacao": ["DI1F25", "di1g25", "DI1E27"]})
>>> df.select(
...     yd.futuro.vencimento_expr("codigo_negociacao", "DI1").alias(
...         "vencimento"
...     )
... )
shape: (3, 1)
┌────────────┐
│ vencimento │
│ ---        │
│ date       │
╞════════════╡
│ 2025-01-02 │
│ 2025-02-03 │
│ null       │
└────────────┘
```

Contratos DAP vencem no dia 15:

```pycon
>>> df = pl.DataFrame({"codigo_negociacao": ["DAPF25"]})
>>> df.select(
...     yd.futuro.vencimento_expr("codigo_negociacao", "DAP").alias(
...         "vencimento"
...     )
... )
shape: (1, 1)
┌────────────┐
│ vencimento │
│ ---        │
│ date       │
╞════════════╡
│ 2025-01-15 │
└────────────┘
```
