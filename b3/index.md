# B3

APIs técnicas para dados brutos ou intermediários da B3.

APIs técnicas da B3.

## boletim

Boletim de Negociação da B3.

Este módulo expõe helpers técnicos para buscar e ler o Price Report da B3. As funções preservam o schema bruto da fonte e servem como base para camadas públicas enriquecidas, como `futuro` e `cpm`.

Estrutura resumida de um registro do XML bruto da B3::

```text
<PricRpt>
    <TradDt><Dt>2026-04-01</Dt></TradDt>
    <SctyId><TckrSymb>DI1F31</TckrSymb></SctyId>
    <FinInstrmId>
        <OthrId>
            <Id>200000235664</Id>
            <Tp><Prtry>8</Prtry></Tp>
        </OthrId>
        <PlcOfListg><MktIdrCd>BVMF</MktIdrCd></PlcOfListg>
    </FinInstrmId>
    <TradDtls><TradQty>29880</TradQty></TradDtls>
    <FinInstrmAttrbts>
        <MktDataStrmId>E</MktDataStrmId>
        ...
        <MinTradLmt Ccy="BRL">12.87</MinTradLmt>
    </FinInstrmAttrbts>
</PricRpt>
```

Ref: https://www.b3.com.br/data/files/16/70/29/9C/6219D710C8F297D7AC094EA8/Catalogo\_precos\_v1.3.pdf

## `baixar_zip(data, boletim_completo=False)`

Baixa o ZIP bruto do Boletim de Negociação da B3.

Faz o download do Price Report da B3, no formato completo (PR) ou no simplified price report (SPR), e retorna apenas ZIPs estruturalmente válidos para persistência como dado bruto.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de negociação no formato 'DD-MM-YYYY', 'DD/MM/YYYY', 'YYYY-MM-DD' ou objeto datetime.date. | *required* |
| `boletim_completo` | `bool` | Se False (padrão), usa o simplified price report (SPR). Se True, usa o price report completo (PR). | `False` |

Returns:

| Type | Description |
| --- | --- |
| `bytes` | Conteúdo do ZIP externo em bytes quando o ZIP contém um XML legível. |
| `bytes` | Retorna `b""` para data sem pregão ou quando a resposta não contém |
| `bytes` | um ZIP válido. |

## `buscar(data, prefixo_ticker=None, comprimento_ticker=None, boletim_completo=False)`

Busca e processa o Boletim de Negociação da B3 no site oficial.

Faz o download do Price Report da B3, no formato completo (PR) ou no simplified price report (SPR), extrai o XML do ZIP publicado em `pesquisapregao` e devolve um DataFrame Polars com dados brutos e colunas no padrão original da B3 (nomes em inglês das tags XML).

O DataFrame retornado **não** contém colunas calculadas (dias\_uteis, dias\_corridos, dv01, taxa\_forward) nem normalização semântica por classe de ativo. O enriquecimento é responsabilidade do módulo consumidor (ex.: `futuro.historico`).

Como esta é uma camada bruta/intermediária, próxima do XML original da B3, os parâmetros de filtro usam a terminologia da fonte (`ticker`/`TckrSymb`). Nas camadas públicas enriquecidas da biblioteca, o vocabulário preferido é o canônico da lib (ex.: `contrato`, `codigo_negociacao`).

**Nota**

O dataset cacheado `pr` (arquivo `b3_pr.parquet`) pode conter um subconjunto de colunas focado em futuros. Esta função, porém, opera no schema bruto do XML da B3 para a data/relatório consultados.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | Data de negociação no formato 'DD-MM-YYYY', 'DD/MM/YYYY', 'YYYY-MM-DD' ou objeto datetime.date. | *required* |
| `prefixo_ticker` | `str \| list[str] \| None` | Prefixo do ticker B3 (ex.: 'DI1', 'DOL', 'CPM') ou lista de prefixos (ex.: \['DI1', 'DAP'\]). Usado como filtro starts-with no XML. Se None (padrão), retorna todos os ativos sem filtro. | `None` |
| `comprimento_ticker` | `int \| None` | Comprimento exato do ticker para filtrar registros. Se None (padrão), retorna todos os tickers que casam com o prefixo (ex.: 6 para futuros, 13 para opções). | `None` |
| `boletim_completo` | `bool` | Se False (padrão), usa o simplified price report (SPR), arquivo leve (\~2 KB) com apenas preços de ajuste. Se True, usa o price report completo (PR, \~2 MB) com todos os dados de negociação. | `False` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame com colunas tipadas no padrão original do XML, |
| `DataFrame` | ordenado por ticker (`TckrSymb`). Inclui todos os registros do XML (sem filtro de |
| `DataFrame` | vencimento). Retorna DataFrame vazio para data inválida ou resposta |
| `DataFrame` | vazia. |

**Output Columns**

- TradDt (Date): data de negociação.
- TckrSymb (String): código de negociação na B3.
- Id (String): identificador do instrumento.
- Prtry (String): tipo do identificador proprietário.
- MktIdrCd (String): código do mercado.
- DaysToSttlm (Int64): dias para liquidação.
- TradQty (Int64): número de negócios.
- MktDataStrmId (String): identificador do fluxo de dados.
- NtlFinVol (Float64): volume financeiro nacional bruto.
- IntlFinVol (Float64): volume financeiro internacional.
- OpnIntrst (Int64): contratos em aberto.
- FinInstrmQty (Int64): quantidade negociada de instrumentos financeiros.
- BestBidPric (Float64): ultima melhor oferta de compra no snapshot diario; pode ser nulo.
- BestAskPric (Float64): ultima melhor oferta de venda no snapshot diario; pode ser nulo. diario; pode ser nulo.
- FrstPric (Float64): preço de abertura.
- MinPric (Float64): preço mínimo negociado.
- MaxPric (Float64): preço máximo negociado.
- TradAvrgPric (Float64): preço médio negociado.
- LastPric (Float64): preço de fechamento.
- RglrTxsQty (Int64): número de negócios regulares.
- NonRglrTxsQty (Int64): número de negócios não regulares.
- RglrTraddCtrcts (Int64): contratos regulares negociados.
- NonRglrTraddCtrcts (Int64): contratos não regulares negociados.
- NtlRglrVol (Float64): volume financeiro regular nacional.
- NtlNonRglrVol (Float64): volume não regular nacional.
- IntlRglrVol (Float64): volume regular internacional.
- IntlNonRglrVol (Float64): volume não regular internacional.
- AdjstdQt (Float64): preço/cotação de ajuste.
- AdjstdQtTax (Float64): taxa de ajuste.
- AdjstdQtStin (String): indicador de cotação ajustada.
- PrvsAdjstdQt (Float64): preço/cotação de ajuste do dia anterior.
- PrvsAdjstdQtTax (Float64): taxa de ajuste do dia anterior.
- PrvsAdjstdQtStin (String): indicador de ajuste anterior.
- OscnPctg (Float64): percentual de oscilação.
- VartnPts (Float64): variação em pontos.
- EqvtVal (Float64): valor equivalente.
- AdjstdValCtrct (Float64): valor do contrato ajustado.
- MaxTradLmt (Float64): limite máximo de variação.
- MinTradLmt (Float64): limite mínimo de variação.

Raises:

| Type | Description |
| --- | --- |
| `HTTPError` | Se a requisição HTTP ao endpoint falhar. |
| `BadZipFile` | Se o ZIP estiver corrompido após re-download. |
| `XMLSyntaxError` | Se o XML recebido estiver malformado. |

Examples:

```pycon
>>> import pyield as yd
>>> df = yd.b3.boletim.buscar("26-04-2024", "DI1")
```

```pycon
>>> # Múltiplos contratos de uma vez
>>> df = yd.b3.boletim.buscar("26-04-2024", ["DI1", "DAP"])
```

## `ler(fonte, prefixo_ticker=None, comprimento_ticker=None)`

Lê e processa o price report da B3 a partir do dado bruto.

Mesma saída de :func:`buscar`, mas recebe o dado bruto em vez de baixar da rede. Aceita o ZIP externo como `bytes` ou `Path` (caminho para o arquivo ZIP), e também XML descomprimido como `bytes`.

Por operar diretamente sobre o dado bruto, esta função preserva a terminologia da fonte para filtros de `TckrSymb`.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `fonte` | `bytes \| Path` | ZIP externo em bytes, caminho (`Path`) para um arquivo ZIP no disco, ou XML já descomprimido em bytes. | *required* |
| `prefixo_ticker` | `str \| list[str] \| None` | Prefixo do ticker B3 (ex.: 'DI1', 'DOL', 'CPM') ou lista de prefixos (ex.: \['DI1', 'DAP'\]). Se None (padrão), retorna todos os ativos sem filtro. | `None` |
| `comprimento_ticker` | `int \| None` | Comprimento exato do ticker para filtrar registros. Se None (padrão), retorna todos os tickers que casam com o prefixo. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | pl.DataFrame: DataFrame com as mesmas colunas documentadas em |
| `DataFrame` | func:`buscar`. |
