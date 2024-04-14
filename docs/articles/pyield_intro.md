---
title: "Introdução ao PYield"
description: "Este artigo fornece uma introdução à biblioteca PYield, explicando seus conceitos básicos e como utilizá-la."
---

<meta property="og:title" content="Introdução ao PYield">
<meta property="og:description" content="Este artigo fornece uma introdução à biblioteca PYield, explicando seus conceitos básicos e como utilizá-la.">
<meta property="og:image" content="https://crdcj.github.io/PYield/images/thumbnail.webp">
<meta property="og:url" content="https://crdcj.github.io/PYield/articles/pyield_intro.html">
<meta property="og:type" content="article">

# Uma biblioteca em Python para obteção de dados relacionado a instrumentos de Renda Fixa brasileira

Se você é um entusiasta de VBA e Excel, pode pular esse artigo que aqui não é lugar para você! Brincadeira, você é bem-vindo também. Afinal, essa pode ser uma ótima desculpa para você finalmente aprender Python 😂

Brincadeiras à parte, qualquer um que trabalhe com análise de renda fixa no Brasil sabe que a obtenção de dados de fontes como ANBIMA e B3 pode ser uma tarefa complicada. Outro ponto refere-se ao tratamento dos feriados e dias úteis, um verdadeiro pesadelo para quem precisa calcular prazos e vencimentos, ainda mais depois que criaram um novo feriado nacional no final do ano passado. Sim, agora temos que considerar duas listas de feriados nacionais, uma para dados ateriores a 26-12-2023 e outra para depois.

Claro que para os afortunados com acesso a serviços pagos como Bloomberg, a obtenção desse tipo de dados já é bem fácil. Mas para a maioria dos analistas financeiros, pesquisadores e entusiastas do mercado, a obtenção e processamento desses dados pode ser um desafio. Afinal, você terá que lidar com chamadas para diversas APIs como a do IBGE, do BACEN, da ANBIMA, da B3, e por aí vai. Em alguns casos, o dado tem que ser extraído diretamente de sites, o que pode ser ainda mais complicado.

## O que é PYield?

A biblioteca Python foi projetada especificamente para a obtenção e tratamento de dados de instrumentos de renda fixa no Brasil. Ou seja, é uma tentativa de  simplificar a obtenção e processamento de dados de fontes primárias como ANBIMA e B3, fornecendo uma API de fácil utilização.

Utilizando a robustez de bibliotecas populares de Python, como Pandas, Requests e Numpy, PYield pode ser usada como backend de aplicações mais complexas, removendo a complexidade relacionada a obtenção e processamento de dados de renda fixa.

## Características Principais

- **Coleta de Dados**: Obtenha dados diretamente de fontes primárias como ANBIMA e B3 de forma simples e rápida.
- **Processamento de Dados**: Os dados são processados e entregues em formatos fáceis de usar, como DataFrames do Pandas.
- **Ferramentas de Análise**: Acesse funções embutidas para tarefas comuns de análise do mercado de renda fixa, como cálculos de dias úteis e feriados.

## Como Instalar o PYield

A instalação do PYield é rápida e fácil através do pip, o gerenciador de pacotes do Python. Basta abrir o terminal e executar o seguinte comando no seu ambiente virtual:

```sh
pip install pyield
```
Este comando instala a última versão do PYield, deixando você pronto para começar a utilizar a biblioteca em seus projetos.

Exemplos Práticos de Uso:

### Ferramentas de Dias Úteis (Feriados brasileiros são automaticamente considerados)
```python
>>> import pyield as yd

# Contar o número de dias úteis entre duas datas.
# A data de início é incluída, a data de término é excluída.
>>> yd.count_bdays(start='2023-12-29', end='2024-01-02')
1

# Obtenha o próximo dia útil após uma determinada data (offset=1).
>>> yd.offset_bdays(dates="2023-12-29", offset=1)
Timestamp('2024-01-02 00:00:00')

# Obtenha o próximo dia útil se não for um dia útil (offset=0).
>>> yd.offset_bdays(dates="2023-12-30", offset=0)
Timestamp('2024-01-02 00:00:00')

# Como 2023-12-29 já é um dia útil, a função retorna a mesma data (offset=0).
>>> yd.offset_bdays(dates="2023-12-29", offset=0)
Timestamp('2023-12-29 00:00:00')

# Gerar uma série de dias úteis entre duas datas.
>>> yd.generate_bdays(start='2023-12-29', end='2024-01-03')
0   2023-12-29
1   2024-01-02
2   2024-01-03
dtype: datetime64[ns]
```

## Dados de Futuro de DI
```python
# Obtenha um DataFrame com os dados dos Futuros de DI da B3 de uma data específica.
>>> yd.fetch_asset(asset_code="DI1", reference_date='2024-03-08')

TradeDate  ExpirationCode ExpirationDate BDToExpiration  ... LastRate LastAskRate LastBidRate SettlementRate
2024-03-08 J24            2024-04-01     15              ... 10.952   10.952      10.956      10.956
2024-03-08 K24            2024-05-02     37              ... 10.776   10.774      10.780      10.777
2024-03-08 M24            2024-06-03     58              ... 10.604   10.602      10.604      10.608
...        ...            ...            ...             ... ...      ...         ...         ...
2024-03-08 F37            2037-01-02     3213            ... <NA>     <NA>        <NA>        10.859
2024-03-08 F38            2038-01-04     3462            ... <NA>     <NA>        <NA>        10.859
2024-03-08 F39            2039-01-03     3713            ... <NA>     <NA>        <NA>        10.85
```

### Dados de Títulos do Tesouro
```python
# Obtenha um DataFrame com os dados dos títulos NTN-B da ANBIMA.
# Os dados da Anbima estão disponíveis para os últimos 5 dias úteis.
# Obs: Para quem é membro da Anbima, o acesso ao histórico é liberado automaticamente pela biblioteca.
>>> yd.fetch_asset(asset_code="NTN-B", reference_date='2024-04-12')

BondType ReferenceDate MaturityDate BidRate AskRate IndicativeRate Price
NTN-B    2024-04-12    2024-08-15   0.07540 0.07504 0.07523        4,271.43565
NTN-B    2024-04-12    2025-05-15   0.05945 0.05913 0.05930        4,361.34391
NTN-B    2024-04-12    2026-08-15   0.05927 0.05897 0.05910        4,301.40082
...      ...           ...          ...     ...     ...            ...
NTN-B    2024-04-12    2050-08-15   0.06039 0.06006 0.06023        4,299.28233
NTN-B    2024-04-12    2055-05-15   0.06035 0.05998 0.06017        4,367.13360
NTN-B    2024-04-12    2060-08-15   0.06057 0.06016 0.06036        4,292.26323
```

### Cálculo de spreads
```python
# Calcule o spread entre o futuro de DI e os títulos pré-fixados do Tesouro.
>>> yd.calculate_spreads(spread_type="di_vs_pre", reference_date="2024-4-11")

BondType ReferenceDate MaturityDate  DISpread
LTN      2024-04-11    2024-07-01    -20.28
LTN      2024-04-11    2024-10-01    -10.19
LTN      2024-04-11    2025-01-01    -15.05
...      ...           ...           ...
NTN-F    2024-04-11    2031-01-01    -0.66
NTN-F    2024-04-11    2033-01-01    -5.69
NTN-F    2024-04-11    2035-01-01    -1.27
```

### Dados de Indicadores
```python
# Obtenha a taxa SELIC meta do BCB em um determinado dia.
>>> yd.fetch_indicator(indicator_code="SELIC", reference_date='2024-04-12')
10.75

# Obtenha a taxa de inflação mensal IPCA do IBGE com base no mês de referência da data.
>>> yd.fetch_indicator(indicator_code="IPCA", reference_date='2024-03-18')
0.16

# Se o indicador não estiver disponível para a data de referência, o retorno será nulo (None).
>>> yd.fetch_indicator(indicator_code="IPCA", reference_date='2024-04-10')
None
```

## Conclusão

Se você precisa obter e tratar dados de renda fixa no Brasil, o PYield pode ser uma ferramenta valiosa nesse processo. Com uma API simples, o seu código pode se tornar mais limpo e eficiente, permitindo que você se concentre na análise dos dados em vez de se preocupar com a obtenção e processamento deles.