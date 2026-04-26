---
title: "Introdução ao PYield"
description: "Biblioteca Python para análise de renda fixa brasileira com dados de ANBIMA, BCB e B3"
---

<meta property="og:title" content="Introdução ao PYield - Análise de Renda Fixa em Python">
<meta property="og:description" content="Biblioteca Python para análise de renda fixa brasileira com dados de ANBIMA, BCB e B3. Retorna Polars DataFrames para pipelines rápidos e type-safe.">
<meta property="og:image" content="https://crdcj.github.io/PYield/images/thumbnail.jpg">
<meta property="og:url" content="https://crdcj.github.io/PYield/articles/pyield_intro.html">
<meta property="og:type" content="article">

# PYield: Análise de Renda Fixa Brasileira em Python

## Introdução

Qualquer pessoa que trabalhe com análise de renda fixa no Brasil sabe que a obtenção de dados de fontes como ANBIMA, B3 e Banco Central pode ser uma tarefa trabalhosa. É preciso lidar com diferentes APIs, formatos de dados variados e, frequentemente, processar informações diretamente de sites. Para quem não tem acesso a terminais pagos como Bloomberg, esse desafio se torna ainda maior.

Além disso, há a complexidade do calendário de dias úteis brasileiro. Feriados nacionais, estaduais e municipais precisam ser considerados em praticamente todos os cálculos financeiros - desde a apuração de taxas até a precificação de títulos. Essa é uma funcionalidade básica, mas essencial para qualquer análise de renda fixa.

PYield foi criado para resolver esses problemas. É uma biblioteca Python que centraliza a obtenção e processamento de dados de instrumentos de renda fixa brasileiros, oferecendo uma API unificada e consistente.

## O que é PYield?

PYield é uma biblioteca Python especializada em análise de renda fixa brasileira. Ela busca e processa dados de múltiplas fontes:

- **ANBIMA**: Taxas indicativas de títulos públicos e dados de estoque (IMA-Q)
- **Banco Central (BCB)**: SELIC, PTAX, taxas de repositório, VNA
- **B3**: Futuros de DI, DDI, cupom cambial e outros contratos
- **IBGE**: Dados de inflação (IPCA)

Todos os dados retornados pela biblioteca são **Polars DataFrames** ou **Series**, proporcionando alto desempenho e segurança de tipos para pipelines de dados modernos.

## Características Principais

- **Coleta unificada de dados**: Uma única biblioteca para acessar ANBIMA, BCB, B3 e IBGE
- **API consistente**: Todos os módulos seguem as mesmas convenções de nomenclatura e assinaturas de função
- **Retorno em Polars**: DataFrames e Series do Polars para pipelines rápidos e type-safe
- **Calendário de dias úteis**: Funções completas para contagem e geração de dias úteis com feriados brasileiros integrados
- **Precificação de títulos**: Cálculo de cotações, preços e spreads de títulos públicos
- **Interpolação de taxas**: Suporte para interpolação linear e flat forward (padrão de mercado) usando convenção 252 dias úteis/ano
- **Conversão flexível de datas**: Aceita diversos formatos de entrada (strings DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD, objetos datetime, etc.)

## Instalação

A instalação é simples via pip:

```sh
pip install pyield
```

**Requisitos**: Python >= 3.12

## Exemplos Práticos

### 1. Dias Úteis (Business Days)

O módulo `du` é a base de todos os cálculos na biblioteca. Feriados brasileiros são automaticamente considerados.

```python
import pyield as yd

# Contar dias úteis entre duas datas (início inclusivo, fim exclusivo)
yd.du.contar("29-12-2023", "02-01-2024")  # -> 1

# Avançar N dias úteis a partir de uma data
yd.du.deslocar("29-12-2023", 1)  # -> datetime.date(2024, 1, 2)

# Ajustar data para o próximo dia útil (se não for dia útil)
yd.du.deslocar("30-12-2023", 0)  # -> datetime.date(2024, 1, 2)

# Como 29-12-2023 já é dia útil, retorna a mesma data
yd.du.deslocar("29-12-2023", 0)  # -> datetime.date(2023, 12, 29)

# Gerar série de dias úteis entre duas datas
yd.du.gerar("22-12-2023", "02-01-2024")
# -> Polars Series: [2023-12-22, 2023-12-26, 2023-12-27, 2023-12-28, 2023-12-29, 2024-01-02]

# Verificar se é dia útil
yd.du.eh_dia_util("25-12-2023")  # -> False (Natal)
```

Todas as funções suportam operações vetorizadas com listas, Series ou arrays.

### 2. Futuros de DI (B3)

Obtenha dados de contratos futuros negociados na B3:

```python
# Dados de Futuro de DI em uma data específica
df = yd.futuro.historico("31-05-2024", "DI1")

# DataFrame retornado contém colunas:
# data_referencia, codigo_negociacao, data_vencimento, dias_uteis, taxa_ajuste, ...

# Outros contratos disponíveis: DDI, FRC, FRO, DAP, DOL, WDO, IND, WIN
df_dap = yd.futuro.historico("31-05-2024", "DAP")  # Cupom Cambial
```

### 3. Títulos Públicos (Tesouro Nacional)

Acesse taxas indicativas da ANBIMA e dados de títulos públicos:

```python
# LTN (Letras do Tesouro Nacional - pré-fixado)
df_ltn = yd.ltn.dados("23-08-2024")
# Colunas: data_referencia, titulo, codigo_selic, data_base, data_vencimento, taxa_indicativa, ...

# NTN-B (Notas do Tesouro Nacional série B - IPCA+)
df_ntnb = yd.ntnb.dados("23-08-2024")
# Colunas: data_referencia, titulo, codigo_selic, data_base, data_vencimento, taxa_indicativa, ...

# NTN-F (Notas do Tesouro Nacional série F - pré-fixado com cupom)
df_ntnf = yd.ntnf.dados("23-08-2024")
```

### 4. Precificação de Títulos

Calcule cotações e preços de títulos públicos:

```python
# Cotação de NTN-B (base 100)
yd.ntnb.cotacao("31-05-2024", "15-05-2035", 0.061490)  # -> 99.3651

# Cotação para vencimento mais longo
yd.ntnb.cotacao("31-05-2024", "15-08-2060", 0.061878)  # -> 99.5341

# Prêmio sobre DI para títulos pré-fixados (em pontos-base)
df_premios = yd.ltn.premio("30-05-2024", pontos_base=True)
# Colunas: titulo, data_vencimento, premio

# Prêmio para NTN-F
df_premios_ntnf = yd.ntnf.premio("30-05-2024", pontos_base=True)
```

### 5. Interpolação de Taxas

Interpolar taxas de juros usando convenção de mercado (252 dias úteis/ano):

```python
# Obter curva de DI Futuro
df = yd.futuro.historico("31-05-2024", "DI1")

# Criar interpolador flat forward (padrão de mercado)
interp = yd.Interpolador(df["dias_uteis"], df["taxa_ajuste"], metodo="flat_forward")

# Interpolar para 45 dias úteis
interp(45)  # -> Taxa interpolada (ex: 0.1037)

# Vetorizado
interp([30, 60, 90])  # -> Polars Series com 3 taxas interpoladas

# Interpolação linear (alternativa)
linear_interp = yd.Interpolador(df["dias_uteis"], df["taxa_ajuste"], metodo="linear")
linear_interp(45)  # -> Taxa interpolada linearmente
```

### 6. Indicadores do Banco Central

Acesse indicadores econômicos do BCB:

```python
# SELIC Over (taxa anualizada)
yd.selic.over("31-05-2024")  # -> 0.104  (10.4% a.a.)

# PTAX (taxa de câmbio oficial)
yd.ptax("31-05-2024")  # -> 5.4407

# Taxa SELIC meta (definida pelo COPOM)
yd.selic.meta("31-05-2024")  # -> 0.1075  (10.75% a.a.)

# VNA da LFT (Valor Nominal Atualizado)
yd.lft.vna("31-05-2024")  # -> 15234.56
```

### 7. Inflação (IPCA)

Obtenha dados de inflação do IBGE:

```python
# Taxas mensais de IPCA
df_ipca = yd.ipca.taxas("01-01-2024", "01-03-2024")
# Colunas: periodo, taxa

# Índices de IPCA
df_indices = yd.ipca.indices("01-01-2024", "01-03-2024")
# Colunas: periodo, indice

# Projeções futuras (quando disponíveis)
df_proj = yd.ipca.taxa_projetada()
```

## Conversão para Pandas

Embora PYield retorne Polars DataFrames por padrão (desde a versão 0.40.0), é fácil converter para Pandas quando necessário:

```python
import pyield as yd

# Obter DataFrame Polars
df_polars = yd.ltn.dados("23-08-2024")

# Converter para Pandas
df_pandas = df_polars.to_pandas(use_pyarrow_extension_array=True)
```

A conversão com `use_pyarrow_extension_array=True` mantém a compatibilidade de tipos e oferece melhor desempenho.

## Manuseio de Datas

PYield aceita formatos flexíveis de data (`DateLike`):

- **Strings**: `"31-05-2024"`, `"31/05/2024"`, `"2024-05-31"`
- **Objetos Python**: `datetime.date`, `datetime.datetime`
- **Objetos Pandas/NumPy**: `pandas.Timestamp`, `numpy.datetime64`

Funções escalares retornam `datetime.date`. Funções vetorizadas retornam `polars.Series`.

Para valores nulos, funções escalares retornam `float('nan')`. Funções vetorizadas propagam `null` element-wise:

```python
# Exemplo com null
yd.ntnb.cotacao(None, "15-05-2035", 0.06149)  # -> nan
yd.du.contar(["01-01-2024", None], "01-02-2024")  # -> Series: [22, null]
```

## Recursos Adicionais

### Colab Notebook

Um notebook interativo com exemplos práticos está disponível no Google Colab:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)

### Documentação Completa

A documentação completa com referência da API está disponível em:
[https://crdcj.github.io/PYield/](https://crdcj.github.io/PYield/)

### Código-Fonte

O código da biblioteca é open source e está hospedado no GitHub:
[https://github.com/crdcj/PYield](https://github.com/crdcj/PYield)

## Conclusão

PYield oferece uma solução integrada para análise de renda fixa brasileira em Python. Com uma API unificada, suporte para múltiplas fontes de dados e retorno em Polars DataFrames, a biblioteca permite que você foque na análise em vez de se preocupar com a obtenção e processamento de dados.

Se você trabalha com renda fixa no Brasil - seja como analista, pesquisador ou desenvolvedor - PYield pode acelerar significativamente seu fluxo de trabalho.

Contribuições são bem-vindas! Entre em contato: cr.cj@outlook.com
