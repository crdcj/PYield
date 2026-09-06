---
title: "PYield"
description: "Documentação do PYield, toolkit Python para análise de renda fixa brasileira"
---

[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org)
[![Powered by Polars](https://img.shields.io/badge/Powered%20by-Polars-blue)](https://pola.rs/)
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)

# PYield: Toolkit de Renda Fixa Brasileira

PYield é uma biblioteca Python para análise, precificação e acompanhamento de
títulos públicos brasileiros. Ela busca e processa dados da ANBIMA, BCB, IBGE,
B3 e Tesouro Nacional, com saídas escalares, `polars.Series` e
`polars.DataFrame`.

## Por onde começar?

- [Quickstart](quickstart.md): instalação e primeiros fluxos de trabalho.
- [Mapa da API](api-map.md): visão por namespace e funções públicas.
- [Desenvolvimento e publicação](desenvolvimento.md): comandos com `uv`, build,
  documentação e publicação no PyPI.
- [Introdução ao PYield](articles/pyield_intro.md): contexto, fontes de dados e
  conceitos da biblioteca.

## Organização da documentação

A documentação está organizada por conceito financeiro e por fonte de dados:

- **Ferramentas:** [dias úteis](du.md), [interpolador](interpolador.md) e
  [taxas a termo](forwards.md).
- **Títulos públicos:** [TPF](tpf.md), [VNA](vna.md), [LFT](lft.md),
  [LTN](ltn.md), [NTN-B](ntnb.md), [NTN-F](ntnf.md) e demais títulos.
- **Mercado de futuros:** [DI1](di1.md) e [futuros](futuro.md).
- **Dados auxiliares:** [IPCA](ipca.md), [Selic](selic.md), [Copom](copom.md),
  [compromissadas](compromissada.md), [CPM](cpm.md) e [B3](b3.md).

As funções públicas retornam dados em português e seguem convenções consistentes
de datas, tipos e colunas. Consulte a página de cada módulo para parâmetros,
fórmulas, fontes e exemplos específicos.

## Links

- [Código-fonte no GitHub](https://github.com/crdcj/PYield)
- [Pacote no PyPI](https://pypi.org/project/pyield/)
- [Notebook de Quickstart no Colab](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb)
