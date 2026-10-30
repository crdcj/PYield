# PYield: Toolkit de Renda Fixa Brasileira

PYield é uma biblioteca Python para análise, precificação e acompanhamento de títulos públicos brasileiros. Ela busca e processa dados da ANBIMA, BCB, IBGE, B3 e Tesouro Nacional, com saídas escalares, `polars.Series` e `polars.DataFrame`.

## Por onde começar?

- [Quickstart](<https://crdcj.github.io/PYield/quickstart/index.md>): instalação e primeiros fluxos de trabalho.
- [Novidades e migração para a 0.60.0](<https://crdcj.github.io/PYield/release-0.60.0/index.md>): contratos com `Decimal` e alterações da API.
- [Mapa da API](<https://crdcj.github.io/PYield/api-map/index.md>): visão por namespace e funções públicas.
- [Desenvolvimento e publicação](<https://crdcj.github.io/PYield/desenvolvimento/index.md>): comandos com `uv`, build, documentação e publicação no PyPI.
- [Introdução ao PYield](<https://crdcj.github.io/PYield/articles/pyield_intro/index.md>): contexto, fontes de dados e conceitos da biblioteca.

## Organização da documentação

A documentação está organizada por conceito financeiro e por fonte de dados:

- **Ferramentas:** [dias úteis](<https://crdcj.github.io/PYield/du/index.md>), [interpolador](<https://crdcj.github.io/PYield/interpolador/index.md>) e [taxas a termo](<https://crdcj.github.io/PYield/forwards/index.md>).
- **Títulos públicos:** [TPF](<https://crdcj.github.io/PYield/tpf/index.md>), [VNA](<https://crdcj.github.io/PYield/vna/index.md>), [LFT](<https://crdcj.github.io/PYield/lft/index.md>), [LTN](<https://crdcj.github.io/PYield/ltn/index.md>), [NTN-B](<https://crdcj.github.io/PYield/ntnb/index.md>), [NTN-F](<https://crdcj.github.io/PYield/ntnf/index.md>) e demais títulos.
- **Mercado de futuros:** [DI1](<https://crdcj.github.io/PYield/di1/index.md>) e [futuros](<https://crdcj.github.io/PYield/futuro/index.md>).
- **Dados auxiliares:** [IPCA](<https://crdcj.github.io/PYield/ipca/index.md>), [IGP-M](<https://crdcj.github.io/PYield/igpm/index.md>), [Selic](<https://crdcj.github.io/PYield/selic/index.md>), [Copom](<https://crdcj.github.io/PYield/copom/index.md>), [compromissadas](<https://crdcj.github.io/PYield/compromissada/index.md>), [CPM](<https://crdcj.github.io/PYield/cpm/index.md>) e [B3](<https://crdcj.github.io/PYield/b3/index.md>).

As funções públicas retornam dados em português e seguem convenções consistentes de datas, tipos e colunas. Consulte a página de cada módulo para parâmetros, fórmulas, fontes e exemplos específicos.

## Links

- [Código-fonte no GitHub](<https://github.com/crdcj/PYield>)
- [Pacote no PyPI](<https://pypi.org/project/pyield/>)
- [Notebook de Quickstart no Colab](<https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb>)
