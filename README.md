[![PyPI version](https://img.shields.io/pypi/v/pyield.svg)](https://pypi.python.org/pypi/pyield)
[![Made with Python](https://img.shields.io/badge/Python->=3.12-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![Powered by Polars](https://img.shields.io/badge/Powered%20by-Polars-blue)](https://pola.rs/)
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/crdcj/PYield/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue?logo=readthedocs&logoColor=white)](https://crdcj.github.io/PYield/)

# PYield: Toolkit de Renda Fixa Brasileira

Português | [English](https://github.com/crdcj/PYield/blob/main/README.en.md)

PYield é uma biblioteca Python voltada para análise de títulos públicos
brasileiros. Ela busca e processa dados da ANBIMA, BCB, IBGE, B3 e Tesouro
Nacional, retornando tipos nativos do Python, `polars.Series` ou
`polars.DataFrame`, conforme a operação.

Embora inclua dados e ferramentas de outros mercados, como DI1, DAP e PTAX,
esses recursos apoiam o objetivo central da biblioteca: análise, precificação e
acompanhamento de títulos públicos brasileiros.

## Instalação

Com `pip`:

```sh
pip install pyield
```

Em um projeto gerenciado pelo `uv`:

```sh
uv add pyield
```

## Próximos passos

- [Quickstart](https://crdcj.github.io/PYield/quickstart/): primeiros usos da biblioteca.
- [Documentação completa](https://crdcj.github.io/PYield/): conceitos e referência por módulo.
- [Mapa da API](https://crdcj.github.io/PYield/api-map/): namespaces e funções públicas.
- [Desenvolvimento e publicação](https://crdcj.github.io/PYield/desenvolvimento/): ambiente, verificações, build e PyPI.
- [Gerar e publicar a documentação](docs/desenvolvimento.md#documentação): geração, visualização local e GitHub Pages.
- [Notebook no Colab](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb): exploração interativa.
- [Pacote no PyPI](https://pypi.org/project/pyield/).

## Mapa da API

| Componente | Tipo | Finalidade | Funções públicas |
|---|---|---|---|
| `yd.du` | módulo | Dias úteis e calendário brasileiro | `contar`, `deslocar`, `eh_dia_util`, `gerar`, `ultimo_dia_util`, `contar_expr`, `deslocar_expr`, `eh_dia_util_expr` |
| `yd.Interpolador` | classe | Interpolação escalar e em pipelines Polars, com métodos `linear` e `flat_forward` | `interpolar`, `interpolar_expr` |
| `yd.interpolar(...)` | função | Interpolação vetorizada flat-forward, curva única ou multi-curva | |
| `yd.forward(...)` | função | Taxa a termo entre dois vértices | |
| `yd.forwards(...)` | função | Curva de taxas a termo | |
| `yd.futuro` | módulo | Contratos futuros da B3 | `di1`, `historico`, `intradia`, `datas_disponiveis`, `vencimento`, `enriquecer`, `dv01_expr`, `vencimento_expr` |
| `yd.di1` | módulo | Curva DI1 e interpolação | `dados`, `interpolar_taxas`, `interpolar_taxa`, `datas_disponiveis` |
| `yd.tpf` | módulo | Títulos públicos federais | `taxas`, `taxas_historicas`, `vencimentos`, `estoque`, `leiloes`, `leiloes_bcb`, `benchmarks`, `dealers`, `curva_pre`, `premios_pre`, `secundario` |
| `yd.rmd(aba)` | função | Relatório Mensal da Dívida do Tesouro Nacional | |
| `yd.lft` | módulo | LFT | `dados`, `vencimentos`, `cotacao`, `pu`, `taxa`, `rentabilidade`, `rentabilidade_expr` |
| `yd.ltn` | módulo | LTN | `dados`, `vencimentos`, `pu`, `taxa`, `duration_expr`, `dv01`, `dv01_expr`, `rentabilidade`, `rentabilidade_expr`, `taxas_forward` |
| `yd.ntnb` | módulo | NTN-B | `dados`, `vencimentos`, `datas_pagamento`, `fluxos_caixa`, `cotacao`, `pu`, `taxa`, `taxas_zero`, `duration`, `duration_expr`, `dv01`, `dv01_expr`, `implicitas` |
| `yd.ntnb1` | módulo | NTN-B1 (Educa+ e Renda+) | `NomeComercial`, `datas_pagamento`, `fluxos_caixa`, `cotacao`, `cotacao_curva_zero`, `taxa_curva_zero`, `pu`, `duration`, `dv01` |
| `yd.ntnbp` | módulo | NTN-B Principal | `cotacao`, `taxa`, `pu`, `dv01` |
| `yd.ntnc` | módulo | NTN-C | `dados`, `datas_pagamento`, `fluxos_caixa`, `cotacao`, `cotacao_curva_zero`, `taxa_curva_zero`, `pu`, `taxa`, `duration`, `duration_expr`, `dv01`, `dv01_expr` |
| `yd.ntnf` | módulo | NTN-F | `dados`, `vencimentos`, `datas_pagamento`, `fluxos_caixa`, `pu`, `taxa`, `taxas_zero`, `taxas_zero_forwards`, `premio_limpo`, `premio_limpo_expr`, `rentabilidade`, `rentabilidade_expr`, `duration`, `duration_expr`, `dv01`, `dv01_expr` |
| `yd.vna` | módulo | Valores nominais atualizados dos títulos públicos | `valor`, `historico`, `ultimo`, `projetado`, `vigencia`, `contexto`, `calcular_vna`, `calcular_base` |
| `yd.copom` | módulo | Calendário automático do Copom | `calendario`, `proxima_reuniao` |
| `yd.compromissadas(inicio, fim)` | função | Leilões de operações compromissadas do BCB | |
| `yd.selic` | módulo | Selic e política monetária | `over`, `over_serie`, `meta`, `meta_serie` |
| `yd.cpm` | módulo | Opções digitais do COPOM e análises derivadas | `contratos`, `probabilidades` |
| `yd.ipca` | módulo | IPCA histórico e projetado | `indice`, `indice_serie`, `taxa`, `taxa_serie`, `taxa_projetada`, `calendario` |
| `yd.igpm` | módulo | Taxas e números-índice do IGP-M | `taxa`, `taxa_serie`, `indice`, `indice_serie`, `calendario` |
| `yd.ptax(data)` | função | PTAX para uma data | |
| `yd.ptax_serie(inicio, fim)` | função | Série histórica da PTAX | |
| `yd.di_over(data)` | função | Taxa DI Over | |
| `yd.b3` | módulo | Boletins e calendário de pregões da B3 | `boletim`, `eh_pregao`, `pregao_anterior`, `proximo_pregao`, `ultimo_pregao` |
| `yd.hoje()` | função | Data atual no Brasil | |
| `yd.agora()` | função | Data e hora atual no Brasil | |

O [mapa completo da API](https://crdcj.github.io/PYield/api-map/) inclui a
documentação detalhada e as assinaturas públicas.

## Novidades na versão 0.61.0

Esta seção cobre apenas as mudanças da `0.60.0` para a `0.61.0`, que não
removem nem alteram APIs existentes.

- Calendário de pregões da B3 em `yd.b3`: `eh_pregao(data)`,
  `pregao_anterior(data)`, `proximo_pregao(data)` e
  `ultimo_pregao(data=None)`. Há pregão em todo dia útil brasileiro, exceto em
  24/12 e 31/12. Fechamentos excepcionais não recorrentes não são modelados.
- `yd.futuro.dv01_expr(coluna_dias_uteis, coluna_taxa, coluna_preco)` passa a
  ser público e retorna uma `pl.Expr` com o DV01 dos contratos futuros.

Para atualizar a partir de versões anteriores, consulte as
[notas de migração da 0.60.0](https://crdcj.github.io/PYield/release-0.60.0/)
e as [releases do GitHub](https://github.com/crdcj/PYield/releases).

## Projeto

- [Código-fonte](https://github.com/crdcj/PYield)
- [Issues](https://github.com/crdcj/PYield/issues)
- [Licença MIT](LICENSE)
