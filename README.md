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
- [Notebook no Colab](https://colab.research.google.com/github/crdcj/PYield/blob/main/examples/pyield_quickstart.ipynb): exploração interativa.
- [Pacote no PyPI](https://pypi.org/project/pyield/).

## Mapa da API

| Componente | Tipo | Finalidade | Funções públicas |
|---|---|---|---|
| `yd.du` | módulo | Dias úteis e calendário brasileiro | `contar`, `deslocar`, `eh_dia_util`, `gerar`, `ultimo_dia_util`, `contar_expr`, `deslocar_expr`, `eh_dia_util_expr` |
| `yd.Interpolador` | classe | Interpolação escalar e em pipelines Polars | `interpolar`, `interpolar_expr`, `linear`, `flat_forward` |
| `yd.interpolar(...)` | função | Interpolação vetorizada flat-forward, curva única ou multi-curva | |
| `yd.forward(...)` | função | Taxa a termo entre dois vértices | |
| `yd.forwards(...)` | função | Curva de taxas a termo | |
| `yd.futuro` | módulo | Contratos futuros da B3 | `di1`, `historico`, `intradia`, `datas_disponiveis`, `vencimento`, `enriquecer`, `vencimento_expr` |
| `yd.di1` | módulo | Curva DI1 e interpolação | `dados`, `interpolar_taxas`, `interpolar_taxa`, `datas_disponiveis` |
| `yd.tpf` | módulo | Títulos públicos federais | `taxas`, `taxas_historicas`, `vencimentos`, `estoque`, `leiloes`, `benchmarks`, `curva_pre`, `premios_pre`, `secundario` |
| `yd.rmd(aba)` | função | Relatório Mensal da Dívida do Tesouro Nacional | |
| `yd.lft` | módulo | LFT | `dados`, `vencimentos`, `cotacao`, `pu`, `taxa`, `vna`, `rentabilidade`, `rentabilidade_expr` |
| `yd.ltn` | módulo | LTN | `dados`, `vencimentos`, `pu`, `taxa`, `duration_expr`, `dv01`, `dv01_expr`, `rentabilidade`, `rentabilidade_expr`, `taxas_forward` |
| `yd.ntnb` | módulo | NTN-B | `dados`, `vencimentos`, `datas_pagamento`, `fluxos_caixa`, `cotacao`, `pu`, `taxa`, `taxas_zero`, `duration`, `dv01`, `dv01_expr`, `implicitas`, `curva` |
| `yd.ntnb1` | módulo | NTN-B1 (Educa+ e Renda+) | `NomeComercial`, `datas_pagamento`, `fluxos_caixa`, `cotacao`, `cotacao_curva_zero`, `taxa_curva_zero`, `pu`, `duration`, `dv01` |
| `yd.ntnbp` | módulo | NTN-B Principal | `cotacao`, `taxa`, `pu`, `dv01` |
| `yd.ntnc` | módulo | NTN-C | `dados`, `datas_pagamento`, `fluxos_caixa`, `cotacao`, `pu`, `taxa`, `duration`, `duration_expr`, `dv01`, `dv01_expr` |
| `yd.ntnf` | módulo | NTN-F | `dados`, `vencimentos`, `datas_pagamento`, `fluxos_caixa`, `pu`, `taxa`, `taxas_zero`, `premio`, `premio_limpo`, `premio_limpo_expr`, `rentabilidade`, `rentabilidade_expr`, `duration`, `duration_expr`, `dv01`, `dv01_expr` |
| `yd.vna` | módulo | Valores nominais atualizados dos títulos públicos | `valor`, `historico`, `projetado`, `vigencia` |
| `yd.copom` | módulo | Calendário automático do Copom | `calendario`, `proxima_reuniao` |
| `yd.compromissadas(...)` | função | Leilões de operações compromissadas do BCB | `inicio`, `fim` |
| `yd.selic` | módulo | Selic e política monetária | `over`, `over_serie`, `meta`, `meta_serie` |
| `yd.cpm` | módulo | Opções digitais do COPOM e análises derivadas | `data`, `probabilidades` |
| `yd.ipca` | módulo | IPCA histórico e projetado | `indice`, `indices`, `indices_ultimos`, `taxa`, `taxas`, `taxas_ultimas`, `taxa_projetada` |
| `yd.ptax(data)` | função | PTAX para uma data | |
| `yd.ptax_serie(inicio, fim)` | função | Série histórica da PTAX | |
| `yd.di_over(data)` | função | Taxa DI Over | |
| `yd.hoje()` | função | Data atual no Brasil | |
| `yd.agora()` | função | Data e hora atual no Brasil | |

O [mapa completo da API](https://crdcj.github.io/PYield/api-map/) inclui a
documentação detalhada e as assinaturas públicas.

## Compatibilidade da API

Este resumo parte da API da `0.56.0`. Mudanças que já faziam parte dessa versão
ou de versões anteriores estão no histórico das
[releases do GitHub](https://github.com/crdcj/PYield/releases).

| Antes | Agora |
|---|---|
| `ntnbp.taxas_zero(...)` | `ntnb.taxas_zero(...)`, sem `incluir_vertices` |
| `ntnb.taxas_zero(..., incluir_cupons=...)` | `ntnb.taxas_zero(...)`, sem `incluir_cupons` |
| `ntnb.taxas_zero(..., percentual=...)` | `ntnb.taxas_zero(...)`, com taxas em formato decimal |
| `yd.tpf.ntnb` | `yd.ntnb` ou `from pyield import ntnb` |
| `yd.tpf.rmd(aba)` | `yd.rmd(aba)` ou `from pyield import rmd` |
| `pyield.tpf.vna.calcular_vna(...)` | `yd.vna.calcular_vna(...)` |
| `yd.ntnb.vna(data)` | `yd.vna.valor("NTN-B", data)` |
| `yd.ntnb.vnas()` | `yd.vna.historico("NTN-B")` |
| `vna_projetado(...)` nos módulos de títulos | `yd.vna.projetado(titulo, data, vna_base, inflacao)` |
| `vigencia(data)` nos módulos de títulos | `yd.vna.vigencia(titulo, data)` |

Os módulos públicos de títulos ficam disponíveis na raiz, enquanto as
implementações continuam organizadas internamente em `pyield/tpf/titulos/`.
Consulte as [releases](https://github.com/crdcj/PYield/releases) para o
histórico completo.

## Projeto

- [Código-fonte](https://github.com/crdcj/PYield)
- [Issues](https://github.com/crdcj/PYield/issues)
- [Licença MIT](LICENSE)
