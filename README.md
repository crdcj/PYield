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
| `yd.futuro` | módulo | Contratos futuros da B3 | `di1`, `historico`, `intradia`, `datas_disponiveis`, `vencimento`, `enriquecer`, `vencimento_expr` |
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
| `yd.vna` | módulo | Valores nominais atualizados dos títulos públicos | `valor`, `historico`, `ultimo`, `projetado`, `vigencia`, `calcular_vna` |
| `yd.copom` | módulo | Calendário automático do Copom | `calendario`, `proxima_reuniao` |
| `yd.compromissadas(inicio, fim)` | função | Leilões de operações compromissadas do BCB | |
| `yd.selic` | módulo | Selic e política monetária | `over`, `over_serie`, `meta`, `meta_serie` |
| `yd.cpm` | módulo | Opções digitais do COPOM e análises derivadas | `contratos`, `probabilidades` |
| `yd.ipca` | módulo | IPCA histórico e projetado | `indice`, `indice_serie`, `taxa`, `taxa_serie`, `taxa_projetada` |
| `yd.igpm` | módulo | Taxas mensais do IGP-M via SGS/BCB | `taxa`, `taxa_serie` |
| `yd.ptax(data)` | função | PTAX para uma data | |
| `yd.ptax_serie(inicio, fim)` | função | Série histórica da PTAX | |
| `yd.di_over(data)` | função | Taxa DI Over | |
| `yd.hoje()` | função | Data atual no Brasil | |
| `yd.agora()` | função | Data e hora atual no Brasil | |

O [mapa completo da API](https://crdcj.github.io/PYield/api-map/) inclui a
documentação detalhada e as assinaturas públicas.

## Novidades na versão 0.59.0

Resumo das principais mudanças em relação à série `0.58`, cuja última versão
foi a `0.58.2`.

### Taxas e DV01 analíticos

As funções `taxa` de `yd.ltn`, `yd.ntnf`, `yd.ntnb`, `yd.ntnc` e `yd.lft`
passam a retornar `float` sem truncamento, em vez de `Decimal` truncado em oito
casas. Entradas inválidas retornam `float("nan")`; use `math.isnan` no lugar de
`.is_nan()`.

`taxa` e `dv01` de todos os títulos descontam os fluxos com `dias úteis / 252`,
sem os truncamentos intermediários da STN. Os valores mudam levemente em
relação à versão anterior, e `taxa(pu(...))` recupera a taxa original apenas
aproximadamente. `pu` e `cotacao` seguem a metodologia oficial e não mudaram.

### Séries do IPCA

As consultas históricas do IPCA seguem o padrão das séries de Selic e PTAX:
`taxa(data)` e `indice(data)` retornam um valor; `taxa_serie` e `indice_serie`
retornam séries por intervalo ou pelos últimos meses.

Para migrar, substitua `ipca.taxas(inicio, fim)` por
`ipca.taxa_serie(inicio, fim)` e `ipca.indices(inicio, fim)` por
`ipca.indice_serie(inicio, fim)`. Substitua `ipca.taxas_ultimas(n)` por
`ipca.taxa_serie(ultimos=n)` e `ipca.indices_ultimos(n)` por
`ipca.indice_serie(ultimos=n)`. Os nomes antigos foram removidos; colunas,
unidades e valores retornados permanecem iguais.

Nas séries, `fim=None` usa a data atual no Brasil. Informe `inicio` ou
`ultimos`; quando informado, `ultimos` tem prioridade sobre as datas.
`taxa_projetada` permanece inalterada.

### Outras mudanças

- Novo módulo `yd.igpm`, com `taxa` e `taxa_serie` para consultar taxas mensais
  do IGP-M via SGS/BCB, em formato decimal.
- CPM passa a expor `yd.cpm.contratos(data)` e
  `yd.cpm.probabilidades(data, tipo_opcao="call")`. Esta última retorna todas
  as reuniões disponíveis; filtre o DataFrame para selecionar uma reunião.
- Leilões do BCB passam de `yd.bc.leiloes` para `yd.tpf.leiloes_bcb`, com nomes
  de colunas alinhados aos de `yd.tpf.leiloes`.
- No mercado secundário, `yd.tpf.secundario.ler(fonte)` substitui `ler_zip` e
  `zip_para_silver`: aceita caminho ou bytes de ZIP e inclui `financeiro` na
  saída. `nome_arquivo_mensal` deixa de ser público.
- `yd.ntnf.premio` foi removida. Use `yd.tpf.premios_pre(data)` e filtre
  `titulo == "NTN-F"` para obter o prêmio bruto.
- `yd.tpf.taxas` preserva colunas e tipos quando não há dados. Nos leilões do
  Tesouro, PU mínimo informado como zero passa a ser nulo, e o financeiro
  ofertado usa o PU médio calculado quando necessário.
- `yd.rmd` passa a propagar erros de download e processamento, em vez de
  retornar um DataFrame vazio nessas situações.

Consulte as [releases do GitHub](https://github.com/crdcj/PYield/releases)
para o histórico completo e as notas de migração de versões anteriores.

## Projeto

- [Código-fonte](https://github.com/crdcj/PYield)
- [Issues](https://github.com/crdcj/PYield/issues)
- [Licença MIT](LICENSE)
