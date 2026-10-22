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
| `yd.vna` | módulo | Valores nominais atualizados dos títulos públicos | `valor`, `historico`, `ultimo`, `projetado`, `vigencia`, `contexto`, `calcular_vna` |
| `yd.copom` | módulo | Calendário automático do Copom | `calendario`, `proxima_reuniao` |
| `yd.compromissadas(inicio, fim)` | função | Leilões de operações compromissadas do BCB | |
| `yd.selic` | módulo | Selic e política monetária | `over`, `over_serie`, `meta`, `meta_serie` |
| `yd.cpm` | módulo | Opções digitais do COPOM e análises derivadas | `contratos`, `probabilidades` |
| `yd.ipca` | módulo | IPCA histórico e projetado | `indice`, `indice_serie`, `taxa`, `taxa_serie`, `taxa_projetada`, `calendario` |
| `yd.igpm` | módulo | Taxas e números-índice do IGP-M | `taxa`, `taxa_serie`, `indice`, `indice_serie`, `calendario` |
| `yd.ptax(data)` | função | PTAX para uma data | |
| `yd.ptax_serie(inicio, fim)` | função | Série histórica da PTAX | |
| `yd.di_over(data)` | função | Taxa DI Over | |
| `yd.hoje()` | função | Data atual no Brasil | |
| `yd.agora()` | função | Data e hora atual no Brasil | |

O [mapa completo da API](https://crdcj.github.io/PYield/api-map/) inclui a
documentação detalhada e as assinaturas públicas.

## Novidades na versão 0.60.0

Esta seção cobre apenas as mudanças da `0.59.0` para a `0.60.0`. Consulte também as
[notas de migração](https://crdcj.github.io/PYield/release-0.60.0/).

### Contratos com Decimal

Os históricos e cálculos de VNA e os indicadores de inflação passam a preservar
precisão decimal. Revise operações que misturam esses retornos com `float`:
use `Decimal("0.0045")` para constantes exatas e `.is_nan()` para verificar
um retorno escalar `Decimal("NaN")`. Converta para `float` explicitamente
apenas quando a aplicação aceitar perder essa precisão.

| Operação | Retorno na 0.60.0 |
|---|---|
| `vna.valor`, `vna.projetado`, `vna.calcular_vna` | `Decimal` |
| Coluna `vna` de `vna.historico` e `vna.ultimo` | `Decimal(18, 6)` |
| `ipca.indice` / coluna `indice` de `ipca.indice_serie` | `Decimal` / `Decimal(18, 2)` |
| `ipca.taxa` / coluna `taxa` de `ipca.taxa_serie` | `Decimal` / `Decimal(18, 4)` |
| `ipca.taxa_projetada().valor_projetado` | `Decimal` com quatro casas |
| `igpm.taxa` / coluna `taxa` de `igpm.taxa_serie` | `Decimal` / `Decimal(18, 4)` |
| `igpm.indice` / coluna `indice` de `igpm.indice_serie` | `Decimal` / `Decimal(18, 10)` |

`vna.calcular_vna` também passa de `float("nan")` para `Decimal("NaN")`
na ausência de dados. Os cálculos de VNA usam 28 dígitos de precisão e aplicam
os truncamentos e arredondamentos da metodologia; resultados intermediários
podem diferir dos obtidos anteriormente com `float`.

### Séries do IPCA

As consultas históricas do IPCA seguem o padrão das séries de Selic e PTAX:
`taxa(data)` e `indice(data)` retornam um valor; `taxa_serie` e `indice_serie`
retornam séries por intervalo ou pelos últimos meses.

Para migrar, substitua `ipca.taxas(inicio, fim)` por
`ipca.taxa_serie(inicio, fim)` e `ipca.indices(inicio, fim)` por
`ipca.indice_serie(inicio, fim)`. Substitua `ipca.taxas_ultimas(n)` por
`ipca.taxa_serie(ultimos=n)` e `ipca.indices_ultimos(n)` por
`ipca.indice_serie(ultimos=n)`. Os nomes antigos foram removidos. As colunas e unidades são preservadas;
os tipos numéricos mudam conforme descrito acima.

O número-índice agora usa `Decimal`: `ipca.indice(data)` retorna `Decimal`
(ou `Decimal("NaN")` quando indisponível), e a coluna `indice` de
`ipca.indice_serie(...)` usa `Decimal(18, 2)`. A conversão parte do texto do
IBGE, sem passar por `float`.

As taxas também usam `Decimal`: `ipca.taxa(data)` retorna `Decimal` (ou
`Decimal("NaN")`), e a coluna `taxa` de `ipca.taxa_serie(...)` usa
`Decimal(18, 4)`. O campo `valor_projetado` de `ipca.taxa_projetada()` passa a
ser `Decimal` com quatro casas. A unidade permanece decimal: 0,45% corresponde
a `Decimal("0.0045")`.

Nas séries, `fim=None` usa a data atual no Brasil. Informe `inicio` ou
`ultimos`; quando informado, `ultimos` tem prioridade sobre as datas.

### Contexto de VNA e calendários de inflação

- `yd.vna.contexto(...)` reúne os parâmetros da vigência de NTN-B ou NTN-C:
  VNAs publicados, competência, inflação e pendências. Exige `vencimento` para
  NTN-C e aceita uma projeção explícita quando a inflação realizada não existe.
  As entradas de inflação em `contexto` e `projetado` aceitam números decimais
  (`0.0045`) ou percentuais explícitos (`"0.45%"` ou `"0,45%"`). O retorno de
  `contexto` é decimal e pode ser passado diretamente a `projetado`.
  Migração: substitua o antigo `inflacao=0.45` por `inflacao="0.45%"` ou
  `inflacao=0.0045`; números puros agora representam taxas decimais.
- `yd.ipca.calendario(ano, atualizar=False)` consulta as datas previstas de
  divulgação no IBGE pelo ano da competência, incluindo dezembro divulgado no
  ano seguinte.
- `yd.igpm.calendario(ano, atualizar=False)` consulta as divulgações mensais
  na FGV IBRE, excluindo prévias decendiais. Os calendários não comprovam
  que os índices já foram publicados.

### Outras mudanças

- Novo módulo `yd.igpm`, com `taxa` e `taxa_serie` para consultar taxas mensais
  do IGP-M via SGS/BCB. `taxa` retorna `Decimal` (ou `Decimal("NaN")` quando
  indisponível), e a coluna `taxa` de `taxa_serie` usa `Decimal(18, 4)`.
  A unidade é decimal: 0,27% corresponde a `Decimal("0.0027")`.
  `indice` e `indice_serie` fornecem os números-índice da FGV via Ipeadata
  (série `IGP12_IGPM12`), como `Decimal` e coluna `Decimal(18, 10)`.
  As dez casas preservam o histórico anterior a 1994. As séries aceitam
  intervalo ou `ultimos`, com competência `periodo` no formato `YYYYMM`.
- No CPM, substitua `yd.cpm.data(data)` por `yd.cpm.contratos(data)` e use
  `yd.cpm.probabilidades(data, tipo_opcao="call")` para as probabilidades.
  Esta última retorna todas as reuniões disponíveis; filtre o DataFrame para
  selecionar uma reunião.
- Leilões do BCB passam de `yd.bc.leiloes` para `yd.tpf.leiloes_bcb`, com nomes
  de colunas alinhados aos de `yd.tpf.leiloes`.
- No mercado secundário, `yd.tpf.secundario.ler(fonte)` substitui `ler_zip` e
  `zip_para_silver`: aceita caminho ou bytes de ZIP e inclui `financeiro` na
  saída. `nome_arquivo_mensal` deixa de ser público.
- `yd.ntnf.premio` foi removida. Use `yd.tpf.premios_pre(data)` e filtre
  `titulo == "NTN-F"` para obter o prêmio bruto.
- `yd.tpf.taxas` e as consultas `dados` de LFT, LTN, NTN-B, NTN-C e NTN-F
  preservam colunas e tipos quando não há dados. Nos leilões do Tesouro, PU
  mínimo informado como zero passa a ser nulo, e o financeiro ofertado usa o
  PU médio calculado quando necessário.
- `yd.rmd` passa a propagar erros de download e processamento, em vez de
  retornar um DataFrame vazio nessas situações.

Consulte as [releases do GitHub](https://github.com/crdcj/PYield/releases)
para o histórico completo e as notas de migração de versões anteriores.

## Projeto

- [Código-fonte](https://github.com/crdcj/PYield)
- [Issues](https://github.com/crdcj/PYield/issues)
- [Licença MIT](LICENSE)
