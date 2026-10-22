# Novidades na versão 0.60.0

Resumo das principais mudanças em relação à `0.59.0`.

## Contratos com Decimal

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

As taxas analíticas e o DV01 dos títulos continuam retornando `float`, conforme
a mudança da `0.59.0`; `pu` e `cotacao` continuam usando `Decimal`.

## Séries do IPCA

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

## Contexto de VNA e calendários de inflação

- `yd.vna.contexto(...)` reúne os parâmetros da vigência de NTN-B ou NTN-C:
  VNAs publicados, competência, inflação e pendências. Exige `vencimento` para
  NTN-C e aceita uma projeção explícita quando a inflação realizada não existe.
  A inflação de `contexto` é decimal; multiplique por 100 para passá-la ao
  parâmetro percentual `inflacao` de `vna.projetado`.
- `yd.ipca.calendario(ano, atualizar=False)` consulta as datas previstas de
  divulgação no IBGE pelo ano da competência, incluindo dezembro divulgado no
  ano seguinte.
- `yd.igpm.calendario(ano, atualizar=False)` consulta as divulgações mensais
  na FGV IBRE, excluindo prévias decendiais. Os calendários não comprovam
  que os índices já foram publicados.

## Outras mudanças

- Novo módulo `yd.igpm`, com `taxa` e `taxa_serie` para consultar taxas mensais
  do IGP-M via SGS/BCB. `taxa` retorna `Decimal` (ou `Decimal("NaN")` quando
  indisponível), e a coluna `taxa` de `taxa_serie` usa `Decimal(18, 4)`.
  A unidade é decimal: 0,27% corresponde a `Decimal("0.0027")`.
  `indice` e `indice_serie` fornecem os números-índice da FGV via Ipeadata
  (série `IGP12_IGPM12`), como `Decimal` e coluna `Decimal(18, 10)`.
  As dez casas preservam o histórico anterior a 1994. As séries aceitam
  intervalo ou `ultimos`, com competência `periodo` no formato `YYYYMM`.
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

