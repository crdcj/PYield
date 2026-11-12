# Novidades na versão 0.65.0

Esta versão corrige a projeção de inflação retornada pela biblioteca, estende a mesma consulta ao IGP-M e padroniza a representação do mês de referência em todas as séries mensais.

## Projeção vigente de IPCA e IGP-M

`yd.ipca.taxa_projetada()` passa a retornar a projeção que vale hoje para a atualização do VNA, e `yd.igpm.taxa_projetada()` é nova, com o mesmo comportamento. As duas consultam a página [Projeções IPCA e IGP-M](<https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm>) do Grupo Consultivo Permanente Macroeconômico da ANBIMA.

Até a 0.64.1, a projeção do IPCA vinha do quadro de indicadores da ANBIMA (`indicadores.xls`). Essa planilha não informa a data de validade e pode continuar com a projeção anterior depois que a nova já está vigente. Na manhã de 12/11/2026, por exemplo, a planilha ainda mostrava 0,87% para o IGP-M de novembro, enquanto a projeção vigente desde aquele dia era 0,78%, coletada em 11/11.

A função seleciona, entre as projeções do mês corrente, a de maior data de validade que não seja posterior à data de hoje no Brasil. Uma projeção divulgada no fim da tarde só é retornada a partir da sua validade. Entre a divulgação do índice fechado e a validade da primeira projeção do mês seguinte, nenhuma projeção está vigente: nesse intervalo, a atualização do VNA usa o índice oficial, e a função retorna `None`.

O objeto retornado, `ProjecaoIndicador`, passa a ter:

| Campo | Tipo | Conteúdo |
| --- | --- | --- |
| `competencia` | `dt.date` | Primeiro dia do mês de referência da projeção |
| `valor_projetado` | `Decimal` | Projeção em decimal, com quatro casas (`0.0078` = 0,78%) |
| `data_coleta` | `dt.date` | Dia em que a ANBIMA coletou as projeções |
| `data_validade` | `dt.date` | Início da vigência para atualização do VNA |

### Migração

`ultima_atualizacao` deixa de existir, pois a nova fonte não publica esse dado; use `data_coleta` e `data_validade`. `periodo_referencia` (`"out/26"`) passa a ser `competencia` (`dt.date(2026, 10, 1)`). Trate o retorno `None`:

```python
# 0.64.1
projecao = yd.ipca.taxa_projetada()
projecao.periodo_referencia  # "out/26"

# 0.65.0
projecao = yd.ipca.taxa_projetada()
if projecao is not None:
    projecao.competencia  # dt.date(2026, 10, 1)
    projecao.data_validade  # dt.date(2026, 10, 26)
```

## Competência como `Date` nas séries mensais

O mês de referência dos dados mensais passa a ser a coluna `competencia`, do tipo `Date`, no primeiro dia do mês, como já era em `yd.ipca.calendario`, `yd.igpm.calendario` e `yd.vna.contexto`.

| Função | 0.64.1 | 0.65.0 |
| --- | --- | --- |
| `yd.ipca.taxa_serie`, `yd.ipca.indice_serie` | `periodo` (`Int64`, `202501`) | `competencia` (`Date`, `2025-01-01`) |
| `yd.igpm.taxa_serie`, `yd.igpm.indice_serie` | `periodo` (`Int64`, `202501`) | `competencia` (`Date`, `2025-01-01`) |
| `yd.rmd` (abas `"1.3"` e `"2.1"`) | `periodo` (`Date`) | `competencia` (`Date`) |

Os valores das taxas e dos índices não mudam. Python e Polars não têm um tipo para mês: a data no primeiro dia do mês permite comparar, ordenar, cruzar com outras datas e avançar meses (`dt.offset_by("1mo")`) sem conversão. O inteiro `YYYYMM` induzia aritmética errada (`202612 + 1 = 202613`) e exigia conversão para cruzar as séries com os calendários e com `yd.vna.contexto`. As fontes BCB/SGS e Ipeadata já publicam o mês dessa forma.

### Migração

Renomeie a coluna e compare com datas:

```python
# 0.64.1
df = yd.ipca.taxa_serie(ultimos=12)
df.filter(pl.col("periodo") == 202501)

# 0.65.0
df = yd.ipca.taxa_serie(ultimos=12)
df.filter(pl.col("competencia") == dt.date(2025, 1, 1))
```

Para manter o inteiro `YYYYMM` em código existente:

```python
df.with_columns(periodo=pl.col("competencia").dt.strftime("%Y%m").cast(pl.Int64))
```
