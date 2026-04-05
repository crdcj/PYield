# AGENTS.md

Orientações para agentes de IA que trabalham neste repositório.

## Visão Geral

PYield é uma biblioteca Python para análise de renda fixa brasileira (requer Python ≥ 3.12). Busca e processa dados da ANBIMA, BCB (Banco Central), IBGE e B3 (Bolsa de Valores). Todas as funções públicas retornam DataFrames/Series do Polars.

## Princípio Fundamental: Redução de Complexidade

O objetivo principal ao trabalhar neste repositório é **reduzir a complexidade do código**. Toda alteração deve, por padrão, simplificar — nunca adicionar ramificações, abstrações ou lógica desnecessária. Só aumente a complexidade quando for estritamente necessário e com justificativa clara do solicitante.

## Comandos de Build e Desenvolvimento

```bash
# Instalar dependências (usa o gerenciador de pacotes uv)
uv sync

# Rodar todos os testes (tests/ + doctests em pyield/)
# pyproject.toml já configura testpaths e --doctest-modules
pytest

# Rodar apenas doctests de um módulo
pytest pyield/du/core.py --doctest-modules

# Rodar um arquivo de teste
pytest tests/du/test_dus.py

# Rodar um teste específico
pytest tests/du/test_dus.py::test_count_new_holiday

# Linting
ruff check

# Verificação de tipos
pyright

# Documentação (MkDocs)
mkdocs serve
```

## Arquitetura

### Estrutura de Módulos

A biblioteca é organizada em namespaces de domínio, todos expostos via `pyield/__init__.py`:

- **`du`** — Calendário de dias úteis (feriados brasileiros embutidos). Funções principais: `contar`, `deslocar`, `gerar`, `e_dia_util`, `ultimo_dia_util`. Variantes para expressões Polars: `contar_expr`, `deslocar_expr`, `e_dia_util_expr`.
- **`anbima`** — Dados da ANBIMA (precificação de títulos públicos, curvas de juros). Funções: `tpf`, `tpf_vencimentos`, `tpf_fonte`, `ettj_ultima`, `ettj_intradia`, `ima_ultimo`, `imaq`, `tpf_difusao`.
- **`bc`** — Indicadores do BCB. Funções: `selic_over`, `selic_over_serie`, `selic_meta`, `selic_meta_serie`, `ptax`, `ptax_serie`, `compromissadas`, `vna_lft`, `leiloes`, `tpf_mensal`, `tpf_intradia`. Submódulos: `copom`, `compromissada`.
- **`b3`** — Dados de mercado da B3. Funções: `futuro`, `futuro_enriquecer`, `futuro_intradia`, `futuro_datas_disponiveis`, `di_over`, `boletim_negociacao`, `boletim_negociacao_extrair`, `boletim_negociacao_ler`, `derivativo_intradia`. Submódulo: `di1`.
- **`tn`** — Módulos do Tesouro Nacional. Funções: `benchmarks`, `leilao`, `rmd`, `premio_pre`. Submódulo: `pre`. Módulos individuais (`ltn`, `ntnb`, `ntnf`, `ntnc`, `lft`, `ntnbprinc`, `ntnb1`) são exportados no topo (`pyield`).
- **`ipca`** — Dados de inflação. Funções: `indices`, `indices_ultimos`, `taxas`, `taxas_ultimas`, `taxa_projetada`.
- **`selic`** — Análises relacionadas ao COPOM. Submódulos: `cpm` (dados brutos de Opções Digitais COPOM da B3), `probabilities` (probabilidades implícitas de reuniões do COPOM).

Funções de topo também exportadas em `pyield`:
- `forwards`, `forward` — Cálculos de taxas a termo via `fwd.py`.
- `Interpolador` — Classe de interpolação de taxas via `interpolador.py`.
- `hoje`, `agora` — Data/hora no fuso de Brasília via `relogio.py`.
- `copom_options` — Alias para `selic.cpm.data`.

### Componentes Transversais

- **`_internal/types.py`** — Aliases de tipo `DateLike` e `ArrayLike`; `any_is_empty()` para detecção de nulo/vazio; `any_is_collection()` para detecção de coleções.
- **`_internal/converters.py`** — `converter_datas()` normaliza diversas entradas de data para `datetime.date` ou `pl.Series[Date]`. `converter_datas_expr()` para pipelines de expressão Polars.
- **`interpolador.py`** — Classe `Interpolador` para interpolação de taxas (método linear ou flat_forward, convenção de 252 dias úteis/ano).
- **`_internal/cache.py`** — Decorator `ttl_cache` para cache com TTL diário.
- **`_internal/data_cache.py`** — Cache de parquet hospedado no GitHub com TTL diário usando `lru_cache` (truque de date-key para auto-invalidação).
- **`_internal/br_numbers.py`** — Expressões Polars para converter strings numéricas no padrão brasileiro (vírgula decimal, ponto de milhar): `float_br`, `taxa_br`, `inteiro_br`, `inteiro_m`.
- **`_internal/retry.py`** — Decorator de retry baseado em Tenacity (`retry_padrao`) para requisições de rede (retry em 429, 5xx, timeouts).
- **`relogio.py`** — `hoje()` e `agora()` retornam data/hora no fuso de Brasília (America/Sao_Paulo).

### Convenções de Tratamento de Datas

- Formatos de string aceitos: `DD-MM-YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD`
- Datas escalares normalizam para `datetime.date`; coleções viram `pl.Series` com dtype `Date`
- Parsing de strings é feito elemento a elemento com fallback entre os formatos aceitos
- Strings inválidas são convertidas para `null` (ou `None` para saídas escalares)
- Entradas nulas (`None`, `NaN`, coleções vazias) fazem short-circuit: funções escalares retornam `None` ou `nan`, funções vetorizadas retornam DataFrame/Series vazio

### Padrão de Fluxo de Dados (ETL)

Módulos que buscam dados externos seguem o padrão ETL de 3 funções internas + função pública:

1. **`_buscar_*()`** — Fetch com `@ttl_cache` e `@retry_padrao`. Retorna dados brutos (`str`, `bytes`, `list[dict]`). Não faz parsing nem transformação.
2. **`_parsear_df()`** — Converte dados brutos em DataFrame com `infer_schema=False` (tudo string). Sem rename nem conversão de tipos.
3. **`_processar_df()`** — Rename, conversão de tipos (usando helpers de `br_numbers`) e cálculos derivados. Quando há colunas derivadas que dependem de outras calculadas, usar `with_columns` antes do `select`. O `select` final define a ordem das colunas.
4. **Função pública** — Orquestra: buscar → parsear → processar → filtrar/ordenar. Erros propagam naturalmente (sem `try/except Exception` genérico).

Referência: `anbima/ima.py` (com colunas derivadas) e `anbima/imaq.py` (sem colunas derivadas).

## Convenções de Nomenclatura

- **Fronteira da API pública:** Considere público o que está documentado e/ou exportado no namespace de topo (`pyield/__init__.py`). Módulos não exportados no topo são internos, mesmo que importáveis por caminho direto.
- **API pública (português):** Nomes de funções públicas, parâmetros e classes exportadas devem, por padrão, estar em português.
- **Exceção para termos técnicos consolidados:** Só manter nomes em inglês na API pública quando houver justificativa clara e o termo técnico já estiver consolidado no domínio ou na base de código. Evitar misturar português e inglês sem necessidade.
- **Nomes de colunas em DataFrames (português):** Colunas de DataFrames retornados por funções públicas usam `snake_case` em português (ex.: `data_referencia`, `taxa_indicativa`, `valor`). Módulos antigos ainda usam PascalCase em inglês, mas estão sendo progressivamente migrados.
- **Parâmetros públicos:** Preferir nomes explícitos em português. Abreviações de domínio podem ser usadas quando forem realmente consagradas e melhorarem a leitura sem sacrificar clareza.
- **Código interno (português):** Variáveis locais, constantes de módulo, mensagens de log e mensagens de exceção devem ser em português.
- **Nomes em módulos internos:** Para módulos de uso interno compartilhado, nomes de função podem permanecer sem prefixo `_` quando isso melhora legibilidade. Use prefixo `_` para helpers locais/privados dentro do módulo.
- **Exceção para módulos utilitários base:** Módulos transversais e fundacionais de uso interno (ex.: `_internal/types.py`, `_internal/retry.py`) podem manter identificadores técnicos em inglês (`is_collection`, `retry_state`, etc.) para preservar legibilidade, reduzir churn e evitar renomeações sem ganho funcional.
- **Exceção para módulos internos com classe de serviço:** Se um módulo não é exposto pela API e é usado apenas internamente por outro módulo, é comum manter **um método principal sem `_`** dentro da classe para sinalizar o ponto de entrada interno. Os demais helpers seguem com `_`. Esse método principal deve permanecer em português e o módulo **não deve** ser exportado em `__init__.py`.
- **Exceção para módulos internos com função principal:** Se um módulo não é exposto pela API e é usado apenas internamente por outro módulo, pode manter **uma função principal sem `_`** como ponto de entrada interno. As demais funções seguem com `_`. A função principal deve ser em português e o módulo **não deve** ser exportado em `__init__.py`.

### Convenção de nomes por camada na B3

- **Camada pública enriquecida da lib:** Preferir nomes canônicos da biblioteca, mesmo que a fonte original use outra terminologia. Ex.: usar `contrato` para identificadores-base como `DI1`, `DAP`, `WDO`, e `codigo_negociacao` para o identificador completo retornado ao usuário.
- **Camada bruta/intermediária próxima da fonte:** Quando a função expõe ou filtra diretamente campos do payload original da B3, pode usar a terminologia da fonte para deixar claro que opera no schema bruto. Ex.: em `boletim.py`, parâmetros como `prefixo_ticker` e `comprimento_ticker` são aceitáveis porque o filtro atua diretamente sobre `TckrSymb`.
- **Regra prática:** Evitar misturar, na mesma camada, vocabulário da fonte e vocabulário canônico da lib para o mesmo conceito. A distinção deve refletir o nível de abstração do módulo.

## Convenções de Docstrings

- Todas as docstrings devem ser escritas em **português** (funções públicas e internas).
- A largura de linha das docstrings deve respeitar `line-length = 88` do projeto (configurado em `pyproject.toml`). Isso inclui a indentação — ex.: docstring dentro de função tem 4 espaços de indent, sobrando 84 caracteres.
- Funções públicas usam seções estilo Google: `Args:`, `Returns:`, `Output Columns:`, `Notes:`, `Examples:`.
- `Output Columns:` lista cada coluna com tipo Polars e descrição (ex: `* data_liquidacao (Date): data de liquidação.`).
- Doctests (seção `Examples:`) usam dados reais e são validados por `pytest --doctest-modules`.
- Renderização Markdown (MkDocs/mkdocstrings): em listas livres dentro do texto (ex.: `Onde:`), evitar linha em branco entre o título e os itens e iniciar os itens imediatamente abaixo.
  Exemplo recomendado:
  `Onde:`
  `- item 1`
  `- item 2`
- Após mudanças em docstrings com listas/fórmulas, validar visualmente com `mkdocs serve` e confirmar o HTML gerado (evitar renderização como bloco de código).

## Convenções Polars

- Em `with_columns`, preferir sintaxe de keyword `col=expr` em vez de `expr.alias("col")`.
- Retornos com encadeamento Polars: usar um único `return (...)` com quebras de linha entre métodos, sem variável intermediária.

## Convenções de Logging

- Não usar `warning` para validações de entrada esperadas (ex.: `None`, vazio, combinação inválida já prevista pelo contrato).
- Nesses casos, retornar o valor de contrato (`None`, `NaN`, `Series/DataFrame` vazio) ou lançar `ValueError` quando apropriado.
- Reservar `warning`/`error` para anomalias operacionais reais (falha de rede, fonte indisponível, schema inesperado, erro de parsing fora do contrato, etc.).

## Convenções de Retorno de Consultas

- Funções públicas que consultam dados externos retornam **DataFrame vazio** (ou `nan`/`None` para escalares) quando não há dados para os parâmetros fornecidos — independentemente do motivo (data futura, fim de semana, feriado, fonte indisponível).
- O chamador testa com `.is_empty()` (DataFrame) ou `math.isnan()` (escalar).
- **Não** lançar `ValueError` para datas válidas sem dados. `ValueError` é reservado para inputs malformados (tipo errado, formato inválido, violação de domínio).
- Analogia: funciona como um `SELECT` que retorna 0 linhas — não é um erro.

## Testes

Testes ficam em `tests/` e doctests estão embutidos nas docstrings. Execute `pytest` para rodar ambos (configurado em `pyproject.toml` via `testpaths` e `addopts = "--doctest-modules"`).

### Configuração de Doctests (conftest.py)

O `conftest.py` da raiz configura o ambiente de doctests:
- **Injeção de namespace:** `yd` (pyield) e `pl` (polars) ficam disponíveis em todos os doctests via fixture `doctest_namespace`.
- **Display do Polars:** `pl.Config.set_tbl_width_chars(150)` garante saída de tabela consistente entre ambientes.
- **Flags de opção:** `ELLIPSIS` e `NORMALIZE_WHITESPACE` habilitados globalmente.

### Padrão de Testes para Módulos que Buscam Dados

Módulos que buscam dados externos (ex.: `bc/compromissada.py`, `bc/tpf_mensal.py`) usam dados de referência locais para testar sem acesso à rede:

1. **Dados de referência** — Um par de arquivos em `tests/<module>/data/`: o dado bruto (CSV, HTML, ZIP) **exatamente como retornado pela fonte** e o resultado esperado (Parquet). O arquivo bruto deve ser salvo byte-a-byte (`write_bytes(resp.content)`) sem normalização de encoding ou line endings.
2. **Teste de pipeline** — Processa o dado bruto local pelas funções internas de processamento e compara com `result.equals(expected_parquet)`.
3. **Teste da função pública** — Substitui (patch) a função de fetch de rede para retornar os dados brutos locais, chama a função pública e compara com o Parquet de referência.

#### Padrão recomendado para ETL mais complexos (HTML/CSV/JSON)

Para pipelines ETL com múltiplas etapas e dependência de rede:

1. **Substituir apenas a camada de rede** — Use `pytest` com `monkeypatch` para sobrescrever a função interna de fetch (ex.: `_obter_csv`, `_buscar_conteudo_url`) e retornar o arquivo bruto local.
2. **Executar o fluxo público completo** — Chame a função pública com a opção de buscar na fonte (`fetch_from_source=True`) para exercitar o mesmo pipeline real.
3. **Comparar com Parquet de referência** — Valide o resultado final com `DataFrame.equals`, sem repetir etapas do pipeline no teste.

Isso reduz complexidade do teste e mantém a cobertura do fluxo real sem rede.

### Quando doctests são suficientes

Módulos com pipeline trivial que retornam valores escalares simples (float, str) sem transformações complexas de DataFrame. Os doctests já validam o comportamento real e servem como documentação. Exemplos: `di_over.py`, `tn/ltn.py`, `fwd.py`, `interpolador.py`.

### Quando criar test files separados

Módulos com pipelines ETL multi-etapa, transformações complexas de DataFrame (5+ colunas), processamento de arquivos binários (ZIP), ou quando é necessário testar múltiplos cenários de edge case que não cabem em doctests.
