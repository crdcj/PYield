# AGENTS.md

Instruções operacionais para agentes de IA neste repositório. Este arquivo não é
documentação do usuário; ele existe para melhorar decisões de implementação,
revisão e manutenção.

## Prioridade Máxima

Reduza complexidade. Toda alteração deve ficar menor, mais coesa ou mais clara do
que a situação anterior. Só aumente a complexidade quando isso for necessário para
preservar contrato público, corrigir ambiguidade real ou evitar empurrar
complexidade para o usuário.

Antes de editar:
- entenda o padrão local antes de criar um novo;
- explicite incertezas quando elas afetarem API pública ou semântica de dados;
- prefira mudanças pequenas, verificáveis e diretamente ligadas ao pedido;
- não refatore código adjacente por gosto;
- não reverta mudanças existentes que você não fez.

## Comandos Essenciais

Use `uv run` para comandos de projeto:
- `uv run pytest ...` para testes;
- `uv run ruff check ...` para lint;
- `uv run pyright ...` para tipos;
- `uv run mkdocs build --strict` para documentação.

Rode verificações focadas no que você alterou. Amplie para a suíte inteira quando
a mudança afetar contratos transversais, API pública ou parsing compartilhado.

## Fronteira Pública

Considere público o que é exportado em `pyield/__init__.py`, documentado em
MkDocs ou exposto por namespace público de objeto. Caminhos importáveis que não
fazem parte dessa fronteira são implementação, mesmo que tecnicamente acessíveis.

A API pública deve ser orientada ao objeto financeiro ou conceito usado pelo
usuário, não à fonte de dados. A fonte continua importante internamente e deve
aparecer na docstring pública, mas não deve forçar ergonomia ruim.

Padrão atual:
- namespaces de objeto para famílias coesas: `yd.futuro.*`, `yd.tpf.*`,
  `yd.di1.*`, `yd.lft.*`, `yd.ipca.*`, `yd.du.*`;
- indicadores simples e muito diretos podem ficar na raiz: `yd.ptax`,
  `yd.ptax_serie`, `yd.selic_over`, `yd.selic_over_serie`,
  `yd.selic_meta`, `yd.selic_meta_serie`, `yd.di_over`;
- namespaces de fonte (`b3`, `bc`, `anbima`, `tn`) devem concentrar APIs
  técnicas, específicas da fonte ou infraestrutura interna. Evite duplicar neles
  aliases públicos que já foram migrados para objeto;
- use `intradia` como termo público padrão, não `intradiario`;
- mantenha testes de API pública enquanto a migração estiver em andamento. Eles
  podem proteger ausências temporárias de aliases legados, mas devem ser
  simplificados depois da migração estabilizar.

Ao migrar API pública:
- atualize exports no namespace de objeto e na raiz quando aplicável;
- remova aliases duplicados dos namespaces de fonte quando essa for a decisão de
  arquitetura;
- mova a docstring canônica para a chamada pública;
- deixe wrappers internos ou de fonte com docstrings curtas apontando para a API
  pública, se eles continuarem existindo;
- atualize docs, README de migração e testes de fronteira pública.

## Nomenclatura

API pública, parâmetros públicos e colunas retornadas devem estar em português,
em `snake_case`, salvo termos técnicos realmente consolidados no domínio.

Use nomes canônicos da biblioteca na camada pública. Nomes da fonte são aceitáveis
na camada bruta/intermediária quando o código opera diretamente no payload da
fonte.

Colunas retornadas por funções públicas devem ser em português e estáveis. Se a
fonte alterna semântica entre taxa e preço, modele isso explicitamente para o
usuário; não esconda a diferença em uma coluna genérica que transfere a
interpretação para quem consome a biblioteca.

## Docstrings

Docstrings devem estar em português e respeitar `line-length = 88`.

Docstring pública é contrato de API e documentação do usuário. Trate como dado
valioso: nunca apague, resuma, mova ou substitua uma docstring pública sem
preservar integralmente o conteúdo original e confirmar que o novo destino é o
correto. Em migrações, primeiro copie a docstring canônica para o destino
pretendido; só depois considere encurtar a origem.

Funções públicas usam estilo Google com as seções relevantes:
- `Args:`
- `Returns:`
- `Output Columns:`
- `Notes:`
- `Examples:`

Para funções públicas que retornam DataFrame, liste as colunas em
`Output Columns:` com tipo Polars e descrição. Inclua a fonte dos dados na
docstring pública quando a função buscar ou representar dado externo.

Evite docstrings públicas duplicadas em vários caminhos para o mesmo dado. A
docstring completa deve ficar no namespace canônico; wrappers devem ser breves.
Se houver dúvida sobre qual caminho é canônico, pare e pergunte antes de editar.

Doctests devem usar dados reais e são validados pelo `pytest` configurado no
projeto. O namespace de doctest já fornece `yd` e `pl`.

## Dados e ETL

Módulos que buscam dados externos devem seguir o fluxo:
1. `_buscar_*`: fetch bruto, cache/retry quando aplicável, sem parsing;
2. `_parsear_*`: transforma bruto em estrutura inicial, sem conversões de domínio;
3. `_processar_*`: renomeia, tipa, calcula colunas e define ordem final;
4. função pública: orquestra, filtra e ordena.

Não use `try/except Exception` genérico para mascarar erro operacional. Erros
inesperados devem aparecer.

Datas válidas sem dados não são erro. Funções públicas de consulta devem retornar
DataFrame vazio, `None` ou `nan`, conforme o contrato. Reserve `ValueError` para
entrada malformada ou violação clara de domínio.

Use os conversores internos de data quando existirem. Entradas escalares devem
normalizar para `datetime.date`; coleções devem virar `pl.Series` com dtype
`Date`; formatos aceitos incluem `DD-MM-YYYY`, `DD/MM/YYYY` e `YYYY-MM-DD`.

## Polars

Todas as funções públicas tabulares retornam `polars.DataFrame` ou
`polars.Series`.

Em `with_columns`, prefira sintaxe de keyword:
`with_columns(coluna=expr)` em vez de `expr.alias("coluna")`.

Quando o encadeamento for simples, prefira um único `return (...)` com quebras por
método. Evite variáveis intermediárias que só repetem o pipeline.

Use parsers e expressões estruturadas do Polars em vez de manipulação manual de
strings quando houver suporte razoável.

## Testes

Teste a superfície que o usuário chama, não apenas helpers internos. Para ETL com
rede, substitua apenas a camada de fetch com `monkeypatch`, execute o fluxo
público e compare com Parquet de referência quando houver.

Dados brutos de referência devem ser salvos byte a byte como vieram da fonte.
Não normalize encoding, quebras de linha ou separadores em fixtures brutas.

Doctests são suficientes para funções escalares simples e pipelines triviais.
Crie testes dedicados para parsing complexo, múltiplos cenários, ZIP/HTML/CSV
não triviais ou DataFrames com transformações relevantes.

## Estilo de Mudança

Mantenha o diff cirúrgico:
- não reformate arquivos inteiros sem necessidade;
- não altere nomes públicos sem atualizar docs e testes;
- não crie abstração para uso único;
- remova apenas código morto criado pela sua alteração;
- registre no comentário final o que foi verificado e o que não foi possível
  verificar.

Se a árvore Git já estiver suja, trabalhe com as mudanças existentes. Elas podem
ser do usuário ou de outro passo da migração.
