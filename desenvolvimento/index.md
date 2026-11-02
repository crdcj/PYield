# Desenvolvimento e publicação

Este guia reúne os comandos usados para trabalhar no repositório, gerar a documentação e publicar uma nova versão do pacote.

Execute os comandos a partir da raiz do projeto. O ambiente e as dependências de desenvolvimento são definidos em `pyproject.toml` e gerenciados pelo `uv`.

## Preparar o ambiente

O ambiente padrão de desenvolvimento usa Python 3.15, definido em `.python-version`, que deve ser versionado. O pacote mantém Python 3.12 como versão mínima para instalação, declarada em `requires-python` no `pyproject.toml`.

Sincronize o ambiente virtual com as dependências do projeto:

```sh
uv sync
```

## Verificações

Execute os testes, o lint e a checagem de tipos antes de abrir uma alteração:

```sh
uv run pytest
uv run ruff check .
uv run ty check
```

Os testes incluem os doctests configurados no projeto. Para uma alteração localizada, é possível passar um caminho ou uma expressão ao `pytest`.

Os testes de integração que precisam acessar a rede usam o marcador `rede`. Alguns doctests também consultam fontes externas; falhas dessas fontes podem causar falhas na execução completa. Para executar os testes locais sem esses downloads nem os doctests da biblioteca:

```sh
uv run pytest tests -m "not rede"
```

Para executar apenas as integrações externas:

```sh
uv run pytest tests -m rede
```

## Documentação

O Zensical fica sem restrição de versão nas dependências de desenvolvimento. Para atualizar a versão resolvida no lockfile e sincronizar o ambiente:

```sh
uv sync --upgrade-package zensical
```

Após atualizar, execute o build estrito abaixo para verificar a compatibilidade da configuração e das páginas.

Para visualizar a documentação localmente com atualização automática:

```sh
uv run zensical serve
```

Para gerar e validar os arquivos em `site/`, tratando avisos como erros:

```sh
uv run zensical build --clean --strict
```

O build também gera `site/llms.txt`, com o índice da documentação, versões Markdown das páginas e `site/llms-full.txt`, com o conteúdo reunido em um único arquivo. As páginas oferecem o botão **Copy as Markdown**, inclusive na referência da API gerada a partir das docstrings. Esses recursos usam o suporte nativo do Zensical, sem plugins adicionais instalados.

Para publicar a documentação no GitHub Pages, após o build estrito, use:

```sh
uv tool run ghp-import -n -p site
```

Esse comando publica o conteúdo de `site/` na branch `gh-pages` do remoto `origin`. No GitHub, configure **Settings → Pages → Build and deployment** com **Source: Deploy from a branch**, branch `gh-pages` e pasta `/ (root)`.

## Notas de versão

Registre as mudanças de cada versão em `docs/release-X.Y.Z.md`, incluindo o comportamento resultante, o racional das decisões e os exemplos de migração quando houver incompatibilidades. Atualize o índice `docs/releases.md` e a navegação em `mkdocs.yml`. O README mantém apenas o link permanente para as notas, sem acumular resumos de versões.

Ao criar uma Release no GitHub, copie o conteúdo dessa página para as notas. A página da documentação é a fonte canônica. Use URLs absolutas para links nas notas, pois o mesmo texto será exibido no GitHub. Notas de correções pequenas podem ter apenas um parágrafo; mudanças incompatíveis devem explicar o impacto e mostrar a migração.

## Atualizar a versão

Confira a versão atual sem alterar arquivos:

```sh
uv version
```

Para atualizar a versão seguindo o próximo incremento semântico, use, por exemplo:

```sh
uv version --bump patch
```

Também estão disponíveis `major`, `minor`, `alpha`, `beta`, `rc`, `post` e `dev`. Revise o diff e os arquivos gerados pelo lockfile antes de continuar.

## Gerar os artefatos

O projeto usa `uv_build` como backend PEP 517. Gere a distribuição a partir da raiz:

```sh
uv build --no-sources --clear
```

O comando cria a distribuição fonte e a wheel no diretório `dist/`. `--no-sources` ignora fontes alternativas de dependências configuradas em `tool.uv.sources`, validando o build com os metadados usados na publicação. `--clear` limpa o diretório de saída antes do build para evitar a publicação de artefatos de versões anteriores. Antes de publicar, confirme o conteúdo dos artefatos e a versão:

```sh
ls dist
```

## Publicar no PyPI

Depois de atualizar a versão, executar as verificações e gerar os artefatos, publique o conteúdo de `dist/`. Para publicação local, mantenha o token no arquivo `.env` (esse arquivo já é ignorado pelo Git):

```text
UV_PUBLISH_TOKEN=pypi-...
```

Carregue o arquivo explicitamente ao publicar:

```sh
uv run --env-file .env uv publish
```

Para validar o fluxo sem enviar os arquivos, use:

```sh
uv run --env-file .env uv publish --dry-run
```

Não inclua tokens no repositório nem em comandos registrados no histórico do shell.

O nome e a versão do pacote publicados precisam ser novos no índice. Depois do upload, confira a página do [PYield no PyPI](<https://pypi.org/project/pyield/>).
