---
title: "Desenvolvimento e publicação"
description: "Como desenvolver, verificar, gerar a documentação e publicar o PYield"
---

# Desenvolvimento e publicação

Este guia reúne os comandos usados para trabalhar no repositório, gerar a
documentação e publicar uma nova versão do pacote.

Execute os comandos a partir da raiz do projeto. O ambiente e as dependências
de desenvolvimento são definidos em `pyproject.toml` e gerenciados pelo `uv`.

## Preparar o ambiente

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

Os testes incluem os doctests configurados no projeto. Para uma alteração
localizada, é possível passar um caminho ou uma expressão ao `pytest`.

## Documentação

Para visualizar a documentação localmente com atualização automática:

```sh
uv run zensical serve
```

Para gerar e validar os arquivos em `site/`, tratando avisos como erros:

```sh
uv run zensical build --clean --strict
```

O procedimento atual de publicação no GitHub Pages usa `ghp-import`:

```sh
uv tool run ghp-import -n -p site
```

Esse comando publica o conteúdo de `site/` na branch `gh-pages` do remoto
`origin`. No GitHub, configure **Settings → Pages → Build and deployment** com
**Source: Deploy from a branch**, branch `gh-pages` e pasta `/ (root)`.

## Atualizar a versão

Confira a versão atual sem alterar arquivos:

```sh
uv version
```

Para atualizar a versão seguindo o próximo incremento semântico, use, por
exemplo:

```sh
uv version --bump patch
```

Também estão disponíveis `major`, `minor`, `alpha`, `beta`, `rc`, `post` e
`dev`. Revise o diff e os arquivos gerados pelo lockfile antes de continuar.

## Gerar os artefatos

O projeto usa `uv_build` como backend PEP 517. Gere a distribuição a partir da
raiz:

```sh
uv build
```

O comando cria a distribuição fonte e a wheel no diretório `dist/`. Antes de
publicar, confirme o conteúdo dos artefatos e a versão:

```sh
ls dist
```

## Publicar no PyPI

Depois de atualizar a versão, executar as verificações e gerar os artefatos,
publique o conteúdo de `dist/`. Para publicação local, mantenha o token no
arquivo `.env` (esse arquivo já é ignorado pelo Git):

```dotenv
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

Em ambientes de CI, configure `UV_PUBLISH_TOKEN` como secret e execute
diretamente `uv publish`. Não inclua tokens no repositório nem em comandos
registrados no histórico do shell.

O nome e a versão do pacote publicados precisam ser novos no índice. Depois do
upload, confira a página do [PYield no PyPI](https://pypi.org/project/pyield/).
