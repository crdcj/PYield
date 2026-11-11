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

O ambiente padrão de desenvolvimento usa Python 3.15, definido em
`.python-version`, que deve ser versionado. O pacote mantém Python 3.12 como
versão mínima para instalação, declarada em `requires-python` no `pyproject.toml`.

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

Os testes de integração que precisam acessar a rede usam o marcador `rede`.
Alguns doctests também consultam fontes externas; falhas dessas fontes podem
causar falhas na execução completa. Para executar os testes locais sem esses
downloads nem os doctests da biblioteca:

```sh
uv run pytest tests -m "not rede"
```

Para executar apenas as integrações externas:

```sh
uv run pytest tests -m rede
```

## Documentação

O Zensical fica sem restrição de versão nas dependências de desenvolvimento.
Para atualizar a versão resolvida no lockfile e sincronizar o ambiente:

```sh
uv sync --upgrade-package zensical
```

Após atualizar, execute o build estrito abaixo para verificar a compatibilidade
da configuração e das páginas.

Para visualizar a documentação localmente com atualização automática:

```sh
uv run zensical serve
```

Para gerar e validar os arquivos em `site/`, tratando avisos como erros:

```sh
uv run zensical build --clean --strict
```

O build também gera `site/llms.txt`, com o índice da documentação, versões
Markdown das páginas e `site/llms-full.txt`, com o conteúdo reunido em um único
arquivo. As páginas oferecem o botão **Copy as Markdown**, inclusive na
referência da API gerada a partir das docstrings. Esses recursos usam o suporte
nativo do Zensical, sem plugins adicionais instalados.

Para publicar a documentação no GitHub Pages, após o build estrito, use:

```sh
uv tool run ghp-import -n -p site
```

Esse comando publica o conteúdo de `site/` na branch `gh-pages` do remoto
`origin`. No GitHub, configure **Settings → Pages → Build and deployment** com
**Source: Deploy from a branch**, branch `gh-pages` e pasta `/ (root)`.

## Notas de versão

Registre as mudanças de cada versão em `docs/release-X.Y.Z.md`, incluindo o
comportamento resultante, o racional das decisões e os exemplos de migração
quando houver incompatibilidades. Atualize o índice `docs/releases.md` e a
navegação em `mkdocs.yml`. O README mantém apenas o link permanente para as notas,
sem acumular resumos de versões.

A Release do GitHub é criada a partir dessa página, que é a fonte canônica.
Links relativos para outras páginas da documentação são convertidos em URLs
absolutas do site na publicação.
Notas de correções pequenas podem ter apenas um parágrafo; mudanças incompatíveis
devem explicar o impacto e mostrar a migração.

## Publicar uma versão

A publicação é feita pelo workflow `.github/workflows/release.yml`, disparado
pelo push de uma tag `vX.Y.Z`. Localmente, basta preparar a versão:

1. Atualize a versão seguindo o incremento semântico (`patch`, `minor` ou
   `major`) e confira o resultado:

    ```sh
    uv version --bump patch
    uv version
    ```

2. Escreva as notas em `docs/release-X.Y.Z.md` e atualize `docs/releases.md` e
   `mkdocs.yml`, conforme a seção anterior.
3. Faça o commit, crie a tag e envie os dois:

    ```sh
    git commit -am "release: X.Y.Z"
    git tag vX.Y.Z
    git push --follow-tags
    ```

O workflow executa, em ordem:

1. **verificar**: confere se a tag corresponde à versão do `pyproject.toml` e
   se as notas existem; executa formatação, lint, tipos, testes locais
   (`tests -m "not rede"`, sem doctests) e o build estrito da documentação;
2. **pypi**: gera os artefatos com `uv build --no-sources --clear` e publica
   no PyPI por Trusted Publishing, sem token;
3. **release-github**: cria a Release `PYield X.Y.Z` com o conteúdo de
   `docs/release-X.Y.Z.md`, convertendo links relativos em URLs absolutas do
   site;
4. **docs**: gera a documentação e publica no GitHub Pages.

Se uma etapa falhar, as seguintes não rodam. Corrija a causa e use **Re-run
failed jobs** na execução do workflow. Não recrie a tag depois que a versão
chegou ao PyPI: o PyPI não aceita reenviar a mesma versão. Nesse caso, publique
uma nova versão de correção.

Acompanhe a execução na aba **Actions** do repositório ou com:

```sh
gh run watch
```

### Configuração única

O Trusted Publishing exige cadastrar o workflow no PyPI. Em
[pypi.org/manage/project/pyield/settings/publishing](https://pypi.org/manage/project/pyield/settings/publishing/),
adicione um publisher do GitHub com:

- Owner: `crdcj`;
- Repository name: `PYield`;
- Workflow name: `release.yml`;
- Environment name: `pypi`.

O ambiente `pypi` é criado pelo GitHub na primeira execução. Em **Settings →
Environments → pypi**, é possível exigir aprovação manual antes do envio ao
PyPI.

### Publicação manual

Use apenas se o workflow estiver indisponível. Gere os artefatos e confira a
versão:

```sh
uv build --no-sources --clear
ls dist
```

Publique com o token mantido no arquivo `.env`, que já é ignorado pelo Git:

```dotenv
UV_PUBLISH_TOKEN=pypi-...
```

```sh
uv run --env-file .env uv publish --dry-run
uv run --env-file .env uv publish
```

Não inclua tokens no repositório nem em comandos registrados no histórico do
shell. Depois, crie a Release no GitHub com as notas da versão e publique a
documentação, como descrito em [Documentação](#documentacao).

Confira o resultado na página do [PYield no PyPI](https://pypi.org/project/pyield/).
