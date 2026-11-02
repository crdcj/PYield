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

O workflow `.github/workflows/testes.yml` executa a mesma suíte em Python 3.12,
3.13, 3.14 e 3.15 em pull requests e pushes na branch `main`. Cada versão usa
um ambiente independente e prevalece sobre o padrão de `.python-version`.
Os testes de integração precisam acessar a rede; falhas de fontes externas
também podem causar falhas no CI. O workflow de release reutiliza essa matriz
e exige que os testes passem nas quatro versões antes de gerar e publicar os
artefatos.
Lint, tipos e documentação são verificados somente em Python 3.15.
A documentação também passa pelo build estrito em pull requests e pushes na
branch `main`. Em tags de release, o build fica na etapa que gera os artefatos
para publicação, sem repeti-lo na matriz de testes.

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

A documentação é publicada pelo workflow de release usando os artefatos de
`site/`. Configure o GitHub Pages com **Source: GitHub Actions**, conforme a seção
[Configuração inicial](#configuracao-inicial).

## Notas de versão

Registre as mudanças de cada versão em `docs/release-X.Y.Z.md`, incluindo o
comportamento resultante, o racional das decisões e os exemplos de migração
quando houver incompatibilidades. Atualize o índice `docs/releases.md` e a
navegação em `mkdocs.yml`. O README mantém apenas o link permanente para as notas,
sem acumular resumos de versões.

O workflow usa o conteúdo dessa página como corpo da Release no GitHub,
associada à tag `vX.Y.Z`. A página da documentação é a fonte canônica. Use URLs
absolutas para links nas notas, pois o mesmo texto será exibido no GitHub.
Notas de correções pequenas podem ter apenas um parágrafo; mudanças incompatíveis
devem explicar o impacto e mostrar a migração.

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
uv build --no-sources --clear
```

O comando cria a distribuição fonte e a wheel no diretório `dist/`.
`--no-sources` ignora fontes alternativas de dependências configuradas em
`tool.uv.sources`, validando o build com os metadados usados na publicação.
`--clear` limpa o diretório de saída antes do build para evitar a publicação de
artefatos de versões anteriores. Antes de publicar, confirme o conteúdo dos
artefatos e a versão:

```sh
ls dist
```

## Publicação automática por tag

O workflow `.github/workflows/release.yml` é acionado pelo envio de uma tag
`vX.Y.Z`. Merges e pushes de branches não publicam versões. São aceitas apenas
versões estáveis com três componentes numéricos.

O fluxo começa executando os testes e doctests em Python 3.12, 3.13, 3.14 e 3.15
no commit da tag, reutilizando `.github/workflows/testes.yml`. Se qualquer versão
falhar, a publicação é bloqueada. Não é necessário consultar manualmente o
resultado dos testes da branch `main` para liberar a publicação.

Após o sucesso da matriz, o fluxo valida a correspondência entre tag e
`pyproject.toml` e exige notas não vazias em `docs/release-X.Y.Z.md`. Depois
executa lint e tipos e gera a documentação estrita e as distribuições em Python
3.15. A suíte de testes não é repetida nessa etapa. Os testes de integração
precisam acessar a rede; falhas de fontes externas também interrompem a
validação. O lockfile continua local e ignorado pelo Git; o CI resolve as
dependências a partir do `pyproject.toml`, como uma instalação nova.

Após a validação, os jobs publicam, nesta ordem:

1. Wheel e distribuição fonte no PyPI, com Trusted Publishing.
2. Documentação no GitHub Pages.
3. GitHub Release com as notas canônicas e os mesmos artefatos do PyPI.

### Configuração inicial

Faça esta configuração uma vez, antes de enviar a primeira tag:

- No GitHub, habilite Actions no repositório e crie o environment `pypi` em
  **Settings → Environments**. Configure sua política de deployment para aceitar
  as tags `v*`.
- No PyPI, em **Manage project → Publishing**, adicione um Trusted Publisher
  para o projeto `pyield`: proprietário `crdcj`, repositório `PYield`, workflow
  `release.yml` e environment `pypi`. Não é necessário cadastrar um token PyPI
  nos secrets do GitHub.
- No GitHub, em **Settings → Pages → Build and deployment**, selecione
  **Source: GitHub Actions**. No environment `github-pages`, permita deployments
  das tags `v*`; uma regra restrita à branch principal bloquearia o workflow.

A configuração usa o [Trusted Publishing do PyPI](https://docs.pypi.org/trusted-publishers/adding-a-publisher/)
e o [deployment do GitHub Pages por Actions](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages).
Os environments podem exigir revisão se essa for a política desejada; sem essa
regra, o envio da tag é a decisão de publicar.

### Preparar e enviar uma release

Atualize a versão com `uv version`, escreva as notas e atualize seu índice e
navegação. Revise e faça commit das alterações na branch principal. Para a versão
`0.62.0`, envie a branch e a tag apontando para esse commit:

```sh
git push origin main
git tag -a v0.62.0 -m "Release 0.62.0"
git push origin v0.62.0
```

O commit marcado deve incluir o workflow. Acompanhe a execução na aba **Actions**
do GitHub. Envie uma versão por vez e aguarde a conclusão antes da próxima.

### Falhas e reexecução

Se a validação falhar, nenhum job de publicação será executado. As publicações
externas não são uma transação: se o PyPI concluir e o Pages falhar, o pacote já
estará disponível. Corrigida uma configuração externa, use **Re-run failed jobs**
no GitHub para continuar sem repetir os jobs concluídos.

O upload ao PyPI usa `--check-url` para reconhecer arquivos idênticos já
publicados em uma tentativa anterior. Isso não permite substituir arquivos
existentes. Não mova uma tag que já publicou o pacote; se a correção exigir mudar
o conteúdo, prepare uma nova versão com uma nova tag.

## Publicar manualmente no PyPI

Este é o procedimento alternativo à automação, para uma publicação local.

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

O workflow de release usa Trusted Publishing, sem esse token. Não inclua tokens
no repositório nem em comandos registrados no histórico do shell.

O nome e a versão do pacote publicados precisam ser novos no índice. Depois do
upload, confira a página do [PYield no PyPI](https://pypi.org/project/pyield/).
