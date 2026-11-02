# Novidades na versão 0.62.0

Esta versão remove o cache automático das consultas às fontes externas, o
parâmetro `atualizar` e as retentativas automáticas de downloads. O cache diário
dos históricos em Parquet de TPF e futuros permanece. A mudança é incompatível
com chamadas que ainda passam `atualizar`, mesmo quando o valor é `False`.

O Polars 2.0 passa a ser a versão mínima exigida. Ambientes com Polars 1.x
precisam atualizar essa dependência para instalar esta versão do PYield.

## Por que a política mudou

O cache das consultas externas foi criado para reduzir requisições repetidas e
proteger as APIs das fontes. Entretanto, ele podia devolver uma resposta anterior
quando o consumidor precisava consultar os dados disponíveis naquele momento.
No VNA, isso levou à criação de `atualizar` para ignorar o cache. O consumidor
precisava conhecer os prazos de expiração e quais downloads esse parâmetro
renovava; os índices de inflação usados no mesmo fluxo tinham caches separados.

A política de reutilização de respostas passa a ser responsabilidade de quem
consome a biblioteca. A PYield busca e processa os dados; a aplicação decide se
quer armazená-los, por quanto tempo e quando consultá-los novamente. Isso elimina
os prazos implícitos e a necessidade de um parâmetro para ignorá-los.

## Comportamento das consultas

Os downloads de BCB, ANBIMA, B3, IBGE, Tesouro Nacional e FGV deixam de manter
cache entre chamadas. Isso abrange os indicadores, calendários, VNA, boletins,
dados intradia, leilões, estoque, benchmarks, dealers, mercado secundário e RMD.
Funções que dependem dessas consultas também passam a receber os dados dos novos
downloads. Os timeouts permanecem; o retry automático foi removido.

Chamadas repetidas podem fazer mais requisições e levar mais tempo. Se sua
aplicação consulta o mesmo dado em um laço, obtenha-o uma vez e reutilize o
resultado, ou aplique sua própria política de cache considerando os limites da
fonte.

Um novo download pode retornar os mesmos dados. A atualização depende da
publicação pela fonte, incluindo eventuais atrasos. Datas de referência e
horários informados nos dados mantêm seu significado; não representam
necessariamente o momento da consulta.

## Falhas de rede e retentativas

A PYield passa a fazer uma tentativa por download. Falhas de conexão, timeout e
respostas HTTP de erro, incluindo 429 e 5xx, encerram essa tentativa sem espera
ou repetição automática. Isso também se aplica aos históricos em Parquet do
projeto e ao FTP da taxa DI over. Os contratos de ausência de dados, como 404
no SGS e arquivo ausente no FTP, continuam válidos.

Antes, o retry padrão fazia até três tentativas, com esperas crescentes entre
elas. Um download com timeout de 60 segundos podia acumular cerca de três
minutos só nas tentativas. Além disso, uma falha na paginação podia repetir
páginas já obtidas. A biblioteca agora entrega a falha à aplicação, que decide
se deve repetir, usar outro dado ou continuar seu processamento.

Uma falha isolada não permite distinguir com certeza um soluço de uma
indisponibilidade prolongada. Retentativas podem ajudar no primeiro caso e
amplificar a carga e a espera no segundo. A decisão de removê-las da biblioteca
concentra essa política na aplicação, onde é possível considerar o prazo total
da operação e a condição da fonte. Essa escolha segue a orientação de evitar
retries sobrepostos em várias camadas da
[Amazon Builders' Library](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/).

Consultas longas de Selic Meta passam a buscar os blocos sequencialmente, como
as demais séries SGS. A primeira falha impede o envio dos blocos seguintes.
Consultas bem-sucedidas com vários blocos podem levar mais tempo que antes,
pois deixam de usar downloads paralelos. Paginação, anexos e consultas a fontes
alternativas previstas no fluxo continuam podendo exigir downloads distintos.

Os tipos das exceções existentes são preservados. Por exemplo,
o consumidor pode tratar uma falha do SGS sem reimplementar a consulta:

```python
import requests
import pyield as yd

try:
    dados = yd.selic.meta_serie(ultimos=1)
except requests.RequestException as erro:
    registrar_falha(erro)  # tratamento definido pela aplicação
else:
    processar(dados)
```

Quem precisar de retentativas deve limitar o número de tentativas e o tempo
total, considerar `Retry-After` e usar espera com jitter para falhas que façam
sentido repetir. Evite repetir erros de entrada ou certificado. Quando a fonte
já estiver reconhecidamente indisponível, a aplicação pode suspender novas
consultas até a próxima verificação de disponibilidade.

Remover o retry elimina a multiplicação das esperas, mas não torna uma consulta
instantânea quando a fonte não responde. Os timeouts existentes variam por
endpoint. No Requests, eles limitam a conexão e a inatividade de leitura, e não
o tempo total da chamada; DNS, múltiplos endereços, redirects e transferências
que continuam recebendo bytes podem ampliar a duração. Não há garantia de um
prazo total rígido. Veja a
[documentação de timeouts do Requests](https://requests.readthedocs.io/en/latest/user/advanced/#timeouts).

## Migração: remova `atualizar`

O parâmetro foi removido das seguintes funções:

| Namespace | Funções |
|---|---|
| `yd.vna` | `valor`, `historico`, `ultimo`, `contexto` |
| `yd.ipca` | `calendario` |
| `yd.igpm` | `calendario` |

Antes:

```python
valor = yd.vna.valor("LFT", "31-05-2024", atualizar=True)
historico = yd.vna.historico("NTN-C", vencimento="01-01-2031", atualizar=True)
ultimo = yd.vna.ultimo("NTN-B", atualizar=False)
contexto = yd.vna.contexto("NTN-B", "22-10-2026", atualizar=True)
calendario_ipca = yd.ipca.calendario(2026, atualizar=True)
calendario_igpm = yd.igpm.calendario(2026, atualizar=False)
```

Agora:

```python
valor = yd.vna.valor("LFT", "31-05-2024")
historico = yd.vna.historico("NTN-C", vencimento="01-01-2031")
ultimo = yd.vna.ultimo("NTN-B")
contexto = yd.vna.contexto("NTN-B", "22-10-2026")
calendario_ipca = yd.ipca.calendario(2026)
calendario_igpm = yd.igpm.calendario(2026)
```

Não há parâmetro substituto. Passar `atualizar` gera `TypeError`.

## Históricos em Parquet

O mecanismo de carregamento dos datasets `anbima_tpf.parquet` e
`b3_futures.parquet` continua com cache em memória por processo. A chave muda
quando vira a data em `America/Sao_Paulo`; a primeira consulta do novo dia carrega
o dataset novamente. Essa renovação é por dia de calendário, não por 24 horas
contadas desde o download.

Esses datasets são publicados e mantidos pelo projeto, com um ciclo de
distribuição próprio. O mecanismo permanece em `yd.tpf.taxas_historicas`, nas
consultas de taxas que usam esse histórico, em `yd.futuro.historico` e nas
consultas de DI1 que usam o histórico de futuros. `yd.tpf.taxas` continua buscando
diretamente na ANBIMA quando a data não está disponível no dataset; essa consulta
direta não mantém cache.

A migração não altera fórmulas, unidades ou schemas de retorno. As notas da
[0.60.0](https://crdcj.github.io/PYield/release-0.60.0/) continuam disponíveis para migrações anteriores.


## Processo de publicação

A publicação passa a contar com um workflow acionado por tags `vX.Y.Z`. Após
validar a versão, as notas, os testes e os builds, o fluxo publica os pacotes no
PyPI, a documentação no Pages e a Release no GitHub com estas mesmas notas.
A escolha da versão e a redação das notas permanecem manuais. A ativação exige
a configuração inicial de Trusted Publishing e GitHub Pages descrita no
[guia de desenvolvimento e publicação](https://crdcj.github.io/PYield/desenvolvimento/).
