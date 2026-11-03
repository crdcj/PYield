# Novidades na versão 0.62.0

> Estas notas descrevem a 0.62.0. Os ajustes posteriores de escalas decimais,
> a série de DI Over, os índices no contexto de VNA e o corte histórico
> do IGP-M estão nas [notas da 0.63.0](https://crdcj.github.io/PYield/release-0.63.0/).

Esta versão remove o cache automático das consultas às fontes externas e o
parâmetro `atualizar`. Os downloads passam a usar uma política compartilhada de
retentativa leve, com no máximo duas tentativas para falhas transitórias.
O cache diário dos históricos em Parquet de TPF e futuros permanece. A mudança
é incompatível com chamadas que ainda passam `atualizar`, mesmo quando o valor
é `False`.

O Polars 2.0 passa a ser a versão mínima exigida. Ambientes com Polars 1.x
precisam atualizar essa dependência para instalar esta versão do PYield.

Selic Over, Selic Meta, DI Over e PTAX passam a retornar `Decimal`, preservando
os valores publicados sem passagem por `float`. As taxas Over também ganham
a opção diária com `anualizada=False`, e DI Over passa do FTP da CETIP para
a API HTTPS do Banco Central. Os novos tipos de retorno exigem atenção na
migração, conforme as seções abaixo.

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
downloads. Os timeouts permanecem; o retry segue a política leve abaixo.

Chamadas repetidas podem fazer mais requisições e levar mais tempo. Se sua
aplicação consulta o mesmo dado em um laço, obtenha-o uma vez e reutilize o
resultado, ou aplique sua própria política de cache considerando os limites da
fonte.

Um novo download pode retornar os mesmos dados. A atualização depende da
publicação pela fonte, incluindo eventuais atrasos. Datas de referência e
horários informados nos dados mantêm seu significado; não representam
necessariamente o momento da consulta.

## Indicadores SGS com Decimal

As consultas `yd.selic.over`, `yd.selic.meta`, `yd.di_over` e `yd.ptax` passam
a retornar `decimal.Decimal`. As séries `yd.selic.over_serie`,
`yd.selic.meta_serie` e `yd.ptax_serie` passam a retornar colunas Polars
`Decimal`, inclusive quando vazias:

| Dado | Tipo da coluna | Unidade |
|---|---|---|
| Selic Over, Selic Meta e DI Over | `Decimal(18, 10)` | Taxa em unidade decimal |
| PTAX | `Decimal(18, 4)` | R$/US$ |

Os valores são lidos diretamente do texto do SGS, sem passagem por `float`.
As taxas percentuais são divididas por 100 em aritmética decimal. A escala
10 preserva até oito casas decimais do percentual publicado; zeros adicionais
não representam precisão adicional da fonte. A PTAX preserva quatro casas.

A mudança é incompatível com código que depende de `float` ou `Float64`.
Escalares sem dados passam de `float("nan")` para `Decimal("NaN")`; use
`valor.is_nan()` para identificá-los. Séries sem dados continuam vazias,
com seus novos esquemas decimais. Para uma conversão explícita:

```python
taxa = yd.selic.over("28-02-2025")
taxa_float = float(taxa)
serie_float = yd.selic.over_serie(ultimos=5).with_columns(
    taxa=pl.col("taxa").cast(pl.Float64)
)
```

A conversão para ponto flutuante pode introduzir aproximação binária.
Em cálculos com `Decimal`, crie constantes a partir de texto, por exemplo,
`Decimal("0.01")`; precisão e arredondamento dos cálculos continuam dependendo
da metodologia. Integrações com NumPy e serializadores JSON podem exigir
conversão explícita. Os cálculos e o esquema de leilões de TPF permanecem em
`Float64`, com a PTAX convertida explicitamente ao entrar nesse fluxo.

## Taxas Over anuais e diárias

`yd.selic.over`, `yd.selic.over_serie` e `yd.di_over` aceitam o parâmetro
opcional `anualizada`, exclusivo por palavra-chave. O padrão `True` preserva
a unidade atual: taxas ao ano, em base 252 dias úteis, agora do tipo `Decimal`.
Com `anualizada=False`, retornam taxas em decimal ao dia útil:

```python
taxa_anual = yd.selic.over("28-02-2025")
taxa_diaria = yd.selic.over("28-02-2025", anualizada=False)
serie_diaria = yd.selic.over_serie("27-02-2025", "28-02-2025", anualizada=False)
di_diaria = yd.di_over("28-02-2025", anualizada=False)
```

A modalidade seleciona a série publicada pelo Banco Central: Selic Over usa
SGS 1178 (anualizada) ou 11 (diária); DI Over usa SGS 4389 (anualizada) ou
12 (diária). A biblioteca converte o percentual da fonte para decimal, sem
recalcular uma modalidade a partir da outra. A frequência das observações é
diária em ambos os casos. Selic Meta mantém seu contrato anual.

## Falhas de rede e retentativas

A PYield faz no máximo duas tentativas por download. Timeout, falha de conexão,
transferência interrompida e HTTP 502, 503 ou 504 permitem uma única repetição,
após uma espera aleatória entre 0,5 e 1 segundo. Se a segunda tentativa falhar,
a exceção é propagada. A retentativa não mantém cache entre chamadas.

A regra é compartilhada pelos downloads HTTP de BCB, ANBIMA, B3, IBGE,
Tesouro Nacional, FGV e históricos em Parquet do projeto. Os POSTs de consulta
do IMA-Q e de exportação do CSV de derivativos da B3 também usam a regra por
serem operações de leitura seguras de repetir. Não há habilitação automática
de retry para qualquer POST.

A consulta `yd.di_over(data)` passa do FTP da CETIP para a API HTTPS do
Banco Central, série SGS 4389 por padrão ou SGS 12 com `anualizada=False`.
O padrão continua sendo a taxa anualizada em base 252, agora do tipo `Decimal`
(por exemplo, `Decimal("0.1315000000")` representa 13,15% a.a.).
O histórico passa a alcançar 06/03/1986, em vez de 20/08/2012. Datas sem dados
retornam `Decimal("NaN")`; HTTP 404 no SGS representa ausência de dados.
Falhas de transporte passam a propagar exceções de `requests`, como nas
demais consultas SGS, em lugar de `ConnectionError` do adaptador FTP.

Erros de entrada, parsing, validação de arquivos e demais erros HTTP, incluindo
429, não são repetidos. Nos status elegíveis, `Retry-After` é respeitado quando
pede até um segundo, tanto em segundos inteiros quanto em data HTTP. Um valor
inválido ou que exige espera maior impede a retentativa; a biblioteca propaga
o erro sem antecipar o prazo pedido pela fonte.

O retry envolve apenas o transporte. Em consultas paginadas, repete somente a
página que falhou, preservando as anteriores. Downloads de anexos e blocos de
séries recebem seu próprio limite, sem repetir todo o fluxo de consulta ou o
processamento dos dados.

Antes, o retry padrão fazia até três tentativas com esperas crescentes. A nova
regra procura absorver um soluço de comunicação com uma única oportunidade
adicional. Recuperar um processo noturno por minutos ou horas continua sendo
responsabilidade da aplicação. Esta separação também evita a multiplicação de
tentativas em várias camadas, discutida na
[Amazon Builders' Library](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/).

Consultas longas de Selic Meta passam a buscar os blocos sequencialmente, como
as demais séries SGS. Esgotada a retentativa de um bloco, os seguintes não são
enviados.
Consultas bem-sucedidas com vários blocos podem levar mais tempo que antes,
pois deixam de usar downloads paralelos. Paginação, anexos e consultas a fontes
alternativas previstas no fluxo continuam podendo exigir downloads distintos.

Nas demais consultas, os tipos das exceções existentes são preservados. Por exemplo,
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

Quem precisar de recuperação prolongada deve limitar o número de execuções e
o tempo total do processo, considerar `Retry-After` e evitar repetir erros de
entrada ou certificado. Quando a fonte já estiver reconhecidamente indisponível,
a aplicação pode suspender novas consultas até a próxima verificação.

As consultas HTTP separam os timeouts: conexão de 5 segundos e leitura com o
limite de cada endpoint, de 10, 30 ou 60 segundos. A DI over passa a usar
conexão de 5 segundos e leitura de 30 segundos, como as demais consultas SGS.

Espera curta entre tentativas não significa prazo total curto. Os timeouts se
aplicam a cada tentativa. Um timeout de
leitura de 60 segundos pode ser consumido novamente na segunda tentativa. No
Requests, os timeouts limitam a conexão e a inatividade de leitura, e não o
tempo total da chamada; DNS, múltiplos endereços, redirects e transferências
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

As mudanças de cache e retry não alteram fórmulas ou unidades. Os indicadores
SGS passam a usar os tipos decimais descritos acima, e as taxas Over ganham
a opção de retorno ao dia útil. As notas da
[0.60.0](https://crdcj.github.io/PYield/release-0.60.0/) continuam disponíveis para migrações anteriores.
