# Novidades na versão 0.64.1

Versão de correção. Reduz a espera quando uma fonte externa aceita a conexão mas não responde e corrige uma falha de leitura em `yd.cpm.contratos`. A API pública, as colunas e os valores retornados não mudam.

## Timeouts de leitura com retentativa mais longa

O timeout de leitura das consultas externas passa a ser menor na primeira tentativa e dobra na retentativa. O padrão é de 5 segundos na primeira e 10 na segunda. O timeout de conexão continua em 5 segundos, e as regras de retentativa não mudam: no máximo duas tentativas, apenas para falhas transitórias de comunicação ou HTTP 502, 503 e 504.

A mudança reduz a espera quando a fonte aceita a conexão mas não envia a resposta. Esse foi o caso das planilhas de VNA do Tesouro Nacional, usadas por `yd.vna` e pelas funções de NTN-B e NTN-C que dependem do VNA: a falha levava cerca de 2 minutos para aparecer e passa a levar cerca de 16 segundos.

Os limites foram revistos com medições do tempo de resposta de cada fonte:

- VNA, RMD e secundário mensal de TPF usavam 60 segundos e respondem em menos de 1 segundo. Passam a 5 e 10.
- Calendários do Copom, do IPCA e do IGP-M usavam 30 segundos e respondem em menos de 2. Passam a 5 e 10.
- SGS, CPM, secundário intradia de TPF e índices do IGP-M mantêm o limite máximo de 30 segundos, agora com 15 na primeira tentativa. Essas fontes levam de 4 a 9 segundos em consultas comuns.
- As demais fontes usavam 10 segundos e passam a 5 e 10, mantendo o mesmo limite máximo.

## Correções

- `yd.cpm.contratos` deixa de gerar `ComputeError` em datas como 30/10/2026. O CSV de derivativos consolidados da B3 era lido por inteiro, e o tipo de colunas não usadas, como `Preço de abertura`, era inferido pelas primeiras linhas. Quando essas linhas tinham apenas inteiros e uma linha seguinte trazia decimais, a leitura falhava. Agora só as colunas de código e de preço de referência são lidas.
