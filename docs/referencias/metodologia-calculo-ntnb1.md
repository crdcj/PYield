# Metodologia de cálculo da NTN-B1 — Tesouro Direto

Esta página preserva as fontes oficiais usadas como referência para a
implementação da NTN-B1, comercializada como Tesouro Educa+ e Tesouro RendA+.
Os documentos descrevem a metodologia de cálculo do preço e da cotação no
Tesouro Direto; não são a metodologia geral da STN para títulos ofertados em
leilões primários.

## Documentos oficiais

### Tesouro Educa+

- Órgão: Tesouro Nacional.
- Título: *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro
  Direto — Tesouro Educa+ (NTN-B1)*.
- [Publicação oficial](https://www.tesourodireto.com.br/documents/d/guest/tesouro_educa_mais).
- [Cópia preservada](NTN-B_Educ_CODIP_10jul23.pdf).
- Arquivo: `NTN-B_Educ_CODIP_10jul23.pdf`.
- SHA-256:
  `91d13c9bcdd550bb505b08790f1e8724f151e39e6334e2f000efe9dbca049e05`.

### Tesouro RendA+

- Órgão: Tesouro Nacional.
- Título: *Cálculo da Rentabilidade dos Títulos Públicos Ofertados no Tesouro
  Direto — Tesouro RendA+ (NTN-B1)*.
- [Publicação oficial](https://www.tesourodireto.com.br/documents/d/guest/tesouro_renda_mais).
- [Cópia preservada](NTN-B_Renda_CODIP_10jul23.pdf).
- Arquivo: `NTN-B_Renda_CODIP_10jul23.pdf`.
- SHA-256:
  `bf97258ce111f167488fd9e8d70dd22752a872f3bf4f8ef56b5d295882915c0e`.

As cópias são mantidas no repositório sem alteração do conteúdo original. Os
hashes permitem verificar a identidade dos arquivos usados como referência.

### Consulta pública da STN

- Órgão: Ministério da Fazenda — Secretaria do Tesouro Nacional.
- Título: *Composição dos pagamentos mensais das Notas do Tesouro Nacional
  Série B, Subsérie 1 (NTN-B1)*.
- [Consulta pública oficial no Participa + Brasil](https://www.gov.br/participamaisbrasil/consulta-portaria-composicao-ntn-b11).
- Processo SEI: `17944.005214/2024-09`.
- Status: consulta encerrada; publicação no DOU em 07/11/2024.

Esta consulta pública apresenta as fórmulas da composição das parcelas de
amortização e rendimento, incluindo `Tai = 1/n`, o ajuste da última parcela e
as precisões de cálculo. Ela é uma proposta submetida à consulta pública, não
uma portaria final; por isso, a referência é usada como fonte pública de
metodologia e não como prova de vigência normativa.

## Correspondência com o PYield

Os documentos oficiais sustentam as seguintes partes da implementação em
`pyield.tpf.titulos.ntnb1`:

- `datas_pagamento` e `fluxos_caixa`: pagamentos mensais a partir da data de
  conversão, com 60 parcelas para o Educa+ e 240 parcelas para o RendA+;
- `fluxos_caixa`: amortizações iguais sobre um fluxo hipotético de 100 e ajuste
  da última parcela para que a soma seja 100;
- `cotacao`: cotação como valor presente dos fluxos, descontados por
  `(1 + taxa) ** (DU / 252)`;
- `pu`: preço como VNI projetado multiplicado pela cotação.
- `pu`: a consulta pública da STN também registra a composição do pagamento a
  partir do preço de aquisição e da taxa de amortização.

Os documentos informam que a razão `DU / 252` é calculada com 14 casas sem
arredondamento e que a cotação é truncada na quarta casa percentual. Na
implementação, cada valor presente é arredondado em 12 casas antes da soma. O
PYield representa a cotação em base 1, por isso a função `cotacao` retorna o
fator correspondente truncado em 6 casas decimais.

O cálculo intermediário do PU pode conservar 6 casas decimais, como nos
exemplos reproduzidos pelos testes. O preço efetivamente exibido ou negociado
é truncado na segunda casa decimal, conforme a ressalva dos documentos.

A documentação oficial consultada não define a interpolação flat-forward, a
regra de manutenção da última taxa após o maior vértice ou a extrapolação da
curva zero. Essas são decisões da implementação de `cotacao_curva_zero` e
`taxa_curva_zero`, e não devem ser atribuídas ao método do Tesouro Direto.
