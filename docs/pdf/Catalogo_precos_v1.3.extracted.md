# Catalogo de Precos v1.3 (resumo do conteudo)

Fonte original: docs/pdf/Catalogo_precos_v1.3.pdf
Metodo: leitura do PDF com extracao textual e consolidacao editorial.

## Objetivo do documento

O catalogo define as mensagens, arquivos, schemas e dicionario de campos usados
na divulgacao de Precos e Indices da B3.

## Historico de revisao (catalogo)

| Data | Versao | Alteracao registrada |
|---|---|---|
| 31/08/2015 | 1.0 | Inclusao da mensagem bvmf.217.01 e arquivos iniciais. |
| 15/02/2016 | 1.1 | Ajustes nos XMLs de exemplo. |
| 09/09/2016 | 1.2 | Alteracao na mensagem bvmf.217.01. |
| 07/10/2021 | 1.3 | Inclusao dos arquivos BVBG.186.01 e BVBG.187.01; alteracao no BVBG.086.01. |

## Mensagens, schemas e arquivos

| Mensagem | Versao XSD | Arquivos associados |
|---|---|---|
| bvmf.217.01 | 1.1 | BVBG.086.01 (PriceReport), BVBG.186.01 (EquitiesSimplifiedPriceReport), BVBG.187.01 (DerivativesSimplifiedPriceReport) |
| bvmf.218.01 | 1.0 | BVBG.087.01 |

## Estrutura tecnica da mensagem PriceReport (bvmf.217.01)

O catalogo descreve a estrutura em niveis (INDEX), com item de mensagem, tag XML,
multiplicidade, tipo de dado e descricao biligue (EN/PT).

### Glossario rapido

- `index`: nivel/caminho do campo na estrutura da mensagem.
- `message_item`: nome funcional do campo no catalogo.
- `tag`: nome da tag XML correspondente.
- `mult`: multiplicidade da tag no XML.
- `data_type`: tipo de dado definido no schema.
- `description_en`: descricao original em ingles.
- `descricao_pt`: descricao em portugues no catalogo.
- `regra`: referencia de regra condicional do layout.

Leitura de `mult`:

- `[1..1]`: obrigatorio, aparece exatamente uma vez.
- `[0..1]`: opcional, aparece zero ou uma vez.
- `[1..*]`: obrigatorio com repeticao, aparece uma ou mais vezes.
- `[0..*]`: opcional com repeticao, aparece zero ou mais vezes.

Leitura de `regra`:

- `-`: sem regra explicita indicada na linha do catalogo.
- `R1`, `R2`, `R1, R2`: campo sujeito as regras nomeadas no manual tecnico.

### Blocos em formato vertical (8 linhas por index)

#### Index 1.0
- index: 1.0
- message_item: TradeDate
- tag: TradDt
- mult: [1..1]
- data_type: +
- description_en: Provides the trade date.
- descricao_pt: Fornece a data de negociacao.
- regra: -

#### Index 1.1
- index: 1.1
- message_item: Date
- tag: Dt
- mult: [1..1]
- data_type: ISODate
- description_en: Specified date.
- descricao_pt: Especifica uma data.
- regra: -

#### Index 2.0
- index: 2.0
- message_item: SecurityIdentification
- tag: SctyId
- mult: [1..1]
- data_type: +
- description_en: This block contains the Ticker Symbol.
- descricao_pt: Este bloco contem informacoes do bloco de negociacao.
- regra: -

#### Index 2.1
- index: 2.1
- message_item: TickerSymbol
- tag: TckrSymb
- mult: [1..1]
- data_type: TickerIdentifier
- description_en: Letters that identify a stock traded on a stock exchange.
- descricao_pt: Codigo que identifica um instrumento negociado/registrado em bolsa de valores.
- regra: -

#### Index 3.0
- index: 3.0
- message_item: FinancialInstrumentIdentification
- tag: FinInstrmId
- mult: [1..1]
- data_type: +
- description_en: Provides details about the security identification.
- descricao_pt: Fornece detalhes da identificacao do instrumento.
- regra: -

#### Index 3.1
- index: 3.1
- message_item: OtherIdentification
- tag: OthrId
- mult: [1..1]
- data_type: +
- description_en: Identification of a security by proprietary or domestic identification scheme.
- descricao_pt: Identificacao proprietaria de um instrumento.
- regra: -

#### Index 3.1.1
- index: 3.1.1
- message_item: Identification
- tag: Id
- mult: [1..1]
- data_type: Max35Text
- description_en: Identification of a security. Instrument sequential code in the Trade Structure system.
- descricao_pt: Identificacao de um instrumento. Codigo sequencial do instrumento no sistema Trade Structure.
- regra: -

#### Index 3.1.2
- index: 3.1.2
- message_item: Type
- tag: Tp
- mult: [1..1]
- data_type: +
- description_en: Identification type.
- descricao_pt: Tipo da identificacao.
- regra: -

#### Index 3.1.2.1
- index: 3.1.2.1
- message_item: Proprietary
- tag: Prtry
- mult: [1..1]
- data_type: Max35Text
- description_en: Unique and unambiguous identification source using a proprietary identification scheme. Valid Values: 8.
- descricao_pt: Identificacao unica e inequivoca usando um esquema de identificacao proprietaria. Valores validos: 8.
- regra: -

#### Index 3.2
- index: 3.2
- message_item: PlaceOfListing
- tag: PlcOfListg
- mult: [1..1]
- data_type: +
- description_en: Market on which the security is listed.
- descricao_pt: Mercado em que o instrumento esta listado.
- regra: -

#### Index 3.2.1
- index: 3.2.1
- message_item: MarketIdentifierCode
- tag: MktIdrCd
- mult: [1..1]
- data_type: MICIdentifier
- description_en: Market Identifier Code (ISO 10383). Default Value = BVMF.
- descricao_pt: Codigo identificador do mercado financeiro conforme ISO 10383. Default = BVMF.
- regra: -

#### Index 4.0
- index: 4.0
- message_item: TradeDetails
- tag: TradDtls
- mult: [0..1]
- data_type: +
- description_en: This block contains information related to trade.
- descricao_pt: Este bloco contem informacoes relacionadas ao negocio.
- regra: -

#### Index 4.1
- index: 4.1
- message_item: DaysToSettlement
- tag: DaysToSttlm
- mult: [0..1]
- data_type: Max4Text
- description_en: Indicates number of days to settlement.
- descricao_pt: Prazo em dias para liquidacao do contrato a termo.
- regra: -

#### Index 4.2
- index: 4.2
- message_item: TradeQuantity
- tag: TradQty
- mult: [0..1]
- data_type: RestrictedBVMF2ActiveAnd0DecimalQuantity
- description_en: Trade Quantity.
- descricao_pt: Quantidade de negocios no dia.
- regra: R2

#### Index 5.0
- index: 5.0
- message_item: FinancialInstrumentAttributes
- tag: FinInstrmAttrbts
- mult: [1..1]
- data_type: +
- description_en: Provides Financial Instrument Attributes.
- descricao_pt: Elementos que caracterizam um instrumento.
- regra: -

#### Index 5.1
- index: 5.1
- message_item: MarketDataStreamIdentification
- tag: MktDataStrmId
- mult: [0..1]
- data_type: ExternalMarketDataStreamIdentificationCode
- description_en: The identifier or name of the price stream.
- descricao_pt: Identificacao ou nome do fluxo preco.
- regra: R1, R2

#### Index 5.2
- index: 5.2
- message_item: NationalFinancialVolume
- tag: NtlFinVol
- mult: [0..1]
- data_type: RestrictedBVMF4ActiveOrHistoricCurrencyAnd8DecimalAmount
- description_en: Financial volume traded (R$).
- descricao_pt: Volume financeiro negociado no dia em R$.
- regra: R2

#### Index 5.3
- index: 5.3
- message_item: InternationalFinancialVolume
- tag: IntlFinVol
- mult: [0..1]
- data_type: RestrictedBVMF4ActiveOrHistoricCurrencyAnd8DecimalAmount
- description_en: Financial traded volume (U$).
- descricao_pt: Volume financeiro negociado no dia em U$.
- regra: R2

#### Index 5.4
- index: 5.4
- message_item: OpenInterest
- tag: OpnIntrst
- mult: [0..1]
- data_type: RestrictedBVMFActiveAnd8DecimalQuantity
- description_en: Quantity of open contract.
- descricao_pt: Quantidade de contratos em aberto.
- regra: -

#### Index 5.5
- index: 5.5
- message_item: FinancialInstrumentQuantity
- tag: FinInstrmQty
- mult: [0..1]
- data_type: RestrictedBVMFActiveAnd8DecimalQuantity
- description_en: Quantity of financial instrument traded.
- descricao_pt: Quantidade de contratos/titulos negociados no dia.
- regra: R2

#### Index 5.6
- index: 5.6
- message_item: BestBidPrice
- tag: BestBidPric
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Best Bid Price.
- descricao_pt: Preco da melhor oferta de compra.
- regra: R2

#### Index 5.7
- index: 5.7
- message_item: BestAskPrice
- tag: BestAskPric
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Best Ask Price.
- descricao_pt: Preco da melhor oferta de venda.
- regra: R2

Observacao operacional (5.6 e 5.7):
- `BestBidPric` e `BestAskPric` representam a ultima melhor oferta de compra/venda
	no snapshot diario do arquivo (nao sao serie intradiaria completa de book).
- Como ambos sao `[0..1]`, podem vir nulos em alguns contratos/dias.
- Quando presentes, o spread de book e dado por `BestAskPric - BestBidPric`.

#### Index 5.8
- index: 5.8
- message_item: FirstPrice
- tag: FrstPric
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Amount of the initial value of the asset.
- descricao_pt: Preco do primeiro negocio do dia.
- regra: -

#### Index 5.9
- index: 5.9
- message_item: MinimumPrice
- tag: MinPric
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Minimum Price.
- descricao_pt: Preco minimo do dia.
- regra: -

#### Index 5.10
- index: 5.10
- message_item: MaximumPrice
- tag: MaxPric
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Maximum Price.
- descricao_pt: Preco maximo do dia.
- regra: -

#### Index 5.11
- index: 5.11
- message_item: TradeAveragePrice
- tag: TradAvrgPric
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Trade Average Price.
- descricao_pt: Preco medio dos negocios do dia.
- regra: -

#### Index 5.12
- index: 5.12
- message_item: LastPrice
- tag: LastPric
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Last price.
- descricao_pt: Preco do ultimo negocio do dia.
- regra: -

#### Index 5.13
- index: 5.13
- message_item: RegularTransactionsQuantity
- tag: RglrTxsQty
- mult: [0..1]
- data_type: RestrictedBVMF2ActiveAnd0DecimalQuantity
- description_en: Number of Transactions.
- descricao_pt: Quantidade de negocios na sessao regular.
- regra: -

#### Index 5.14
- index: 5.14
- message_item: NonRegularTransactionsQuantity
- tag: NonRglrTxsQty
- mult: [0..1]
- data_type: RestrictedBVMF2ActiveAnd0DecimalQuantity
- description_en: Number of Transactions.
- descricao_pt: Quantidade de negocios na sessao nao regular.
- regra: R2

#### Index 5.15
- index: 5.15
- message_item: RegularTradedContracts
- tag: RglrTraddCtrcts
- mult: [0..1]
- data_type: RestrictedBVMF2ActiveAnd0DecimalQuantity
- description_en: Non regular traded contracts.
- descricao_pt: Quantidade de contratos/titulos negociados na sessao regular.
- regra: R2

#### Index 5.16
- index: 5.16
- message_item: NonRegularTradedContracts
- tag: NonRglrTraddCtrcts
- mult: [0..1]
- data_type: RestrictedBVMF2ActiveAnd0DecimalQuantity
- description_en: Non regular traded contracts.
- descricao_pt: Quantidade de contratos/titulos negociados na sessao nao regular.
- regra: R2

#### Index 5.17
- index: 5.17
- message_item: NationalRegularVolume
- tag: NtlRglrVol
- mult: [0..1]
- data_type: RestrictedBVMF4ActiveOrHistoricCurrencyAnd8DecimalAmount
- description_en: Traded volume (R$) - After Market.
- descricao_pt: Volume em R$ na sessao regular.
- regra: R2

#### Index 5.18
- index: 5.18
- message_item: NationalNonRegularVolume
- tag: NtlNonRglrVol
- mult: [0..1]
- data_type: RestrictedBVMF4ActiveOrHistoricCurrencyAnd8DecimalAmount
- description_en: Traded volume (R$) - After Market.
- descricao_pt: Volume em R$ na sessao nao regular.
- regra: R2

#### Index 5.19
- index: 5.19
- message_item: InternationalRegularVolume
- tag: IntlRglrVol
- mult: [0..1]
- data_type: RestrictedBVMF4ActiveOrHistoricCurrencyAnd8DecimalAmount
- description_en: Traded volume (U$) - After Market.
- descricao_pt: Volume em U$ na sessao regular.
- regra: R2

#### Index 5.20
- index: 5.20
- message_item: InternationalNonRegularVolume
- tag: IntlNonRglrVol
- mult: [0..1]
- data_type: RestrictedBVMF4ActiveOrHistoricCurrencyAnd8DecimalAmount
- description_en: Traded volume (U$) - After Market.
- descricao_pt: Volume em U$ na sessao nao regular.
- regra: R2

#### Index 5.21
- index: 5.21
- message_item: AdjustedQuote
- tag: AdjstdQt
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Adjusted quote.
- descricao_pt: Cotacao ajuste (futuro) e opcoes com ajustes.
- regra: -

#### Index 5.22
- index: 5.22
- message_item: AdjustedQuoteTax
- tag: AdjstdQtTax
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Adjusted quote.
- descricao_pt: Cotacao ajuste (fut) e opc c/ ajuste (em taxa).
- regra: -

#### Index 5.23
- index: 5.23
- message_item: AdjustedQuoteSituation
- tag: AdjstdQtStin
- mult: [0..1]
- data_type: Max1Text
- description_en: Adjust quote situation.
- descricao_pt: Situacao do ajuste do dia.
- regra: -

#### Index 5.24
- index: 5.24
- message_item: PreviousAdjustedQuote
- tag: PrvsAdjstdQt
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Previous session adjusted quote.
- descricao_pt: Cotacao de ajuste do dia anterior (futuro).
- regra: -

#### Index 5.25
- index: 5.25
- message_item: PreviousAdjustedQuoteTax
- tag: PrvsAdjstdQtTax
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Previous session adjusted quote.
- descricao_pt: Cotacao de ajuste do dia anterior (fut) (em Taxa).
- regra: -

#### Index 5.26
- index: 5.26
- message_item: PreviousAdjustedQuoteSituation
- tag: PrvsAdjstdQtStin
- mult: [0..1]
- data_type: Max1Text
- description_en: Previous session adjusted quote situation.
- descricao_pt: Situacao do ajuste do dia anterior.
- regra: -

#### Index 5.27
- index: 5.27
- message_item: OscillationPercentage
- tag: OscnPctg
- mult: [0..1]
- data_type: RestrictedBVMFActiveAnd2DecimalQuantity
- description_en: Rate of oscillation.
- descricao_pt: Percentual de oscilacao.
- regra: R2

#### Index 5.28
- index: 5.28
- message_item: VariationPoints
- tag: VartnPts
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Variation in points.
- descricao_pt: Diferenca dos precos de ajustes do dia anterior - fechamento para Derivativos.
- regra: R2

#### Index 5.29
- index: 5.29
- message_item: EquivalentValue
- tag: EqvtVal
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Equivalence value.
- descricao_pt: Somente para agricolas, conversao para Real (R$) do preco de ajuste atual ou do preco de exercicio para instrumentos de opcoes.
- regra: R2

#### Index 5.30
- index: 5.30
- message_item: AdjustedValueContract
- tag: AdjstdValCtrct
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Adjust value the contract.
- descricao_pt: Valor do Ajuste por contrato em R$.
- regra: R2

#### Index 5.31
- index: 5.31
- message_item: MaximumTradeLimit
- tag: MaxTradLmt
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Maximum trade limit.
- descricao_pt: Limite maximo para negociacao.
- regra: R2

#### Index 5.32
- index: 5.32
- message_item: MinimumTradeLimit
- tag: MinTradLmt
- mult: [0..1]
- data_type: RestrictedBVMFActiveOrHistoricCurrencyAnd12DecimalAmount
- description_en: Minimum trade limit.
- descricao_pt: Limite minimo para negociacao.
- regra: R2

## Notas de leitura

1. A extracao do PDF possui quebras de linha e hifenizacao; os textos acima foram recompostos para leitura continua.
2. Em alguns itens, o texto em ingles do proprio catalogo apresenta inconsistencias (ex.: 5.15 e 5.16), mantidas aqui por fidelidade.
3. O conteudo acima e exclusivamente descritivo do PDF, sem proposta de renomeacao.

## Observacao

Este arquivo e um resumo do conteudo principal do PDF.
