# Operações compromissadas

`yd.compromissadas` consulta os leilões de operações compromissadas realizados pelo Banco Central do Brasil.

`compromissadas` fica na raiz porque representa leilões de operações compromissadas, e não uma série da taxa Selic.

Módulo para consulta dos leilões de operações compromissadas (repos) realizados pelo BCB.

Fonte oficial (API OData): https://olinda.bcb.gov.br/olinda/servico/leiloes\_selic/versao/v1/aplicacao\#!/recursos/leiloes\_compromissadas

Exemplo de chamada bruta (CSV): https://olinda.bcb.gov.br/olinda/servico/leiloes\_selic/versao/v1/odata/leiloes\_compromissadas(dataLancamentoInicio=@dataLancamentoInicio,dataLancamentoFim=@dataLancamentoFim,horaInicio=@horaInicio,dataLiquidacao=@dataLiquidacao,dataRetorno=@dataRetorno,publicoPermitidoLeilao=@publicoPermitidoLeilao,nomeTipoOferta=@nomeTipoOferta)?@dataLancamentoInicio='2025-08-21'&amp;@dataLancamentoFim='2025-08-21'&amp;$format=text/csv

Exemplo (CSV original): id , dataMovimento, horaInicio, publicoPermitidoLeilao, numeroComunicado, nomeTipoOferta , ofertante , prazoDiasCorridos, dataLiquidacao, dataRetorno, volumeAceito, taxaCorte, percentualCorte ac1b013d13d6fb1d9d9e251b800010ee, 2025-08-21 , 09:00 , SomenteDealer , null , Tomador , Banco Central, 1, 2025-08-21 , 2025-08-22 , 647707406, "14,9" , 0 ac1b013d13d6fb1d9d9e251b8000121e, 2025-08-21 , 12:00 , TodoMercado , 43716 , Compromissada 1047, Banco Central, 91, 2025-08-22 , 2025-11-21 , 5000000, "99,78" , "64,13"

## `compromissadas(inicio=None, fim=None)`

Consulta e retorna leilões de operações compromissadas do BCB.

Semântica dos parâmetros de período (API OData): - inicio somente: dados de inicio até o fim da série. - fim somente: dados do início da série até fim. - ambos omitidos: série histórica completa.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Data inicial (inclusive) ou None. | `None` |
| `fim` | `DateLike \| None` | Data final (inclusive) ou None. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com colunas normalizadas em português e tipos |
| `DataFrame` | enriquecidos (frações decimais, inteiros, datas). |

**Output Columns**

- data\_leilao (Date): data de ocorrência do leilão.
- data\_liquidacao (Date): data de liquidação (início da operação).
- data\_retorno (Date): data de recompra / término da operação.
- hora\_inicio (Time): horário de início do leilão.
- prazo\_dc (Int64): dias corridos até a data de retorno.
- prazo\_du (Int64): dias úteis entre liquidação e retorno.
- comunicado (Int64): número do comunicado/aviso do BC (pode ser nulo).
- tipo\_oferta (String): classif. do tipo de oferta (ex: Tomador, Compromissada 1047).
- publico (String): público permitido no leilão (SomenteDealer, TodoMercado).
- financeiro\_aceito (Float64): financeiro aceito no leilão em reais (convertido de milhares).
- taxa\_corte (Float64): taxa de corte (ex. 0.1490 = 14,90%). Nula se financeiro\_aceito = 0.
- pct\_aceito (Float64): percentual do volume ofertado efetivamente aceito (0-100). 100 = nenhuma rejeição. 0 indica nada aceito (volume\_aceito = 0).

**Notes**

- Dados ordenados por: data\_leilao, hora\_inicio, tipo\_oferta.

Examples:

```pycon
>>> import polars as pl
>>> _ = pl.Config.set_tbl_width_chars(210)
>>> _ = pl.Config.set_tbl_cols(-1)
>>> import pyield as yd
>>> yd.compromissadas(inicio="21-08-2025", fim="21-08-2025")
shape: (2, 12)
┌─────────────┬─────────────────┬──────────────┬─────────────┬──────────┬──────────┬────────────┬────────────────────┬───────────────┬───────────────────┬────────────┬────────────┐
│ data_leilao ┆ data_liquidacao ┆ data_retorno ┆ hora_inicio ┆ prazo_dc ┆ prazo_du ┆ comunicado ┆ tipo_oferta        ┆ publico       ┆ financeiro_aceito ┆ taxa_corte ┆ pct_aceito │
│ ---         ┆ ---             ┆ ---          ┆ ---         ┆ ---      ┆ ---      ┆ ---        ┆ ---                ┆ ---           ┆ ---               ┆ ---        ┆ ---        │
│ date        ┆ date            ┆ date         ┆ time        ┆ i64      ┆ i64      ┆ i64        ┆ str                ┆ str           ┆ f64               ┆ f64        ┆ f64        │
╞═════════════╪═════════════════╪══════════════╪═════════════╪══════════╪══════════╪════════════╪════════════════════╪═══════════════╪═══════════════════╪════════════╪════════════╡
│ 2025-08-21  ┆ 2025-08-21      ┆ 2025-08-22   ┆ 09:00:00    ┆ 1        ┆ 1        ┆ null       ┆ Tomador            ┆ SomenteDealer ┆ 6.4771e11         ┆ 0.149      ┆ 100.0      │
│ 2025-08-21  ┆ 2025-08-22      ┆ 2025-11-21   ┆ 12:00:00    ┆ 91       ┆ 64       ┆ 43716      ┆ Compromissada 1047 ┆ TodoMercado   ┆ 5.0000e9          ┆ 0.9978     ┆ 35.87      │
└─────────────┴─────────────────┴──────────────┴─────────────┴──────────┴──────────┴────────────┴────────────────────┴───────────────┴───────────────────┴────────────┴────────────┘
>>> _ = pl.Config.restore_defaults()
>>> _ = pl.Config.set_tbl_width_chars(150)
```
