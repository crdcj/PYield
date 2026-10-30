# Calendário do Copom

`yd.copom` combina atas históricas e o calendário ICS do BCB automaticamente. As datas futuras dependem da agenda publicada, sem atualização manual anual.

## Migração

Use `yd.copom.calendario(inicio=..., fim=...)` no lugar de `yd.selic.copom.calendar(start=..., end=...)` e `yd.copom.proxima_reuniao(referencia=...)` no lugar de `next_meeting(reference=...)`. As colunas `MeetingNumber`, `StartDate`, `EndDate` e `ExpiryDate` passam a ser `nro_reuniao`, `data_inicio`, `data_decisao` e `data_efetividade`. O início fica nulo quando não está disponível no ICS; não é inferido da decisão.

Calendário de reuniões do Copom: calendario e proxima\_reuniao.

## `calendario(inicio=None, fim=None)`

Consulta reuniões históricas e agendadas do Copom nas fontes do BCB.

Combina a API de atas históricas com o calendário ICS de reuniões do BCB, sem datas anuais fixas. As atas têm prioridade na data de decisão; o ICS complementa o início e mantém reuniões ainda sem ata, inclusive passadas.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Limite inicial inclusivo para a data de decisão. Sem limite quando None. Aceita DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD e date. | `None` |
| `fim` | `DateLike \| None` | Limite final inclusivo para a data de decisão, nos mesmos formatos. Sem limite quando None. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame ordenado por data\_decisao, sem duplicatas nessa coluna. |
| `DataFrame` | Consultas sem registros retornam DataFrame vazio com o mesmo esquema. |

**Output Columns**

nro\_reuniao (Int64): Número sequencial informado na ata; nulo sem ata. data\_inicio (Date): Primeiro dia informado no ICS; nulo fora da agenda. data\_decisao (Date): Último dia da reunião, dataReferencia nas atas. data\_publicacao (Date): Publicação da ata; nula quando não disponível. data\_efetividade (Date): Próximo dia útil após a decisão, calculado pelo calendário brasileiro de yd.du; data de vencimento/liquidação CPM. status (String): "Realizada" se a decisão for até hoje no Brasil; "A realizar" caso contrário. Classificação por data, não por ata. titulo (String): Título original da ata; nulo quando não disponível. fonte (String): "Ata" ou "Calendário", conforme a origem da decisão.

**Notes**

O horizonte depende das datas publicadas pelo BCB. A consulta às atas solicita até 500 registros. O conteúdo bruto fica em cache por oito horas; o status é recalculado a cada chamada. Erros de rede e payload inválido são propagados. O ICS publica um evento por dia: dias únicos consecutivos devem formar pares; grupos incompletos geram ValueError. O início histórico não é inferido subtraindo um dia da decisão.

Examples:

```pycon
>>> cal = yd.copom.calendario(inicio="01-01-2026")
>>> cal["data_decisao"].is_sorted()
True
```

## `proxima_reuniao(referencia=None)`

Consulta a primeira reunião com decisão em ou após a referência.

Usa a API de atas e o calendário ICS do BCB, através de calendario.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `referencia` | `DateLike \| None` | Data inclusiva nos formatos DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD ou date. None usa a data atual no Brasil. | `None` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com até uma linha, com o mesmo esquema de calendario. |
| `DataFrame` | Retorna vazio quando não há reunião publicada a partir da referência. |

**Output Columns**

nro\_reuniao (Int64): Número sequencial da ata, quando disponível. data\_inicio (Date): Primeiro dia no ICS, quando disponível. data\_decisao (Date): Último dia da reunião. data\_publicacao (Date): Publicação da ata, quando disponível. data\_efetividade (Date): Próximo dia útil após a decisão. status (String): "Realizada" ou "A realizar", conforme a data atual. titulo (String): Título da ata, quando disponível. fonte (String): "Ata" ou "Calendário".

Examples:

```pycon
>>> yd.copom.proxima_reuniao("01-01-2026").height
1
```
