# Calendário do Copom

`yd.copom` combina atas históricas e o calendário ICS do BCB automaticamente.
As datas futuras dependem da agenda publicada, sem atualização manual anual.

## Migração

Use `yd.copom.calendario(inicio=..., fim=...)` no lugar de
`yd.selic.copom.calendar(start=..., end=...)` e
`yd.copom.proxima_reuniao(referencia=...)` no lugar de `next_meeting(reference=...)`.
As colunas `MeetingNumber`, `StartDate`, `EndDate` e `ExpiryDate` passam a ser
`nro_reuniao`, `data_inicio`, `data_decisao` e `data_efetividade`.
O início fica nulo quando não está disponível no ICS; não é inferido da decisão.

::: pyield.copom
    options:
      members:
        - calendario
        - proxima_reuniao
