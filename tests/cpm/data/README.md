Capturas brutas obtidas em 02/11/2026, preservadas byte a byte:

- `SPRD250129.zip`: GET
  `https://www.b3.com.br/pesquisapregao/download?filelist=SPRD250129.zip`.
- `consolidados_20250129.csv`: POST
  `https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR`, com o JSON:

  ```json
  {
    "Name": "ConsolidatedTradesDerivatives",
    "Date": "2025-01-29",
    "FinalDate": "2025-01-29",
    "ClientId": "",
    "Filters": {}
  }
  ```

O CSV contém apenas o cabeçalho: a B3 não mantém o preço de referência de
datas antigas nessa exportação. A comparação com `cpm_29012025.parquet`
verifica os demais campos; os preços históricos desse Parquet permanecem
como referência dos testes de probabilidades.
