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

- `consolidados_20261030.csv`: o mesmo POST, capturado em 11/11/2026, com
  `"Date"` e `"FinalDate"` iguais a `"2026-10-30"`. Contém os preços de
  referência e reproduz a falha de inferência de tipo em `Preço de abertura`
  (inteiros nas primeiras linhas e `27,68` na linha 187).

O CSV de 29/01/2025 contém apenas o cabeçalho: a B3 não mantém o preço de referência de
datas antigas nessa exportação. A comparação com `cpm_29012025.parquet`
verifica os demais campos; os preços históricos desse Parquet permanecem
como referência dos testes de probabilidades.
