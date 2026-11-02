# Selic

Este namespace reúne os indicadores diretamente relacionados à taxa Selic.
Os contratos CPM e suas probabilidades implícitas estão documentados
separadamente em [CPM](cpm.md).

`over` e `over_serie` retornam a taxa anualizada em base 252 por padrão.
Com `anualizada=False`, consultam a taxa ao dia útil publicada pelo Banco
Central. A frequência das observações é diária nas duas modalidades;
`meta` e `meta_serie` retornam a meta anual definida pelo Copom.

As consultas escalares retornam `Decimal`; as séries têm colunas `data`
(`pl.Date`) e `taxa` (`pl.Decimal(18, 10)`). Valores percentuais são convertidos
diretamente do texto para unidade decimal, sem passagem por `float`.
Escalares sem dados retornam `Decimal("NaN")`; séries sem dados retornam
DataFrame vazio com o mesmo esquema.

```python
import pyield as yd

taxa_anual = yd.selic.over("28-02-2025")
taxa_diaria = yd.selic.over("28-02-2025", anualizada=False)
serie_diaria = yd.selic.over_serie(ultimos=5, anualizada=False)
meta = yd.selic.meta("28-02-2025")
```

As docstrings abaixo identificam as séries de origem. Veja as
[notas de migração da 0.62.0](release-0.62.0.md#indicadores-sgs-com-decimal)
para converter explicitamente os resultados para ponto flutuante.

::: pyield.selic
    options:
      members:
        - over
        - over_serie
        - meta
        - meta_serie
