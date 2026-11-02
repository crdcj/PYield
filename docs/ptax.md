# PTAX

`yd.ptax` e `yd.ptax_serie` consultam a cotação de venda do dólar no Banco
Central, em R$/US$. Os valores são convertidos diretamente do texto da fonte
para `Decimal`, com quatro casas decimais.

A consulta escalar retorna `Decimal("NaN")` se não houver cotação. A série
retorna um DataFrame com `data` (`pl.Date`) e `cotacao` (`pl.Decimal(18, 4)`),
preservando o esquema quando vazio.

```python
import pyield as yd

cotacao = yd.ptax("22-04-2025")
historico = yd.ptax_serie("22-04-2025", "25-04-2025")
```

Veja as [notas de migração da 0.62.0](release-0.62.0.md#indicadores-sgs-com-decimal)
para converter explicitamente os resultados para ponto flutuante.

::: pyield.ptax

::: pyield.ptax_serie
