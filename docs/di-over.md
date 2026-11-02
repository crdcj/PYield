# DI Over

`yd.di_over` consulta a taxa DI Over da B3/CETIP republicada pelo Banco Central.
A consulta usa a API HTTPS do SGS, substituindo o FTP da CETIP.

O retorno é `Decimal` com dez casas decimais, em unidade decimal. Por padrão,
a taxa é anualizada em base 252 dias úteis; com `anualizada=False`, retorna
a taxa ao dia útil publicada pela fonte. As duas modalidades têm observações
de frequência diária e usam diretamente os valores das respectivas séries,
sem recalcular uma modalidade a partir da outra.

```python
import pyield as yd

taxa_anual = yd.di_over("28-02-2025")
taxa_diaria = yd.di_over("28-02-2025", anualizada=False)
```

Datas sem dados retornam `Decimal("NaN")`. O histórico começa em 06/03/1986,
ampliando a cobertura anterior do FTP, limitada a 20/08/2012. A docstring
abaixo identifica as séries de origem. Veja as
[notas de migração da 0.62.0](release-0.62.0.md#indicadores-sgs-com-decimal)
para converter explicitamente os resultados para ponto flutuante.

::: pyield.di_over
