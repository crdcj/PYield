# DI Over

`yd.di_over` consulta a taxa DI Over da B3/CETIP para uma data;
`yd.di_over_serie` consulta um intervalo ou os últimos registros disponíveis.
As consultas usam a API HTTPS do SGS do Banco Central, substituindo o FTP da CETIP.

O retorno é `Decimal`, em unidade decimal. Por padrão, a taxa é anualizada
em base 252 dias úteis e tem quatro casas decimais; com `anualizada=False`,
retorna a taxa ao dia útil com oito casas decimais. Essas escalas correspondem
às duas e seis casas percentuais publicadas pela fonte. As modalidades têm
observações de frequência diária e usam diretamente os valores das respectivas séries,
sem recalcular uma modalidade a partir da outra.

```python
import pyield as yd

taxa_anual = yd.di_over("28-02-2025")
taxa_diaria = yd.di_over("28-02-2025", anualizada=False)
serie_anual = yd.di_over_serie("27-02-2025", "28-02-2025")
serie_diaria = yd.di_over_serie("27-02-2025", "28-02-2025", anualizada=False)
ultimas_taxas = yd.di_over_serie(ultimos=5)
```

As séries retornam um DataFrame Polars com colunas `data` (`Date`) e `taxa`
(`Decimal(18, 4)` na anual ou `Decimal(18, 8)` na diária).
O intervalo inclui as datas inicial e final; se `fim` for omitido, considera
a data atual no Brasil. `ultimos` tem prioridade sobre
o intervalo. Datas sem observações são omitidas; sem dados, o DataFrame é
vazio e preserva o esquema. Intervalos longos são divididos automaticamente
em blocos seguros para o SGS, dispensando consultas individuais por data.

As escalas foram ajustadas e `di_over_serie` foi adicionada na
[0.63.0](release-0.63.0.md).
Na consulta escalar, datas sem dados retornam `Decimal("NaN")`.
O histórico começa em 06/03/1986,
ampliando a cobertura anterior do FTP, limitada a 20/08/2012. A docstring
abaixo identifica as séries de origem. Veja as
[notas de migração da 0.62.0](release-0.62.0.md#indicadores-sgs-com-decimal)
para converter explicitamente os resultados para ponto flutuante.

::: pyield.di_over

::: pyield.di_over_serie
