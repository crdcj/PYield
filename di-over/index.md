# DI Over

`yd.di_over` consulta a taxa DI Over da B3/CETIP republicada pelo Banco Central. A consulta usa a API HTTPS do SGS, substituindo o FTP da CETIP.

O retorno é `Decimal` com dez casas decimais, em unidade decimal. Por padrão, a taxa é anualizada em base 252 dias úteis; com `anualizada=False`, retorna a taxa ao dia útil publicada pela fonte. As duas modalidades têm observações de frequência diária e usam diretamente os valores das respectivas séries, sem recalcular uma modalidade a partir da outra.

```python
import pyield as yd

taxa_anual = yd.di_over("28-02-2025")
taxa_diaria = yd.di_over("28-02-2025", anualizada=False)
```

Datas sem dados retornam `Decimal("NaN")`. O histórico começa em 06/03/1986, ampliando a cobertura anterior do FTP, limitada a 20/08/2012. A docstring abaixo identifica as séries de origem. Veja as [notas de migração da 0.62.0](<https://crdcj.github.io/PYield/release-0.62.0/#indicadores-sgs-com-decimal>) para converter explicitamente os resultados para ponto flutuante.

Obtém a taxa DI over anualizada ou diária via SGS do Banco Central.

Consulta a taxa DI (Depósito Interfinanceiro) da B3/CETIP, republicada pelo Banco Central na série SGS 4389 (anualizada em base 252) ou SGS 12 (diária), para a data informada. Use `pyield.di_over` na API pública principal.

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `data` | `DateLike` | data da consulta para buscar a taxa DI. | *required* |
| `anualizada` | `bool` | Se `True`, retorna a taxa ao ano, em base 252 dias úteis. Se `False`, retorna a taxa ao dia útil publicada pela fonte. | `True` |

Returns:

| Name | Type | Description |
| --- | --- | --- |
| `Decimal` | `Decimal` | Taxa DI em formato decimal com dez casas decimais, ao ano se |
|  | `Decimal` | `anualizada=True` (0.1315 = 13,15% a.a.) ou ao dia útil se |
|  | `Decimal` | `anualizada=False`. Retorna `Decimal('NaN')` se não houver dado |
|  | `Decimal` | para a data, incluindo feriados e fins de semana. |

**Notes**

As duas séries têm frequência diária. Por padrão, os valores são taxas anuais em base 252 dias úteis. O percentual informado pela série escolhida é convertido diretamente do texto para Decimal e dividido por 100, sem passagem por float, com dez casas decimais. Essa escala preserva até oito casas decimais do percentual publicado, sem recalcular uma modalidade a partir da outra. O histórico começa em 06/03/1986; a consulta anterior via FTP estava limitada a datas a partir de 20/08/2012.

Examples:

```pycon
>>> import pyield as yd
>>> taxa = yd.di_over("28/02/2025")  # decimal (0.1315 = 13,15% a.a.)
>>> f"{taxa:.2%}"
'13.15%'
>>> yd.di_over("01/01/2025")  # Feriado
Decimal('NaN')
```
