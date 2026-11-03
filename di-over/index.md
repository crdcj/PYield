# DI Over

`yd.di_over` consulta a taxa DI Over da B3/CETIP para uma data; `yd.di_over_serie` consulta um intervalo ou os últimos registros disponíveis. As consultas usam a API HTTPS do SGS do Banco Central, substituindo o FTP da CETIP.

O retorno é `Decimal`, em unidade decimal. Por padrão, a taxa é anualizada em base 252 dias úteis e tem quatro casas decimais; com `anualizada=False`, retorna a taxa ao dia útil com oito casas decimais. Essas escalas correspondem às duas e seis casas percentuais publicadas pela fonte. As modalidades têm observações de frequência diária e usam diretamente os valores das respectivas séries, sem recalcular uma modalidade a partir da outra.

```python
import pyield as yd

taxa_anual = yd.di_over("28-02-2025")
taxa_diaria = yd.di_over("28-02-2025", anualizada=False)
serie_anual = yd.di_over_serie("27-02-2025", "28-02-2025")
serie_diaria = yd.di_over_serie("27-02-2025", "28-02-2025", anualizada=False)
ultimas_taxas = yd.di_over_serie(ultimos=5)
```

As séries retornam um DataFrame Polars com colunas `data` (`Date`) e `taxa` (`Decimal(18, 4)` na anual ou `Decimal(18, 8)` na diária). O intervalo inclui as datas inicial e final; se `fim` for omitido, considera a data atual no Brasil. `ultimos` tem prioridade sobre o intervalo. Datas sem observações são omitidas; sem dados, o DataFrame é vazio e preserva o esquema. Intervalos longos são divididos automaticamente em blocos seguros para o SGS, dispensando consultas individuais por data.

As escalas foram ajustadas e `di_over_serie` foi adicionada na [0.63.0](<https://crdcj.github.io/PYield/release-0.63.0/index.md>). Na consulta escalar, datas sem dados retornam `Decimal("NaN")`. O histórico começa em 06/03/1986, ampliando a cobertura anterior do FTP, limitada a 20/08/2012. A docstring abaixo identifica as séries de origem. Veja as [notas de migração da 0.62.0](<https://crdcj.github.io/PYield/release-0.62.0/#indicadores-sgs-com-decimal>) para converter explicitamente os resultados para ponto flutuante.

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
| `Decimal` | `Decimal` | Taxa DI em formato decimal com quatro casas decimais ao ano se |
|  | `Decimal` | `anualizada=True` (0.1315 = 13,15% a.a.) ou oito casas ao dia útil se |
|  | `Decimal` | `anualizada=False`. Retorna `Decimal('NaN')` se não houver dado |
|  | `Decimal` | para a data, incluindo feriados e fins de semana. |

**Notes**

As duas séries têm frequência diária. Por padrão, os valores são taxas anuais em base 252 dias úteis. O percentual informado pela série escolhida é convertido diretamente do texto para Decimal e dividido por 100, sem passagem por float, com quatro casas decimais na taxa anual e oito na diária. As escalas preservam as duas casas percentuais anuais e as seis diárias, sem recalcular uma modalidade a partir da outra. O histórico começa em 06/03/1986; a consulta anterior via FTP estava limitada a datas a partir de 20/08/2012.

Examples:

```pycon
>>> import pyield as yd
>>> taxa = yd.di_over("28/02/2025")  # decimal (0.1315 = 13,15% a.a.)
>>> taxa
Decimal('0.1315')
>>> f"{taxa:.2%}"
'13.15%'
```

Taxa ao dia útil para a mesma data, com oito casas decimais:

```pycon
>>> yd.di_over("28/02/2025", anualizada=False)
Decimal('0.00049037')
```

```pycon
>>> yd.di_over("01/01/2025")  # Feriado
Decimal('NaN')
```

Obtém a série de taxas DI Over via SGS do Banco Central.

Consulta a taxa DI (Depósito Interfinanceiro) da B3/CETIP, republicada pelo Banco Central na série SGS 4389 (anualizada em base 252) ou SGS 12 (diária).

Parameters:

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `inicio` | `DateLike \| None` | Data inicial do intervalo, inclusive. | `None` |
| `fim` | `DateLike \| None` | Data final do intervalo, inclusive. Se `None`, usa a data atual no Brasil. | `None` |
| `ultimos` | `int \| None` | Número de registros mais recentes a retornar. Se informado, tem prioridade sobre `inicio` e `fim`. | `None` |
| `anualizada` | `bool` | Se `True`, retorna a taxa ao ano, em base 252 dias úteis. Se `False`, retorna a taxa ao dia útil publicada pela fonte. | `True` |

Returns:

| Type | Description |
| --- | --- |
| `DataFrame` | DataFrame com as taxas em decimal, ou vazio se não houver dados. |

**Output Columns**

data (pl.Date): Data de referência da observação. taxa (pl.Decimal(18, 4) ou pl.Decimal(18, 8)): Taxa em decimal, ao ano com escala 4 se `anualizada=True` ou ao dia útil com escala 8 se `anualizada=False`.

**Notes**

A frequência das observações é diária nas duas modalidades. Os valores percentuais da série escolhida são convertidos diretamente do texto para Decimal e divididos por 100, sem passagem por float e sem recalcular uma modalidade a partir da outra. As escalas preservam as duas casas percentuais das taxas anuais e as seis das taxas diárias. O histórico começa em 06/03/1986. Datas sem observações são omitidas. Intervalos longos são divididos automaticamente em blocos seguros para consulta ao SGS.

Raises:

| Type | Description |
| --- | --- |
| `ValueError` | Se não informar `inicio` nem `ultimos`. |

Examples:

```pycon
>>> yd.di_over_serie("27-02-2025", "28-02-2025")
shape: (2, 2)
┌────────────┬───────────────┐
│ data       ┆ taxa          │
│ ---        ┆ ---           │
│ date       ┆ decimal[18,4] │
╞════════════╪═══════════════╡
│ 2025-02-27 ┆ 0.1315        │
│ 2025-02-28 ┆ 0.1315        │
└────────────┴───────────────┘
```

Taxas ao dia útil para o mesmo intervalo, com oito casas decimais:

```pycon
>>> yd.di_over_serie("27-02-2025", "28-02-2025", anualizada=False)
shape: (2, 2)
┌────────────┬───────────────┐
│ data       ┆ taxa          │
│ ---        ┆ ---           │
│ date       ┆ decimal[18,8] │
╞════════════╪═══════════════╡
│ 2025-02-27 ┆ 0.00049037    │
│ 2025-02-28 ┆ 0.00049037    │
└────────────┴───────────────┘
```
