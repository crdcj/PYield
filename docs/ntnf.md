## Comparação experimental de curvas zero

`yd.ntnf.taxas_zero_forwards` constrói uma curva conjunta de LTN e NTN-F,
com forwards constantes entre vértices. A LTN tem prioridade somente quando
os vencimentos coincidem. Uma NTN-F entre duas LTNs participa da calibração.
O método existente `yd.ntnf.taxas_zero` e `yd.tpf.curva_pre` permanecem inalterados.

```python
import polars as pl

import pyield as yd

ltn = yd.ltn.dados("04-09-2026")
ntnf = yd.ntnf.dados("04-09-2026")
entradas = dict(
    data_liquidacao="04-09-2026",
    vencimentos_ltn=ltn["data_vencimento"],
    taxas_ltn=ltn["taxa_indicativa"],
    vencimentos_ntnf=ntnf["data_vencimento"],
    taxas_ntnf=ntnf["taxa_indicativa"],
)
antiga = yd.ntnf.taxas_zero(**entradas)
proposta = yd.ntnf.taxas_zero_forwards(**entradas)
comparacao = antiga.join(
    proposta, on=["data_vencimento", "dias_uteis"], suffix="_proposta"
).with_columns(
    diferenca_pb=(pl.col("taxa_zero_proposta") - pl.col("taxa_zero")) * 10_000
)
```

A proposta retorna todos os vencimentos selecionados, inclusive os de LTN.
Preserve todos esses vértices ao interpolar por `flat_forward`: filtrar apenas
as datas de NTN-F pode mudar os descontos intermediários. O preço-alvo da
calibração não aplica arredondamentos ou truncamentos, como no bootstrap da NTN-B;
por isso, pode diferir ligeiramente do PU oficial.

::: pyield.tpf.ntnf
