# Valor nominal atualizado

As funções `yd.vna.vigencia("NTN-B", data)` e `yd.vna.vigencia("NTN-C", data)` retornam o
início inclusivo e o fim exclusivo da vigência do VNA, sem consultar a rede.
Essas datas permitem selecionar a base e a projeção mensal na aplicação,
inclusive quando operação e liquidação pertencem a vigências diferentes.

- NTN-B: dia 15 até o dia 15 seguinte.
- NTN-C: primeiro dia do mês até o primeiro dia do mês seguinte.

A aplicação fornece o VNA ao calcular o preço completo dos títulos indexados:

```python
cotacao = yd.ntnb.cotacao("14-08-2026", "15-08-2026", 0.132098)
pu = yd.ntnb.pu(4742.6373, cotacao)
# Decimal('4880.439369')
```

A seleção do VNA oficial ou projetado permanece explícita no consumidor.

## Consulta por título

```python
import pyield as yd

ultimo = yd.vna.ultimo("NTN-B")  # DataFrame com data e VNA publicados
historico = yd.vna.historico("NTN-C", vencimento="01-01-2031")
valor = yd.vna.valor("NTN-B", "15-12-2025")
projecao_ntnb = yd.vna.projetado("NTN-B", "30-06-2026", 4731.856412, 0.45)
projecao_lft = yd.vna.projetado(
    "LFT", "18-09-2026", 19905.773236,
    selic="13.65%",
)
```

`projetado` é a operação comum de projeção para todos os títulos. Para NTN-B e
NTN-C, `inflacao` representa a variação mensal em percentual e aceita `float` ou
`Decimal`. Para LFT, `selic` representa a taxa anual informada em formato
decimal ou percentual explícito. Para reproduzir o acruamento oficial, derive
a taxa Selic diária publicada pelo SGS 11. Como ela é publicada em percentual,
`0,050788%` equivale à taxa decimal `0,00050788` e ao fator derivado
`1,00050788`; converta essa taxa diária para uma taxa anual equivalente. A
série 1178 é a Selic Over anualizada e serve apenas como aproximação. A
data-base é o último dia útil anterior à data solicitada e a projeção avança
exatamente um dia útil. A função retorna `Decimal`.
`historico` e `ultimo` retornam DataFrames com coluna `vna` do tipo `Float64`.
O cálculo genérico `calcular_vna` mantém seu retorno `float` existente.

Por exemplo, o SGS 11 publicou `0,050788%` para 18/09/2026. Isso corresponde
ao fator diário `1,00050788`; aplicado ao VNA de 17/09/2026 (`19905,773236`),
produz `19915,882980111...`, truncado para `19915,882980`, exatamente o valor
publicado pelo BCB. A série 1178 publicou `13,65%` anualizado na mesma data;
usá-la diretamente produz uma pequena diferença por causa do arredondamento.

`ultimo` retorna a última referência de cada série; para NTN-C sem filtro de
vencimento, pode retornar mais de uma linha. Datas e valores permanecem juntos.
Não há projeção automática. A LFT não possui histórico mensal nem operação de
última referência nessa API; sua projeção usa a quantidade de dias úteis do
intervalo, base 252 e taxa anual constante dentro da mesma operação `projetado`.
Quando o intervalo não corresponde a exatamente um dia útil, o cálculo é feito
normalmente e a ocorrência é registrada no log. Em uma segunda-feira, por
exemplo, a base é a sexta-feira útil anterior.

Use apenas `yd.vna` para operações de VNA. Os aliases `vna`, `vnas`,
`vna_projetado` e `vigencia` foram removidos dos módulos dos títulos.
As implementações internas e suas metodologias foram preservadas.

::: pyield.vna

## Atualização manual

As consultas `valor`, `historico` e `ultimo` aceitam `atualizar=True`:

```python
historico = yd.vna.historico("NTN-C", vencimento="01-01-2031", atualizar=True)
ultimo = yd.vna.ultimo("NTN-B", atualizar=True)
valor = yd.vna.valor("LFT", "31-05-2024", atualizar=True)
```

A opção ignora e renova o cache interno de 60 segundos do arquivo de VNA.
Chamadas seguintes reutilizam o download renovado. Para NTN-B e NTN-C, o cache
é por planilha do título, compartilhado entre datas e séries de vencimentos;
para LFT, é por data de referência. Erros de download são propagados; a entrada
anterior permanece disponível somente até sua expiração original.

O padrão continua sendo `atualizar=False`. A opção não invalida caches da
aplicação nem o cache dos índices do IPCA usados no pró-rata da NTN-B.
A coluna `data` representa a referência do VNA, não o horário do download ou
uma nova publicação. Um novo download pode retornar os mesmos dados.
