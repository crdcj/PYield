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
projecao_ntnb = yd.vna.projetado("NTN-B", "30-06-2026", 4731.856412, "0.45%")
projecao_lft = yd.vna.projetado(
    "LFT", "18-09-2026", 19905.773236,
    selic="13.65%",
)
```

`projetado` é a operação comum de projeção para todos os títulos. Para NTN-B e
NTN-C, `inflacao` representa a variação mensal e aceita `float` ou `Decimal`
em unidade decimal (`0.0045`) ou string percentual (`"0.45%"` ou `"0,45%"`). Para LFT, `selic` representa
a taxa anual informada em formato
decimal ou percentual explícito. Para reproduzir o acruamento oficial, derive
a taxa Selic diária publicada pelo SGS 11. Como ela é publicada em percentual,
`0,050788%` equivale à taxa decimal `0,00050788` e ao fator derivado
`1,00050788`; converta essa taxa diária para uma taxa anual equivalente. A
série 1178 é a Selic Over anualizada e serve apenas como aproximação. A
data-base é o último dia útil anterior à data solicitada. A projeção usa a
quantidade de dias úteis até a data solicitada e retorna `Decimal`. Intervalos
diferentes de um dia útil são calculados normalmente e registrados no log.
Todos os cálculos de VNA, inclusive o genérico `calcular_vna`, usam `Decimal`
com 28 dígitos de precisão e retornam `Decimal`, para aplicar exatamente os
truncamentos e arredondamentos da metodologia (fator do IPCA T-16, pró-rata
T-14, projeção A-2 percentual, equivalente a A-4 decimal, e VNA T-6).
Entradas `float` são convertidas pela sua
representação textual, de modo que `4731.856412` vira `Decimal('4731.856412')`.

Por exemplo, o SGS 11 publicou `0,050788%` para 18/09/2026. Isso corresponde
ao fator diário `1,00050788`; aplicado ao VNA de 17/09/2026 (`19905,773236`),
produz `19915,882980111...`, truncado para `19915,882980`, exatamente o valor
publicado pelo BCB. A série 1178 publicou `13,65%` anualizado na mesma data;
usá-la diretamente produz uma pequena diferença por causa do arredondamento.

`ultimo` retorna a última referência de cada série; para NTN-C sem filtro de
vencimento, pode retornar mais de uma linha. Datas e valores permanecem juntos.
`historico` e `ultimo` retornam a coluna `vna` como `Decimal(18, 6)`,
preservando exatamente as seis casas publicadas pelo Tesouro Nacional. Uma
célula da planilha com data válida e valor ilegível gera erro, em vez de ser
descartada e alterar silenciosamente o pró-rata entre referências.
Não há projeção automática. A LFT não possui histórico mensal nem operação de
última referência nessa API; sua projeção usa a quantidade de dias úteis do
intervalo, base 252 e taxa anual constante dentro da mesma operação `projetado`.
Quando o intervalo não corresponde a exatamente um dia útil, o cálculo é feito
normalmente e a ocorrência é registrada no log. Em uma segunda-feira, por
exemplo, a base é a sexta-feira útil anterior.

Use apenas `yd.vna` para operações de VNA. Os aliases `vna`, `vnas`,
`vna_projetado` e `vigencia` foram removidos dos módulos dos títulos.
As implementações internas e suas metodologias foram preservadas.

## Cálculo do VNA a partir da origem

`yd.vna.calcular_base(titulo, indice_atual, vencimento=None)` calcula uma nova
base mensal da NTN-B ou NTN-C com o número-índice realizado do IPCA ou IGP-M,
respectivamente, ou o VNA diário da LFT com o fator Selic acumulado desde
01/07/2000. As bases originais e os índices iniciais são constantes internas.
O usuário fornece apenas o título e o índice ou fator novo; para NTN-C,
informa também o vencimento para selecionar a série.

A função não consulta a rede: pode ser usada assim que o IBGE ou a FGV divulgar
o índice, mesmo que o Tesouro ainda não tenha publicado o novo VNA. Aceita
`float` ou `Decimal` e retorna `Decimal` com seis casas. Para LFT, o fator
acumulado deve ser fornecido pronto; a função não busca nem acumula as taxas
diárias. Não há preenchimento automático nas consultas de VNA publicado.

O índice corresponde ao mês anterior à nova data-base. Para NTN-B, a referência
ocorre no dia 15; para NTN-C, no primeiro dia do mês. Os exemplos abaixo
conferem o cálculo com valores conhecidos publicados pelas fontes oficiais.

### NTN-B: base de 15/08/2026

```python
indice_atual = 7657.73  # Pode ser obtido com yd.ipca.indice("01-07-2026")
nova_base = yd.vna.calcular_base("NTN-B", indice_atual)  # 15/08/2026
# Decimal('4742.744422')
assert nova_base == yd.vna.valor("NTN-B", "15-08-2026")
```

### NTN-C: base de 01/10/2026, série de 2031

```python
indice_atual = 1225.846  # Pode ser obtido com yd.igpm.indice("01-09-2026")
nova_base = yd.vna.calcular_base(
    "NTN-C", indice_atual, vencimento="01-01-2031",  # 01/10/2026
)
# Decimal('6671.452284')
assert nova_base == yd.vna.valor("NTN-C", "01-10-2026", "01-01-2031")
```

Para NTN-C, o vencimento é obrigatório e a seleção usa seu ano. Os vencimentos
2005, 2008, 2011, 2017, 2021 e 2031 usam R$ 1.000 em 01/07/2000. Os de 2002 e
2006 incluem internamente o ajuste inicial da emissão de 03/12/1999, sem perda
de precisão intermediária. Um vencimento sem série conhecida retorna
`Decimal('NaN')`. Para NTN-B e LFT, omita o vencimento.

### LFT: VNA válido em 03/06/2024

O fator abaixo foi conferido com o produtório da Selic diária SGS 11 desde
01/07/2000 até 31/05/2024, inclusive. Ele já incorpora toda a acumulação;
uma taxa diária ou anual isolada não é essa entrada.

```python
fator_acumulado = 14.903011480282196
nova_base = yd.vna.calcular_base("LFT", fator_acumulado)
# Decimal('14903.011480')
assert nova_base == yd.vna.valor("LFT", "31-05-2024")
```

O arquivo APC238 gerado em 31/05/2024 informa o VNA válido para 03/06/2024,
próximo dia útil. A consulta `valor` recebe a data do arquivo; o fator acumulado
para uma data de vigência inclui as taxas até o dia útil anterior. A função
de cálculo não seleciona datas nem consulta relatórios automaticamente.

### Precisão e referências

Para NTN-B e NTN-C, a sequência é `T6(vna_base * T16(indice_atual / indice_base))`: trunca o
fator acumulado em dezesseis casas e o VNA em seis, com precisão interna de
28 dígitos. Não arredonda o fator nem usa a taxa mensal percentual arredondada.
O cálculo da base não tem pró-rata de dias nem arredondamento de projeção.
Para LFT, a sequência é `T6(1000 * A16(fator_acumulado))`: arredonda o fator
em dezesseis casas (meio para cima) e trunca o VNA em seis. A base original
de R$ 1.000 também fica embutida na biblioteca.

Usar o VNA do mês anterior já truncado pode perder uma unidade na sexta casa;
encadear essas bases acumula a perda. Por isso, a referência de partida para
reconstrução exata deve ser a original, selecionada pela biblioteca. Consulte
o registro da fórmula, da
validação histórica e da divergência identificada em fevereiro de 2014 na
[metodologia da STN](referencias/metodologia-calculo-tpf-stn.md#vna-base-mensal-da-ntn-b).
O valor calculado não substitui a consulta ao VNA publicado por `yd.vna.valor`.
O registro da reconstrução e das datas da LFT está na
[metodologia complementar](referencias/metodologia-calculo-letras-notas-stn.md#conferencia-da-reconstrucao-desde-a-origem).

## Interpolação e projeção

`calcular_vna` recebe um DataFrame porque trabalha sobre uma série de referências:
retorna o ponto publicado na data ou seleciona as duas referências que a cercam
e calcula o pró-rata. Não exige ordenação prévia nem usa necessariamente o último
VNA da série. Para NTN-C, filtre o vencimento para não misturar séries.

```python
referencias = yd.vna.historico("NTN-C", vencimento="01-01-2031")
valor = yd.vna.calcular_vna(referencias, "16-12-2025")
# Decimal('6449.641358')
```

O VNA de entrada pode ser `float`; não é necessário construir objetos `Decimal`.
A data da consulta aceita os formatos brasileiros `DD-MM-YYYY` e `DD/MM/YYYY`,
além de `YYYY-MM-DD` e objetos de data. No DataFrame, a coluna `data` deve ser
do tipo Polars `Date`, como já vem de `historico`. O retorno permanece `Decimal`.

O parâmetro opcional `fator_variacao` substitui a razão entre os dois VNAs,
mas ainda exige as duas referências para determinar o intervalo. É um fator
multiplicativo: `1.0045` representa alta de 0,45%. A função retorna
`Decimal('NaN')` fora do intervalo publicado, mesmo com esse fator informado.
Para NTN-B, prefira `yd.vna.valor` para reproduzir o valor oficial: ela usa os
números-índice do IPCA e os truncamentos próprios desse cálculo, em vez de
inferir o fator pela razão entre VNAs publicados.

Para projetar, use `projetado`, que já recebe um VNA-base escalar. Se a base vem
do histórico, selecione a referência do **início da vigência da data-alvo**.
Ela só coincide com o último VNA publicado quando essa última referência é
o início da vigência desejada. Por exemplo, em um cenário de inflação de 0,45%:

```python
data = "30-06-2026"
inicio, fim = yd.vna.vigencia("NTN-B", data)
referencias = yd.vna.historico("NTN-B")
base = referencias.filter(pl.col("data") == inicio).item(0, "vna")
valor = yd.vna.projetado("NTN-B", data, base, "0.45%")
# Decimal('4742.491138')
```

O exemplo pressupõe que a referência inicial está disponível no histórico.
Se o VNA da próxima referência já tiver sido publicado, os dias anteriores a
ela continuam pertencendo à vigência anterior. Para calcular esses dias com
a inflação realizada, use `yd.vna.valor`; na NTN-B, ela consulta os índices
do IPCA. `projetado` aplica as regras de arredondamento da inflação projetada
e não substitui esse cálculo realizado a partir dos números-índice.

## Migração das entradas de inflação

O parâmetro `inflacao` de NTN-B e NTN-C em `yd.vna.projetado` recebia
números em percentual. Substitua `inflacao=0.45` por `inflacao="0.45%"`
ou `inflacao=0.0045` para manter a inflação mensal de 0,45%. Pelo contrato
atual, o número puro `0.45` representa 45%. Strings sem `%`, como `"0.45"`,
são rejeitadas. Os resultados e as regras de arredondamento do cálculo
permanecem iguais quando se informa a mesma inflação econômica.

## Contexto da vigência

`yd.vna.contexto` reúne os parâmetros disponíveis para uma vigência de NTN-B
ou NTN-C em um DataFrame com uma linha. Inclui as datas da vigência, os VNAs
publicados no início e no fim, a competência da inflação, sua origem e
situação (`realizado`, `projetado` ou `indisponivel`) e uma lista de pendências.
Dados numéricos indisponíveis são nulos. Para NTN-C, informe `vencimento`.

```python
contexto = yd.vna.contexto("NTN-B", "30-06-2026")
contexto_ntnc = yd.vna.contexto(
    "NTN-C", "15-06-2026", vencimento="01-01-2031",
)
```

A projeção opcional `inflacao_projetada` só é usada quando a taxa realizada
estiver indisponível. Esse parâmetro aceita as mesmas entradas de
`yd.vna.projetado`: `0.0045`, `Decimal("0.0045")`, `"0.45%"` ou `"0,45%"`
representam 0,45%. As entradas são convertidas para `Decimal` sem
arredondamento; a coluna `inflacao` permanece em unidade decimal e pode ser
passada diretamente a `yd.vna.projetado`. O contexto não calcula o VNA nem busca projeções automaticamente.
Ele reflete os dados disponíveis no momento da consulta, sem reconstruir o
que era conhecido na data solicitada. Para comparar operação e liquidação,
consulte cada data e compare `inicio_vigencia`.

### Transição e atenção à divulgação

Cada data seleciona sua própria base, inclusive quando um fim de semana separa
operação e liquidação:

```python
d0 = yd.vna.contexto("NTN-B", "14-08-2026")
d1 = yd.vna.contexto("NTN-B", "17-08-2026")
mudou_vigencia = d0.item(0, "inicio_vigencia") != d1.item(0, "inicio_vigencia")
```

Os calendários permitem identificar projeções que merecem revisão, sem presumir
que o índice já foi publicado. Por exemplo, para NTN-C:

```python
data_operacao = yd.hoje()
contexto = yd.vna.contexto(
    "NTN-C", data_operacao, vencimento="01-01-2031",
    inflacao_projetada="0,45%",
)
agenda = yd.igpm.calendario(contexto.item(0, "competencia").year)
diagnostico = contexto.join(agenda, on="competencia", how="left").with_columns(
    revisar_projecao=(
        (pl.col("situacao_inflacao") == "projetado")
        & (pl.col("data_divulgacao") <= data_operacao)
    ).fill_null(False)
)
```

Compare a divulgação com a **data de operação**, mesmo quando o contexto descreve
a liquidação. D+1 não antecipa a publicação. Sem data oficial, o exemplo não
inventa um alerta. Mensagens, cores, janelas de proximidade e a decisão de impedir
uma precificação permanecem na aplicação. A inflação realizada pode estar
disponível antes do VNA final da vigência.

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
