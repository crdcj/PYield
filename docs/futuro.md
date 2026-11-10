# Futuro

## Compra e venda em contratos de taxa

Contratos como DI1 e DAP são negociados em taxa. Na convenção da B3, o que se
compra e se vende é a própria taxa:

- **comprar DI1 é comprar taxa**;
- **vender DI1 é vender taxa**.

É por isso que as ofertas de compra e venda têm a ordem descrita abaixo, o
inverso da intuição de quem vem dos títulos públicos.

| Operação | Jargão | Equivale a | Ganha quando a taxa |
|---|---|---|---|
| Comprar DI1 (comprar taxa) | ficar **tomado**, tomar taxa | vender PU | sobe |
| Vender DI1 (vender taxa) | ficar **aplicado**, dar taxa | comprar PU | cai |

Quem toma taxa quer pagar barato, por isso oferta a taxa mais baixa. Quem dá
taxa quer receber caro, por isso oferta a taxa mais alta. Assim, no livro de
ofertas de um contrato de taxa:

- a **oferta de compra** (bid) é a de quem quer ficar tomado e tem a taxa
  **menor**;
- a **oferta de venda** (ask) é a de quem quer ficar aplicado e tem a taxa
  **maior**.

Quem toma taxa a mercado fecha negócio na oferta de venda, pagando a taxa
maior. Quem dá taxa a mercado fecha na oferta de compra.

Nos títulos públicos a lógica é a oposta, porque o título é negociado em PU.
Comprar uma LTN é comprar PU: quem compra quer PU menor, ou seja, taxa
maior. A posição comprada no título é aplicada, como a posição vendida no
DI1. Por isso, quem tem LTN em carteira se protege da alta de juros comprando
DI1 (ficando tomado).

O ajuste diário segue a mesma equivalência: para calcular o ajuste, a B3
converte a taxa em PU, e a posição comprada em taxa é tratada como vendida em
PU.

Na biblioteca, as colunas mantêm os nomes da B3. Em `yd.futuro.historico`
(`taxa_ultima_oferta_compra` e `taxa_ultima_oferta_venda`) e em
`yd.futuro.intradia` (`taxa_oferta_compra` e `taxa_oferta_venda`), compra é
compra de taxa. Logo, a taxa de compra fica abaixo da de venda. Os contratos
cotados por preço, como DOL, WDO, IND e WIN, seguem a convenção usual: compra
abaixo de venda, em preço.

::: pyield.futuro
