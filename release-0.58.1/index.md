# Novidades na versão 0.58.1

Nota reconstituída a partir do histórico Git, usando o [commit de incremento da versão](<https://github.com/crdcj/PYield/commit/d6d61658>) e o [diff desde o incremento anterior](<https://github.com/crdcj/PYield/compare/dbcec48a...d6d61658>). O recorte é definido pelos commits de versão, não por tags de publicação.

## DV01 de LTN e NTN-F

`yd.ltn.dv01`, `yd.ntnf.dv01` e suas versões `dv01_expr` deixam de receber o argumento `pu`. O cálculo passa a usar a diferença entre dois preços teóricos: o preço na taxa informada e o preço com aumento de um ponto-base.

Para migrar, remova o argumento de PU dessas chamadas. Esse ajuste também afeta expressões Polars que encaminhavam uma coluna de PU para `dv01_expr`.

## Projeção de VNA da LFT

`yd.vna.projetado` passa a aceitar o argumento exclusivo por nome `selic` para LFT. A taxa deve ser informada nesse argumento; usar `inflacao` para LFT gera `ValueError`. A taxa não é buscada automaticamente. NTN-B e NTN-C continuam usando `inflacao`, com o contrato de unidade vigente naquela versão.

A documentação distingue a taxa diária de Selic da taxa anual equivalente exigida pela projeção. Para migrar uma projeção de LFT, passe a taxa anual em `selic=`, em formato decimal ou percentual explícito.

## Precificação por curva zero

NTN-C ganha `yd.ntnc.cotacao_curva_zero` e `yd.ntnc.taxa_curva_zero`, com desconto dos fluxos pela curva zero. `yd.ntnbp.taxa` deixa de arredondar a taxa interpolada em quatro casas: passa a retornar o valor em formato decimal sem arredondamento. Arredondamento comercial e spreads ficam a cargo do consumidor.

Fontes adicionais: commits de [DV01](<https://github.com/crdcj/PYield/commit/7c2daa5f>) e [curvas e projeção de VNA](<https://github.com/crdcj/PYield/commit/aa9a9759>).
