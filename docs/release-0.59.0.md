# Novidades na versão 0.59.0

Nota reconstituída a partir do histórico Git, usando o [commit de incremento da versão](https://github.com/crdcj/PYield/commit/07a2f4ed) e o [diff desde o incremento anterior](https://github.com/crdcj/PYield/compare/7a535165...07a2f4ed). O recorte é definido pelos commits de versão, não por tags de publicação.

## Taxas e DV01 analíticos

As funções `taxa` de `yd.ltn`, `yd.ntnf`, `yd.ntnb`, `yd.ntnc` e `yd.lft`
passam a retornar `float` sem truncamento, em vez de `Decimal` truncado em oito
casas. Entradas inválidas retornam `float("nan")`; use `math.isnan` no lugar de
`.is_nan()`.

`taxa` e `dv01` de todos os títulos descontam os fluxos com `dias úteis / 252`,
sem os truncamentos intermediários da STN. Os valores mudam levemente em
relação à versão anterior, e `taxa(pu(...))` recupera a taxa original apenas
aproximadamente. `pu` e `cotacao` seguem a metodologia oficial e não mudaram.

## Migração

Revise verificações de valores ausentes e operações que pressupõem `Decimal`
no retorno de `taxa`. Use `math.isnan(resultado)` e converta explicitamente o
valor quando sua aplicação exigir `Decimal`. Uma conversão posterior não
recupera a precisão decimal anterior.

Comparações que dependiam do truncamento dos resultados de taxa e DV01 devem
ser revistas. Os tipos de retorno de `pu` e `cotacao` foram preservados.
