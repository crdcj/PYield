As funções `yd.ntnc.cotacao_curva_zero` e `yd.ntnc.taxa_curva_zero` recebem
uma curva fornecida pelo consumidor, com colunas `dias_uteis` e `taxa_zero`.
A primeira retorna a cotação em base 100 sem truncamento final; a segunda,
a TIR equivalente como `float`, sem arredondamento comercial. A biblioteca
não escolhe a curva de referência nem aplica piso de taxa ANBIMA.

::: pyield.ntnc
