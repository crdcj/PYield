# Novidades na versão 0.61.0

Esta seção cobre apenas as mudanças da `0.60.0` para a `0.61.0`, que não removem nem alteram APIs existentes.

- Calendário de pregões da B3 em `yd.b3`: `eh_pregao(data)`, `pregao_anterior(data)`, `proximo_pregao(data)` e `ultimo_pregao(data=None)`. Há pregão em todo dia útil brasileiro, exceto em 24/12 e 31/12. Fechamentos excepcionais não recorrentes não são modelados.
- `yd.futuro.dv01_expr(coluna_dias_uteis, coluna_taxa, coluna_preco)` passa a ser público e retorna uma `pl.Expr` com o DV01 dos contratos futuros.
