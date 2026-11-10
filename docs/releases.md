# Notas de versão

Cada versão tem uma página com as mudanças, o racional das decisões e as
orientações de migração. Consulte também as
[releases do GitHub](https://github.com/crdcj/PYield/releases) para as publicações.

| Versão | Principais mudanças |
|---|---|
| [0.64.0](release-0.64.0.md) | Fonte explícita em `yd.tpf.leiloes` (`fonte="tn"` ou `fonte="bc"`) e remoção de `yd.tpf.leiloes_bcb`; taxas de `yd.tpf.secundario.mensal` em decimal; `nome_comercial` obrigatório em `yd.ntnb1.dv01`; erro para prazos conflitantes na curva zero e no interpolador; duration sem truncamento; ofertas de compra e venda de `yd.futuro.historico` na convenção da B3; correções em prêmio limpo, curvas zero e leilões. |
| [0.63.0](release-0.63.0.md) | Escalas decimais por modalidade, série histórica de DI Over, números-índice no contexto de VNA e índices do IGP-M desde agosto de 1994. |
| [0.62.0](release-0.62.0.md) | Consultas externas sem cache, remoção de `atualizar`, retry leve, indicadores SGS com `Decimal`, taxas Over diárias e Polars 2.0 como mínimo. |
| [0.61.0](release-0.61.0.md) | Calendário de pregões da B3 e exposição de `futuro.dv01_expr`. |
| [0.60.1](release-0.60.1.md) | Correção de consultas de um dia no SGS. |
| [0.60.0](release-0.60.0.md) | Contratos com `Decimal` e migração da API. |
| [0.59.0](release-0.59.0.md) | Taxas e DV01 analíticos; taxas retornam `float`. |
| [0.58.2](release-0.58.2.md) | Documentação e avisos no cálculo interno de projeção da LFT. |
| [0.58.1](release-0.58.1.md) | DV01 sem PU de referência, projeção da LFT e curvas zero de NTN-C. |
| [0.58.0](release-0.58.0.md) | Cotações em base 100 e taxas percentuais explícitas. |

## Sobre a recuperação do histórico

As notas de `0.58.0`, `0.58.1`, `0.58.2`, `0.59.0` e `0.60.1` foram
reconstituídas dos READMEs e diffs locais, com links para os commits usados.
O repositório local não contém tags dessas versões: os limites foram definidos
pelos incrementos de versão no `pyproject.toml`. Não foram inferidas datas de
publicação nem reconstruído um changelog completo.

O quadro de migração acumulada desde a `0.56.0`, que constava no README da
`0.58.0`, foi preservado naquela nota com seu escopo original. As orientações
históricas devem ser lidas em sequência com as notas seguintes para migrar até
a API atual.
