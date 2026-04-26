# Títulos Públicos Federais

Módulos de precificação e análise por tipo de título. Cada módulo expõe funções
para cotação, duration, prêmio e dados indicativos da ANBIMA.

Para consultas de mercado (taxas indicativas, vencimentos disponíveis, estoque,
negociações secundárias, leilões e benchmarks), use o módulo
[`tpf`](../tpf.md).

## Títulos Disponíveis

| Título | Indexador | Módulo |
|--------|-----------|--------|
| LFT | Selic | [`lft`](lft.md) |
| LTN | Prefixado | [`ltn`](ltn.md) |
| NTN-F | Prefixado + cupom semestral | [`ntnf`](ntnf.md) |
| NTN-B | IPCA + cupom semestral | [`ntnb`](ntnb.md) |
| NTN-B Principal | IPCA (sem cupom) | [`ntnbp`](ntnbp.md) |
| NTN-B1 | IPCA + cupom (sem recompra) | [`ntnb1`](ntnb1.md) |
| NTN-C | IGP-M + cupom semestral | [`ntnc`](ntnc.md) |
| PRE | Prefixado genérico | [`pre`](pre.md) |

## Relatório Mensal da Dívida

O RMD consolida saldos e emissões da dívida pública federal. Ver [`tpf.rmd`](rmd.md).