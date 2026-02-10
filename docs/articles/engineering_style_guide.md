# Guia de Referência de Engenharia (PYield)

Este guia define padrões de arquitetura para decisões recorrentes no projeto.
Objetivo principal: **reduzir complexidade** sem perder clareza de contrato.

## 1) Fronteira de API Pública

Use esta regra como fonte de verdade:

- Público: o que está documentado e/ou reexportado na API de topo (`pyield/__init__.py`).
- Interno: módulos auxiliares fora da API de topo, mesmo que importáveis por caminho direto.

Princípios:

- Não promover utilitário interno para público sem necessidade real de usuário final.
- Evitar “vazamento acidental” de API por conveniência de import.
- Toda adição pública deve ter compromisso de estabilidade e depreciação.

## 2) Organização de Internals

Para utilitários transversais (ex.: conversão, tipos, retry), preferir namespace interno:

- `pyield/_internal/converters.py`
- `pyield/_internal/types.py`
- `pyield/_internal/retry.py`

Diretriz de naming:

- Dentro de `_internal`, usar nomes normais de função (sem `_` no nome por padrão).
- Reservar `_nome` para helper local que não deve ser ponto de entrada nem interno.

## 3) Contrato de Estabilidade

Antes de mudar assinatura/comportamento, classificar alvo:

1. API pública:
   - Exige compatibilidade retroativa ou depreciação explícita.
   - Exige release notes.
2. API interna:
   - Pode evoluir mais rápido.
   - Ainda deve manter consistência entre módulos consumidores.

## 4) Conversão de Datas: Dois Caminhos Claros

Não unificar camadas diferentes.

- Conversão escalar/array (fora de expressão Polars): manter função dedicada.
- Conversão em `pl.Expr` (dentro de pipeline Polars): criar função dedicada para `Expr`.

Regras sugeridas para `Expr`:

- Parse tolerante por linha.
- Data inválida vira `null` (não explode ETL).
- Semântica explicitamente documentada.

## 5) Checklist de Decisão (rápido)

Use este checklist antes de implementar:

1. Isso é público ou interno?
2. Se público, qual política de depreciação?
3. Existe utilitário interno que evita duplicação?
4. A mudança reduz complexidade acidental?
5. Há teste cobrindo comportamento novo e regressão principal?
6. O comportamento de erro/null está explícito?

## 6) Testes Mínimos por Tipo de Mudança

- Mudança em utilitário interno transversal:
  - teste unitário do utilitário;
  - pelo menos 1 teste de integração no módulo consumidor principal.
- Mudança em API pública:
  - testes de contrato (entrada/saída/erro);
  - caso de compatibilidade retroativa (quando aplicável).
- Mudança em `Expr`:
  - teste com `LazyFrame`/`select`;
  - teste com entrada inválida e nula.

## 7) Bibliotecas de Referência

Projetos para consultar decisões de arquitetura e manutenção:

- Pydantic
- FastAPI
- Pandas
- SciPy
- scikit-learn
- Polars
- HTTPX
- SQLAlchemy

Use essas referências para padrão de API estável, internals, depreciação e testes.
