# Novidades na versão 0.60.1

Nota reconstituída a partir do histórico Git, usando o [commit de incremento da versão](<https://github.com/crdcj/PYield/commit/26e898a5>) e o [diff desde o último snapshot da 0.60.0](<https://github.com/crdcj/PYield/compare/90734c00...26e898a5>). O recorte é definido pelos commits de versão, não por tags de publicação.

## Consultas de um dia no SGS

A consulta ao SGS passa a incluir o dia anterior quando um bloco teria início e fim iguais, evitando o HTTP 400 dessa requisição. O processamento descarta o dia extra e mantém somente o intervalo solicitado, removendo duplicatas e ordenando por data.

A correção alcança as consultas baseadas no SGS, incluindo Selic, PTAX e taxas do IGP-M. Não exige mudança nas chamadas da aplicação e preserva os contratos de retorno.

## Documentação e ferramentas

O Zensical deixa de ter versão fixa nas dependências de desenvolvimento. O guia de desenvolvimento passa a descrever sua atualização e a validação estrita da documentação. Também foram adicionadas referências de metodologia à navegação.
