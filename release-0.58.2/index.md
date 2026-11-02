# Novidades na versão 0.58.2

Nota reconstituída a partir do histórico Git, usando o [commit de incremento da versão](<https://github.com/crdcj/PYield/commit/7a535165>) e o [diff desde o incremento anterior](<https://github.com/crdcj/PYield/compare/d6d61658...7a535165>). O recorte é definido pelos commits de versão, não por tags de publicação.

## Projeção de VNA da LFT

Ajustes de documentação sobre o intervalo de acruamento e inclusão de aviso de log no cálculo interno quando o avanço difere de um dia útil. O helper interno passa a emitir o aviso em vez de lançar erro nessa condição; isso não deve ser interpretado como remoção de todas as validações do fluxo público.

Não houve mudança de assinatura pública nesse recorte. O incremento acompanha os commits de [logging](<https://github.com/crdcj/PYield/commit/e2af2e96>) e [documentação](<https://github.com/crdcj/PYield/commit/735d149f>).
