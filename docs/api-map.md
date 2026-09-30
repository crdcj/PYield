# Mapa da API

Visão geral das principais funções públicas do PYield.

??? "`yd.du` (dias úteis)"
    ```text
    yd.du
    ├── contar(inicio, fim, calendario="auto")
    ├── contar_expr(inicio, fim, calendario="auto")
    ├── deslocar(datas, deslocamento, ajuste="seguinte", calendario="auto")
    ├── deslocar_expr(data, deslocamento, ajuste="seguinte", calendario="auto")
    ├── eh_dia_util(datas, calendario="auto")
    ├── eh_dia_util_expr(data, calendario="auto")
    ├── gerar(inicio=None, fim=None, limites_inclusivos="ambos", calendario="auto")
    └── ultimo_dia_util()
    ```

??? "`yd.futuro` (futuros B3)"
    ```text
    yd.futuro
    ├── di1
    ├── historico(data, contrato)
    ├── intradia(contrato)
    ├── datas_disponiveis(contrato)
    ├── enriquecer(df, contrato)
    ├── vencimento(codigo, contrato)
    └── vencimento_expr(coluna_codigo, contrato)
    ```

??? "`yd.di1` (curva DI1)"
    ```text
    yd.di1
    ├── dados(datas, inicio_mes=False, filtrar_pre=False)
    ├── interpolar_taxa(...)
    ├── interpolar_taxas(...)
    └── datas_disponiveis()
    ```

??? "`yd.tpf` (títulos públicos federais)"
    ```text
    yd.tpf
    ├── taxas(data, titulo=None)
    ├── taxas_historicas(inicio=None, fim=None, titulo=None)
    ├── vencimentos(data, titulo)
    ├── estoque(data)
    ├── dealers(data=None)
    ├── leiloes(data=..., inicio=..., fim=...)
    ├── leiloes_bcb(data=..., inicio=..., fim=...)
    ├── secundario.mensal(data, extragrupo=...)
    ├── secundario.intradia()
    ├── secundario.baixar_zip(data, extragrupo=...)
    ├── secundario.ler(fonte)
    ├── benchmarks(...)
    ├── curva_pre(data)
    ├── premios_pre(...)
    └── TipoTPF
    ```

??? "`yd.copom` (calendário do Copom)"
    ```text
    yd.copom
    ├── calendario(inicio=None, fim=None)
    └── proxima_reuniao(referencia=None)
    ```

??? "`yd.compromissadas` (leilões de operações compromissadas)"
    ```text
    yd.compromissadas(inicio=None, fim=None)
    ```

??? "`yd.selic` (Selic e política monetária)"
    ```text
    yd.selic
    ├── over(data)
    ├── over_serie(...)
    ├── meta(data)
    └── meta_serie(...)
    ```

??? "`yd.cpm` (opções digitais do COPOM e probabilidades implícitas)"
    ```text
    yd.cpm
    ├── contratos(data)
    └── probabilidades(data, tipo_opcao="call")
    ```

??? "`yd.ipca` (inflação IPCA)"
    ```text
    yd.ipca
    ├── indice(data)
    ├── indice_serie(inicio=None, fim=None, *, ultimos=None)
    ├── taxa(data)
    ├── taxa_serie(inicio=None, fim=None, *, ultimos=None)
    └── taxa_projetada(...)
    ```

??? "`yd.igpm` (inflação IGP-M)"
    ```text
    yd.igpm
    ├── taxa(data)
    └── taxa_serie(inicio=None, fim=None, *, ultimos=None)
    ```

??? "`yd.lft` (Tesouro Selic)"
    ```text
    yd.lft
    ├── dados(data)
    ├── vencimentos(data)
    ├── cotacao(...)
    ├── pu(...)
    ├── taxa(...)
    ├── rentabilidade(...)
    └── rentabilidade_expr(...)
    ```

??? "`yd.ltn` (Tesouro Prefixado)"
    ```text
    yd.ltn
    ├── dados(data)
    ├── vencimentos(data)
    ├── pu(...)
    ├── taxa(...)
    ├── rentabilidade(...)
    ├── rentabilidade_expr(...)
    ├── duration_expr(...)
    ├── dv01(...)
    ├── dv01_expr(...)
    └── taxas_forward(data)
    ```

??? "`yd.ntnb` (Tesouro IPCA+ com cupom)"
    ```text
    yd.ntnb
    ├── dados(data)
    ├── vencimentos(data)
    ├── datas_pagamento(...)
    ├── fluxos_caixa(...)
    ├── cotacao(...)
    ├── pu(...)
    ├── taxa(...)
    ├── duration(...)
    ├── duration_expr(...)
    ├── dv01(...)
    ├── dv01_expr(...)
    ├── taxas_zero(data_liquidacao, vencimentos, taxas)
    └── implicitas(data_liquidacao, vencimentos_tir, taxas_tir, ...)
    ```

??? "`yd.ntnf` (Tesouro Prefixado com cupom)"
    ```text
    yd.ntnf
    ├── dados(data)
    ├── vencimentos(data)
    ├── datas_pagamento(...)
    ├── fluxos_caixa(...)
    ├── pu(...)
    ├── taxa(...)
    ├── duration(...)
    ├── duration_expr(...)
    ├── dv01(...)
    ├── dv01_expr(...)
    ├── taxas_zero(data_liquidacao, vencimentos_ltn, taxas_ltn, ...)
    ├── taxas_zero_forwards(data_liquidacao, vencimentos_ltn, taxas_ltn, ...)
    ├── rentabilidade(...)
    ├── rentabilidade_expr(...)
    ├── premio_limpo(...)
    └── premio_limpo_expr(...)
    ```

??? "`yd.ntnb1` (NTN-B1: Educa+ e Renda+)"
    ```text
    yd.ntnb1
    ├── NomeComercial
    ├── datas_pagamento(...)
    ├── fluxos_caixa(...)
    ├── cotacao(...)
    ├── cotacao_curva_zero(...)
    ├── taxa_curva_zero(...)
    ├── pu(...)
    ├── duration(...)
    └── dv01(...)
    ```

??? "`yd.ntnbp` (NTN-B Principal)"
    ```text
    yd.ntnbp
    ├── cotacao(...)
    ├── taxa(...)
    ├── pu(...)
    └── dv01(...)
    ```

??? "`yd.ntnc` (Tesouro IGP-M+ com cupom)"
    ```text
    yd.ntnc
    ├── dados(data)
    ├── datas_pagamento(...)
    ├── fluxos_caixa(...)
    ├── cotacao(...)
    ├── cotacao_curva_zero(...)
    ├── taxa_curva_zero(...)
    ├── pu(...)
    ├── taxa(...)
    ├── duration(...)
    ├── duration_expr(...)
    ├── dv01(...)
    └── dv01_expr(...)
    ```

??? "`yd.ptax` (PTAX para uma data)"
    ```text
    yd.ptax(data)
    ```

??? "`yd.ptax_serie` (série histórica da PTAX)"
    ```text
    yd.ptax_serie(inicio, fim)
    ```

??? "`yd.di_over` (taxa DI Over)"
    ```text
    yd.di_over(data)
    ```

??? "`yd.forward` (taxa a termo entre dois vértices)"
    ```text
    yd.forward(...)
    ```

??? "`yd.forwards` (curva de taxas a termo)"
    ```text
    yd.forwards(...)
    ```

??? "`yd.forwards_expr` (taxas a termo em expressão Polars)"
    ```text
    yd.forwards_expr(...)
    ```

??? "`yd.Interpolador` (interpolação de curvas)"
    ```text
    yd.Interpolador
    ```

??? "`yd.interpolar` (interpolação direta de curvas)"
    ```text
    yd.interpolar(...)
    ```

??? "`yd.hoje` (data atual no Brasil)"
    ```text
    yd.hoje()
    ```

??? "`yd.agora` (data e hora atual no Brasil)"
    ```text
    yd.agora()
    ```

??? "`yd.rmd` (Relatório Mensal da Dívida)"
    ```text
    yd.rmd(aba)
    ```

??? "`yd.vna` (valor nominal atualizado)"
    ```text
    yd.vna
    ├── valor(titulo, data, vencimento=None)
    ├── historico(titulo, vencimento=None)
    ├── ultimo(titulo, vencimento=None)
    ├── vigencia(titulo, data)
    ├── projetado(titulo, data, vna_base, inflacao=None, selic=None)
    ├── calcular_vna(df, data, fator_variacao=...)
    └── TipoTitulo
    ```

??? "`yd.b3` (APIs técnicas da B3)"
    ```text
    yd.b3
    ├── boletim.baixar_zip(...)
    ├── boletim.buscar(...)
    └── boletim.ler(...)
    ```
