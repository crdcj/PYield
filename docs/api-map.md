# Mapa da API

Visão geral das principais funções públicas do PYield.

??? "`yd.du` (dias úteis)"
    ```text
    yd.du
    ├── contar(inicio, fim)
    ├── contar_expr(inicio, fim)
    ├── deslocar(data, n)
    ├── deslocar_expr(data, n)
    ├── eh_dia_util(data)
    ├── eh_dia_util_expr(data)
    ├── gerar(inicio, fim)
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
    └── vencimento_expr(codigo, contrato)
    ```

??? "`yd.di1` (curva DI1)"
    ```text
    yd.di1
    ├── dados(data)
    ├── interpolar_taxa(...)
    ├── interpolar_taxas(...)
    └── datas_disponiveis()
    ```

??? "`yd.tpf` (títulos públicos federais)"
    ```text
    yd.tpf
    ├── taxas(data, titulo)
    ├── taxas_historicas(inicio, fim, titulo)
    ├── vencimentos(data, titulo)
    ├── estoque(data)
    ├── leiloes(data=..., inicio=..., fim=...)
    ├── secundario.mensal(data, extragrupo=...)
    ├── secundario.intradia()
    ├── secundario.nome_arquivo_mensal(data, extragrupo=...)
    ├── secundario.baixar_zip(data, extragrupo=...)
    ├── secundario.zip_para_silver(conteudo_zip)
    ├── secundario.ler_zip(caminho)
    ├── benchmarks(...)
    ├── curva_pre(data)
    ├── premios_pre(...)
    ├── rmd
    └── TipoTPF
    ```

??? "`yd.selic` (Selic, COPOM e política monetária)"
    ```text
    yd.selic
    ├── over(data)
    ├── over_serie(...)
    ├── meta(data)
    ├── meta_serie(...)
    ├── compromissadas(...)
    ├── compromissada
    ├── copom
    ├── cpm
    └── probabilities
    ```

??? "`yd.ipca` (inflação IPCA)"
    ```text
    yd.ipca
    ├── indice(data)
    ├── indices(...)
    ├── indices_ultimos(...)
    ├── taxa(...)
    ├── taxas(...)
    ├── taxas_ultimas(...)
    └── taxa_projetada(...)
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
    ├── rentabilidade_expr(...)
    └── vna(data)
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
    ├── vnas()
    ├── vna(data)
    ├── vna_projetado(data, vna_base, inflacao)
    ├── datas_pagamento(...)
    ├── fluxos_caixa(...)
    ├── cotacao(...)
    ├── pu(...)
    ├── taxa(...)
    ├── duration(...)
    ├── duration_expr(...)
    ├── dv01(...)
    ├── dv01_expr(...)
    ├── taxas_zero(data_liquidacao, vencimentos, taxas, ...)
    ├── implicitas(data_liquidacao, vencimentos_tir, taxas_tir, ...)
    └── curva(data_liquidacao, vencimentos_tir, taxas_tir, ...)
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
    ├── rentabilidade(...)
    ├── rentabilidade_expr(...)
    ├── premio(data, pontos_base=...)
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
    ├── taxas_zero(...)
    ├── cotacao(...)
    ├── taxa(...)
    ├── pu(...)
    └── dv01(...)
    ```

??? "`yd.ntnc` (Tesouro IGP-M+ com cupom)"
    ```text
    yd.ntnc
    ├── dados(data)
    ├── vnas()
    ├── vna(data, vencimento)
    ├── vna_projetado(data, vna_base, inflacao)
    ├── datas_pagamento(...)
    ├── fluxos_caixa(...)
    ├── cotacao(...)
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

??? "`yd.Interpolador` (interpolação de curvas)"
    ```text
    yd.Interpolador
    ```

??? "`yd.hoje` (data atual no Brasil)"
    ```text
    yd.hoje()
    ```

??? "`yd.agora` (data e hora atual no Brasil)"
    ```text
    yd.agora()
    ```
