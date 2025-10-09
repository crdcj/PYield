import logging

import requests

logger = logging.getLogger(__name__)

url_pagina_inicial = "https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/1.0018/taxasOnline.asp"
url_consulta_dados = "https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/1.0018/exibedados.asp"
url_download = "https://www.anbima.com.br/sistemas/taxasonline/consulta/versao/1.0018/download_dados.asp?extensao=csv"


def _fetch_url_data(data_desejada) -> str:
    headers = {  # Cabeçalhos para simular um navegador real
        # O Referer precisa ser a página de onde a ação se origina.
        "Referer": url_pagina_inicial,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",  # noqa
        "Origin": "https://www.anbima.com.br",
        # Necessário para POST com payload
        "Content-Type": "application/x-www-form-urlencoded",
    }

    payload = {  # Payload com a data. Usaremos para a consulta e o download.
        "dataref": data_desejada,
        "dtRefIdioma": data_desejada,
        "layoutimprimir": "0",
        "idioma": "",
        "idresumo": "",
        "nome": "",
        "provedor": "",
        "codigo": "",
        "vencimento": "",
        "referencia": "",
        "dbNome": "",
        "fldColunas": ["C2", "C17", "C18", "C19"],
    }

    # 1. Iniciar a sessão
    with requests.Session() as s:
        # P1: Fazer um GET na página inicial para obter os cookies de sessão.
        s.get(url_pagina_inicial, headers=headers)

        # P2: Simular o clique em "Consultar" enviando o POST para o endpoint correto.
        try:
            response_consulta = s.post(
                url_consulta_dados, headers=headers, data=payload
            )
            response_consulta.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao registrar a data na sessão: {e}")
            return ""

        # P3: Com a data devidamente registrada na sessão, solicitar o download.
        try:
            response_download = s.post(url_download, headers=headers, data=payload)
            response_download.raise_for_status()

            # Checando se o conteúdo parece ser um CSV e não uma página de erro
            if "text/html" in response_download.headers.get("Content-Type", ""):
                logger.error("AVISO: O servidor respondeu com HTML em vez de CSV.")
                logger.error(f"Conteúdo recebido: {response_download.text[:500]}")
                return ""
            response_download.encoding = "iso-8859-1"
            return response_download.text

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro durante o download: {e}")
            if "response_download" in locals():
                logger.error(f"Resposta do servidor: {response_download.text[:500]}")
            return ""
