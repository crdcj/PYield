"""
Exemplo de resposta da API de leilões do Tesouro:
https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/resultados?dataleilao=30/09/2025

{
  "registros": [
    {
      "quantidade_bcb": 0,
      "data_leilao": "30/09/2025",
      "oferta": 150000,
      "titulo": "NTN-B",
      "liquidacao": "01/10/2025",
      "quantidade_aceita": 10000,
      "prazo": 1050,
      "vencimento": "15/08/2028",
      "benchmark": "NTN-B 3 anos",
      "pu_medio": 4351.91338575,
      "taxa_media": 8.1375,
      "financeiro_aceito": 43518468.56,
      "pu_minimo": 4351.846856,
      "numero_edital": 231,
      "taxa_maxima": 8.1375,
      "tipo_leilao": "Venda",
      "financeiro_bcb": 0
    },
    {
      "quantidade_bcb": 0,
      "liquidacao_segunda_volta": "2025-10-01T00:00:00.000Z",
      "oferta_segunda_volta": 37499,
      "data_leilao": "30/09/2025",
      "oferta": 150000,
      "titulo": "LFT",
      "liquidacao": "01/10/2025",
      "financeiro_aceito_segunda_volta": 0,
      "quantidade_aceita": 150000,
      "prazo": 1067,
      "vencimento": "01/09/2028",
      "benchmark": "LFT 3 anos",
      "pu_medio": 17434.81182753125,
      "taxa_media": 0.0669,
      "financeiro_aceito": 2615194916.22,
      "pu_minimo": 17434.632775,
      "numero_edital": 230,
      "taxa_maxima": 0.0669,
      "tipo_leilao": "Venda",
      "financeiro_bcb": 0,
      "quantidade_aceita_segunda_volta": 0
    },
    {
      "quantidade_bcb": 0,
      "liquidacao_segunda_volta": "2025-10-01T00:00:00.000Z",
      "oferta_segunda_volta": 187499,
      "data_leilao": "30/09/2025",
      "oferta": 750000,
      "titulo": "LFT",
      "liquidacao": "01/10/2025",
      "financeiro_aceito_segunda_volta": 0,
      "quantidade_aceita": 590500,
      "prazo": 2162,
      "vencimento": "01/09/2031",
      "benchmark": "LFT 6 anos",
      "pu_medio": 17360.929790289472,
      "taxa_media": 0.1064,
      "financeiro_aceito": 10250991400.75,
      "pu_minimo": 17359.849959,
      "numero_edital": 230,
      "taxa_maxima": 0.1064,
      "tipo_leilao": "Venda",
      "financeiro_bcb": 0,
      "quantidade_aceita_segunda_volta": 0
    },
    {
      "quantidade_bcb": 0,
      "liquidacao_segunda_volta": "2025-10-02T00:00:00.000Z",
      "oferta_segunda_volta": 37500,
      "data_leilao": "30/09/2025",
      "oferta": 150000,
      "titulo": "NTN-B",
      "liquidacao": "01/10/2025",
      "financeiro_aceito_segunda_volta": 0,
      "quantidade_aceita": 150000,
      "prazo": 2511,
      "vencimento": "15/08/2032",
      "benchmark": "NTN-B 7 anos",
      "pu_medio": 1255.811481,
      "taxa_media": 7.774,
      "financeiro_aceito": 627417931.33,
      "pu_minimo": 0,
      "numero_edital": 232,
      "taxa_maxima": 7.774,
      "tipo_leilao": "Venda",
      "financeiro_bcb": 0,
      "quantidade_aceita_segunda_volta": 0
    },
    {
      "quantidade_bcb": 0,
      "data_leilao": "30/09/2025",
      "oferta": 150000,
      "titulo": "NTN-B",
      "liquidacao": "01/10/2025",
      "quantidade_aceita": 65450,
      "prazo": 7167,
      "vencimento": "15/05/2045",
      "benchmark": "NTN-B 25 anos",
      "pu_medio": 1564.3624848235295,
      "taxa_media": 7.28,
      "financeiro_aceito": 267111959.97,
      "pu_minimo": 0,
      "numero_edital": 232,
      "taxa_maxima": 7.28,
      "tipo_leilao": "Venda",
      "financeiro_bcb": 0
    }
  ],
  "status": "ok"
}
"""  # noqa

from datetime import datetime

import polars as pl
import requests


def process_treasury_auction_data(auction_date: str) -> pl.DataFrame:
    """
    Fetches and processes Brazilian Treasury auction data for a given date.

    This function queries the Tesouro Nacional API to retrieve auction results
    for a specific date. It then processes the JSON response using the Polars
    library to create a well-structured and typed DataFrame.

    Args:
        auction_date: The date of the auction in the format 'DD/MM/YYYY'.

    Returns:
        A Polars DataFrame containing the processed auction data.
        Returns an empty DataFrame if the request fails or no data is found.
    """
    # Validate the input date format
    try:
        datetime.strptime(auction_date, "%d/%m/%Y")
    except ValueError:
        print("Error: Invalid date format. Please use 'DD/MM/YYYY'.")
        return pl.DataFrame()

    # The base URL for the Tesouro Nacional auctions API
    base_url = (
        "https://apiapex.tesouro.gov.br/aria/v1/api-leiloes-pub/custom/resultados"
    )

    # Set the parameters for the API request
    params = {"dataleilao": auction_date}

    try:
        # Make the GET request to the API
        response = requests.get(base_url, params=params)

        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()

        # Parse the JSON response
        data = response.json()

        # Check if the 'registros' key exists and is not empty
        if "registros" in data and data["registros"]:
            # Convert the list of auction records into a Polars DataFrame
            df = pl.from_dicts(data["registros"])

            # Define the schema for data type conversion
            schema = {
                "quantidade_bcb": pl.Int64,
                "data_leilao": pl.Utf8,
                "oferta": pl.Int64,
                "titulo": pl.Utf8,
                "liquidacao": pl.Utf8,
                "quantidade_aceita": pl.Int64,
                "prazo": pl.Int64,
                "vencimento": pl.Utf8,
                "benchmark": pl.Utf8,
                "pu_medio": pl.Float64,
                "taxa_media": pl.Float64,
                "financeiro_aceito": pl.Float64,
                "pu_minimo": pl.Float64,
                "numero_edital": pl.Int64,
                "taxa_maxima": pl.Float64,
                "tipo_leilao": pl.Utf8,
                "financeiro_bcb": pl.Int64,
                "liquidacao_segunda_volta": pl.Utf8,
                "oferta_segunda_volta": pl.Int64,
                "financeiro_aceito_segunda_volta": pl.Float64,
                "quantidade_aceita_segunda_volta": pl.Int64,
            }

            # Select and cast columns according to the defined schema
            # Using a loop to handle missing columns gracefully
            select_cols = []
            for col, dtype in schema.items():
                if col in df.columns:
                    select_cols.append(pl.col(col).cast(dtype, strict=False))

            df = df.select(select_cols)

            # Convert date columns from string to Date type
            date_columns = ["data_leilao", "liquidacao", "vencimento"]
            for col_name in date_columns:
                if col_name in df.columns:
                    # Handle different date formats that might appear in the API
                    df = df.with_columns(
                        pl.when(pl.col(col_name).str.contains("T"))
                        .then(
                            pl.col(col_name)
                            .str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                            .dt.date()
                        )
                        .otherwise(
                            pl.col(col_name).str.to_date("%d/%m/%Y", strict=False)
                        )
                        .alias(col_name)
                    )

            # Handle the specific format of 'liquidacao_segunda_volta'
            if "liquidacao_segunda_volta" in df.columns:
                df = df.with_columns(
                    pl.col("liquidacao_segunda_volta")
                    .str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    .dt.date()
                    .alias("liquidacao_segunda_volta")
                )

            return df
        else:
            print(f"No auction data found for the date: {auction_date}")
            return pl.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")
        return pl.DataFrame()
    except ValueError as e:
        print(f"An error occurred while parsing the JSON response: {e}")
        return pl.DataFrame()
