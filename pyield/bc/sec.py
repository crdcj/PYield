import logging

import pandas as pd

from pyield import global_retry
from pyield.date_converter import DateScalar, convert_input_dates

logger = logging.getLogger(__name__)


def _build_download_url(target_date: DateScalar) -> str:
    """
    URL com todos os arquivos disponíveis:
    https://www4.bcb.gov.br/pom/demab/negociacoes/apresentacao.asp?frame=1

    Exemplo de URL para download:
    https://www4.bcb.gov.br/pom/demab/negociacoes/download/NegE202409.ZIP

    File format: NegEYYYYMM.ZIP
    """
    target_date = convert_input_dates(target_date)
    file_date = target_date.strftime("%Y%m")
    file_name = f"NegE{file_date}.ZIP"
    base_url = "https://www4.bcb.gov.br/pom/demab/negociacoes/download"
    return f"{base_url}/{file_name}"


@global_retry
def _fetch_data_from_url(file_url: str) -> pd.DataFrame:
    df = pd.read_csv(
        file_url,
        sep=";",
        decimal=",",
        dtype_backend="numpy_nullable",
    )
    for col in ["DATA MOV", "EMISSAO", "VENCIMENTO"]:
        df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")

    return df


def sec(target_date: DateScalar) -> pd.DataFrame:
    url = _build_download_url(target_date)
    return _fetch_data_from_url(url)
