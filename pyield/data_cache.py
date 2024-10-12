from datetime import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd

from pyield import bday

TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")

UPDATE_HOUR = 21
GIT_URL = "https://raw.githubusercontent.com/crdcj/pyield-data/main"

DI_FILE = "di_data.parquet"
DI_URL = f"{GIT_URL}/{DI_FILE}"

ANBIMA_FILE = "anbima_data.parquet"
ANBIMA_URL = f"{GIT_URL}/{ANBIMA_FILE}"


class DataCache:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataCache, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self._datasets = {
            "di": {"url": DI_URL, "df": None, "last_date": None},
            "anbima": {"url": ANBIMA_URL, "df": None, "last_date": None},
        }

    def _should_update_dataset(self, key) -> bool:
        data_info = self._datasets[key]
        last_date_in_dataset = data_info["last_date"]
        bz_now = dt.now(TIMEZONE_BZ)
        bz_hour = bz_now.hour
        bz_today = bz_now.date()
        bz_last_bday = bday.offset(bz_today, 0, roll="backward").date()
        condition1 = bz_hour >= UPDATE_HOUR
        condition2 = bz_last_bday != last_date_in_dataset
        return condition1 and condition2

    def _load_dataset(self, key: str):
        dataset_info = self._datasets[key]
        df = pd.read_parquet(dataset_info["url"])
        dataset_info["df"] = df
        if key == "di":
            dataset_info["last_date"] = df["TradeDate"].max().date()
        elif key == "anbima":
            dataset_info["last_date"] = df["ReferenceDate"].max().date()

    def get_dataframe(self, key: str) -> pd.DataFrame:
        dataset_info = self._datasets.get(key)
        if dataset_info is None:
            raise ValueError(f"Dataset '{key}' não está configurado.")
        if dataset_info["df"] is None or self._should_update_dataset(key):
            self._load_dataset(key)
        return dataset_info["df"].copy()


# Instância única da classe para uso interno da biblioteca
_data_cache = DataCache()


# Funções de acesso para os módulos
def get_di_dataset():
    return _data_cache.get_dataframe("di")


def get_anbima_dataset():
    return _data_cache.get_dataframe("anbima")
