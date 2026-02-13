import importlib
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pyield import b3

futures_core = importlib.import_module("pyield.b3.futures.core")

TEST_DATA_DIR = Path(__file__).parent / "data"
ARQUIVO_POR_DATA = {
    "02-02-2023": "PR230202.zip",
    "03-02-2025": "PR250203.zip",
    "12-01-2026": "PR260112.zip",
}


def obter_arquivo_teste_local(file_name: str) -> Path:
    """Retorna o caminho de um arquivo de teste local já versionado no repositório."""
    local_path = TEST_DATA_DIR / file_name
    if not local_path.exists():
        raise FileNotFoundError(f"Arquivo de teste não encontrado: {local_path}")
    return local_path


def obter_parquet_referencia(date_str: str, contract_code: str) -> Path:
    """Retorna o caminho do parquet canônico para a data e contrato."""
    dia, mes, ano = date_str.split("-")
    nome = f"futures_xml_{ano}{mes}{dia}_{contract_code}.parquet"
    return obter_arquivo_teste_local(nome)


def prepare_data(
    date_str: str,
    contract_code: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Prepare Polars DataFrames for comparison."""
    df_expect = pl.read_parquet(obter_parquet_referencia(date_str, contract_code))

    def _buscar_df_historico_local(data, codigo_contrato):
        data_str = data.strftime("%d-%m-%Y")
        arquivo_local = obter_arquivo_teste_local(ARQUIVO_POR_DATA[data_str])
        return b3.read_price_report(
            file_path=arquivo_local, contract_code=codigo_contrato
        )

    monkeypatch.setattr(
        futures_core.hcore, "buscar_df_historico", _buscar_df_historico_local
    )
    df_result = b3.futures(contract_code=contract_code, date=date_str)

    return df_result, df_expect


@pytest.mark.parametrize(
    ("date", "contract_code"),
    [
        ("02-02-2023", "DI1"),
        ("02-02-2023", "FRC"),
        ("02-02-2023", "DDI"),
        ("02-02-2023", "DAP"),
        ("02-02-2023", "DOL"),
        ("02-02-2023", "WDO"),
        ("02-02-2023", "IND"),
        ("02-02-2023", "WIN"),
        ("03-02-2025", "DI1"),
        ("03-02-2025", "FRC"),
        ("03-02-2025", "DDI"),
        ("03-02-2025", "DAP"),
        ("03-02-2025", "DOL"),
        ("03-02-2025", "WDO"),
        ("03-02-2025", "IND"),
        ("03-02-2025", "WIN"),
        ("12-01-2026", "DI1"),
        ("12-01-2026", "FRC"),
        ("12-01-2026", "DDI"),
        ("12-01-2026", "DAP"),
        ("12-01-2026", "DOL"),
        ("12-01-2026", "WDO"),
        ("12-01-2026", "IND"),
        ("12-01-2026", "WIN"),
    ],
)
def test_fetch_and_prepare_data(date, contract_code, monkeypatch):
    """Compara `futures` com parquet canônico usando dados locais offline."""
    result_df, expect_df = prepare_data(date, contract_code, monkeypatch=monkeypatch)
    assert_frame_equal(
        result_df, expect_df, rel_tol=1e-4, check_exact=False, check_dtypes=True
    )
