from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import revolut_utils as ru


@pytest.fixture(autouse=True)
def reset_env(monkeypatch: pytest.MonkeyPatch) -> None:
    ru._ENV_LOADED = False
    monkeypatch.delenv("RUN_DATE", raising=False)
    monkeypatch.delenv("EXCHANGE_RATE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_RATE_API_URL", raising=False)


def test_get_latest_dated_folder(tmp_path: Path) -> None:
    (tmp_path / "2024-01-01").mkdir()
    (tmp_path / "2024-03-01").mkdir()
    (tmp_path / "notes").mkdir()

    latest = ru.get_latest_dated_folder(tmp_path)
    assert latest.name == "2024-03-01"


def test_get_run_date_reads_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("RUN_DATE=2024-02-01\n", encoding="utf-8")
    input_root = tmp_path / "input"
    (input_root / "2024-01-01").mkdir(parents=True)

    monkeypatch.chdir(tmp_path)
    ru._ENV_LOADED = False

    assert ru.get_run_date(input_root=input_root) == "2024-02-01"


def test_get_latest_balance_history_value_account_specific(tmp_path: Path) -> None:
    output_root = tmp_path / "output"
    older_dir = output_root / "2024-01-01"
    newer_dir = output_root / "2024-02-01"
    older_dir.mkdir(parents=True)
    newer_dir.mkdir(parents=True)

    older_file = older_dir / "balance.csv"
    older_file.write_text(
        "Date,Balance,Original Balance,Account\n"
        "2024-01-01,100,100,Checking\n"
        "2024-01-02,110,110,Checking\n"
        "2024-01-02,200,200,Savings\n",
        encoding="utf-8",
    )

    newer_file = newer_dir / "balance.csv"
    newer_file.write_text(
        "Date,Balance,Original Balance,Account\n"
        "2024-02-01,120,120,Checking\n",
        encoding="utf-8",
    )

    latest = ru.get_latest_balance_history_value(
        output_root,
        "balance.csv",
        run_date="2024-03-01",
        column="Balance",
        account="Checking",
    )
    assert latest == 120.0

    prior = ru.get_latest_balance_history_value(
        output_root,
        "balance.csv",
        run_date="2024-02-01",
        column="Balance",
        account="Checking",
    )
    assert prior == 110.0


def test_get_latest_balance_history_value_missing_account(tmp_path: Path) -> None:
    output_root = tmp_path / "output"
    dated_dir = output_root / "2024-01-01"
    dated_dir.mkdir(parents=True)
    (dated_dir / "balance.csv").write_text(
        "Date,Balance,Original Balance,Account\n"
        "2024-01-01,100,100,Checking\n",
        encoding="utf-8",
    )

    with pytest.raises(ru.ConfigError):
        ru.get_latest_balance_history_value(
            output_root,
            "balance.csv",
            run_date="2024-02-01",
            column="Balance",
            account="Savings",
        )


def test_build_balance_history_usd_uses_amount() -> None:
    transactions = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "Account": ["Checking", "Checking"],
            "Amount": [10.0, -5.0],
            "Original Amount": [8.0, -4.0],
        }
    )

    account = ru.AccountConfig("Checking", "USD", starting_balance=100.0)

    def rate_lookup(_date_str: str, _currency: str) -> float | None:
        raise AssertionError("rate_lookup should not be called for USD accounts")

    result = ru.build_balance_history(transactions, [account], rate_lookup)
    assert result["Balance"].tolist() == [110.0, 105.0]
    assert result["Original Balance"].tolist() == [110.0, 105.0]


def test_build_balance_history_non_usd_uses_rates() -> None:
    transactions = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "Account": ["Savings", "Savings"],
            "Amount": [12.0, -6.0],
            "Original Amount": [10.0, -5.0],
        }
    )

    account = ru.AccountConfig("Savings", "GBP", starting_balance=100.0)
    rates = {"2024-01-01": 2.0, "2024-01-02": 1.0}

    def rate_lookup(date_str: str, currency: str) -> float | None:
        if currency != "GBP":
            return None
        return rates.get(date_str)

    result = ru.build_balance_history(transactions, [account], rate_lookup)
    assert result["Original Balance"].tolist() == [110.0, 105.0]
    assert result["Balance"].tolist() == [220.0, 105.0]
