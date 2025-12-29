from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import math
import os
from pathlib import Path
from typing import Callable, Iterable, Mapping

import pandas as pd
import requests
from dotenv import load_dotenv

DATE_FORMAT = "%Y-%m-%d"
API_URL = "https://api.exchangerate.host/timeframe"
_ENV_LOADED = False


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class AccountConfig:
    name: str
    currency: str
    starting_balance: float


def load_env() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    load_dotenv()
    _ENV_LOADED = True


def get_latest_dated_folder(root: Path) -> Path:
    if not root.exists():
        raise FileNotFoundError(f"Missing directory: {root}")

    candidates: list[tuple[datetime, Path]] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            parsed = datetime.strptime(child.name, DATE_FORMAT)
        except ValueError:
            continue
        candidates.append((parsed, child))

    if not candidates:
        raise FileNotFoundError(f"No dated folders found in {root}")

    return max(candidates, key=lambda item: item[0])[1]


def get_run_date(input_root: Path = Path("input"), env_var: str = "RUN_DATE") -> str:
    load_env()
    override = os.getenv(env_var)
    if override:
        try:
            datetime.strptime(override, DATE_FORMAT)
        except ValueError as exc:
            raise ConfigError(
                f"{env_var} must be in {DATE_FORMAT} format, got: {override}"
            ) from exc
        return override

    latest = get_latest_dated_folder(input_root)
    return latest.name


def input_path(run_date: str, filename: str, input_root: Path = Path("input")) -> Path:
    path = input_root / run_date / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")
    return path


def output_dir(run_date: str, output_root: Path = Path("output")) -> Path:
    path = output_root / run_date
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_latest_balance_history_value(
    output_root: Path,
    filename: str,
    run_date: str | None = None,
    column: str = "Original Balance",
    account: str | None = None,
    account_column: str = "Account",
) -> float | None:
    candidates: list[tuple[datetime, Path]] = []
    for file in output_root.glob(f"*/{filename}"):
        if not file.is_file():
            continue
        try:
            folder_date = datetime.strptime(file.parent.name, DATE_FORMAT)
        except ValueError:
            continue
        candidates.append((folder_date, file))

    if not candidates:
        return None

    if run_date:
        try:
            run_dt = datetime.strptime(run_date, DATE_FORMAT)
        except ValueError as exc:
            raise ConfigError(f"run_date must be in {DATE_FORMAT} format") from exc
        candidates = [candidate for candidate in candidates if candidate[0] < run_dt]
        if not candidates:
            return None

    latest_file = max(candidates, key=lambda item: item[0])[1]
    df = pd.read_csv(latest_file)
    if account:
        if account_column not in df.columns:
            raise ConfigError(f"Missing column '{account_column}' in {latest_file}")
        df = df[df[account_column] == account]
        if df.empty:
            raise ConfigError(f"No rows found for account '{account}' in {latest_file}")
    if "Date" in df.columns:
        df = df.sort_values("Date")
    if column not in df.columns:
        raise ConfigError(f"Missing column '{column}' in {latest_file}")
    series = df[column].dropna()
    if series.empty:
        raise ConfigError(f"No values found for '{column}' in {latest_file}")
    return float(series.iloc[-1])


def require_api_key(env_var: str = "EXCHANGE_RATE_API_KEY") -> str:
    load_env()
    api_key = os.getenv(env_var)
    if not api_key:
        raise ConfigError(f"Missing {env_var}. Set it in your environment.")
    return api_key


def get_api_url(env_var: str = "EXCHANGE_RATE_API_URL") -> str:
    load_env()
    return os.getenv(env_var, API_URL)


def fetch_timeframe_quotes(
    start_date: date, end_date: date, api_key: str, api_url: str | None = None
) -> tuple[str, Mapping[str, Mapping[str, float]]]:
    if api_url is None:
        api_url = get_api_url()
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "access_key": api_key,
    }
    response = requests.get(api_url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if not data.get("success", True):
        raise RuntimeError(f"Exchange rate API error: {data}")

    quotes = data.get("quotes")
    if not isinstance(quotes, dict):
        raise RuntimeError(f"Unexpected API response: {data}")

    source_currency = data.get("source", "USD")
    return source_currency, quotes


def build_usd_rates(
    quotes_by_date: Mapping[str, Mapping[str, float]],
    source_currency: str,
    currencies: Iterable[str],
    target_currency: str = "USD",
) -> dict[tuple[str, str], float]:
    if source_currency != target_currency:
        raise RuntimeError(
            f"Unsupported API source currency {source_currency}; expected {target_currency}"
        )

    normalized = sorted({currency for currency in currencies if currency})
    rates: dict[tuple[str, str], float] = {}

    for date_str, daily_quotes in quotes_by_date.items():
        for currency in normalized:
            if currency == target_currency:
                rates[(date_str, currency)] = 1.0
                continue
            quote_key = f"{source_currency}{currency}"
            rate = daily_quotes.get(quote_key)
            if rate:
                rates[(date_str, currency)] = 1 / rate

    return rates


def build_usd_rate_series(
    quotes_by_date: Mapping[str, Mapping[str, float]],
    source_currency: str,
    currency: str,
    target_currency: str = "USD",
) -> dict[str, float]:
    if source_currency != target_currency:
        raise RuntimeError(
            f"Unsupported API source currency {source_currency}; expected {target_currency}"
        )

    rates: dict[str, float] = {}
    for date_str, daily_quotes in quotes_by_date.items():
        if currency == target_currency:
            rates[date_str] = 1.0
            continue
        quote_key = f"{source_currency}{currency}"
        rate = daily_quotes.get(quote_key)
        if rate:
            rates[date_str] = 1 / rate
    return rates


def parse_money_amount(value: object, symbol: str) -> float:
    if value is None:
        return 0.0
    if isinstance(value, float) and math.isnan(value):
        return 0.0
    text = str(value).replace(symbol, "").replace(",", "").strip()
    if not text:
        return 0.0
    return float(text)


def monarch_row(
    date_str: str,
    description: str,
    account: str,
    amount_usd: float,
    amount_original: float,
) -> dict[str, object]:
    return {
        "Date": date_str,
        "Merchant": description,
        "Category": "",
        "Account": account,
        "Original Statement": description,
        "Notes": "",
        "Amount": amount_usd,
        "Original Amount": amount_original,
        "Tags": "",
    }


def load_monarch_transactions(
    input_root: Path = Path("output"),
) -> pd.DataFrame:
    files = list(input_root.glob("**/*_import.csv"))
    if not files:
        raise FileNotFoundError(f"No Monarch import files found in {input_root}")

    frames: list[pd.DataFrame] = []
    for file in files:
        try:
            df = pd.read_csv(file, parse_dates=["Date"])
        except Exception as exc:
            print(f"WARN: Skipping {file}: {exc}")
            continue
        required = {"Date", "Account", "Amount", "Original Amount"}
        if not required.issubset(df.columns):
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError("No valid Monarch import files found.")

    return pd.concat(frames, ignore_index=True)


def build_balance_history(
    transactions: pd.DataFrame,
    accounts: Iterable[AccountConfig],
    rate_lookup: Callable[[str, str], float | None],
    target_currency: str = "USD",
) -> pd.DataFrame:
    balance_frames: list[pd.DataFrame] = []

    for account in accounts:
        acc_txns = transactions[transactions["Account"] == account.name]
        if acc_txns.empty:
            continue

        if account.currency == target_currency:
            daily = (
                acc_txns.groupby("Date", as_index=False)["Amount"]
                .sum()
                .sort_values("Date")
            )
            daily["Balance"] = daily["Amount"].cumsum() + account.starting_balance
            daily["Original Balance"] = daily["Balance"]
        else:
            daily = (
                acc_txns.groupby("Date", as_index=False)["Original Amount"]
                .sum()
                .sort_values("Date")
            )
            daily["Original Balance"] = (
                daily["Original Amount"].cumsum() + account.starting_balance
            )

            def convert_to_usd(row: pd.Series) -> float | None:
                date_str = row["Date"].date().isoformat()
                rate = rate_lookup(date_str, account.currency)
                if rate is None:
                    print(
                        f"WARN: No exchange rate for {date_str} {account.currency}, skipping..."
                    )
                    return None
                return round(row["Original Balance"] * rate, 2)

            daily["Balance"] = daily.apply(convert_to_usd, axis=1)

        daily["Account"] = account.name
        balance_frames.append(daily[["Date", "Balance", "Original Balance", "Account"]])

    if not balance_frames:
        raise RuntimeError("No balances generated for requested accounts.")

    final = pd.concat(balance_frames, ignore_index=True)
    final = final.sort_values(["Account", "Date"])
    final["Date"] = final["Date"].dt.strftime("%Y-%m-%d")
    return final
