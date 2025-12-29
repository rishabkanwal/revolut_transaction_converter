from revolut_utils import (
    AccountConfig,
    build_balance_history,
    build_usd_rate_series,
    fetch_timeframe_quotes,
    get_run_date,
    get_latest_balance_history_value,
    load_monarch_transactions,
    output_dir,
    require_api_key,
)

# === CONFIG ===
OUTPUT_CSV_NAME = "savings_balance_history_import.csv"
ACCOUNT_NAME = "Revolut Savings"
ACCOUNT_CURRENCY = "GBP"
# =================


def main() -> None:
    run_date = get_run_date()
    output_folder = output_dir(run_date)
    output_root = output_folder.parent

    starting_balance = get_latest_balance_history_value(
        output_root,
        OUTPUT_CSV_NAME,
        run_date=run_date,
        column="Original Balance",
        account=ACCOUNT_NAME,
    )
    if starting_balance is None:
        raise RuntimeError(
            "No prior balance history found; cannot infer starting balance."
        )

    transactions = load_monarch_transactions()
    transactions = transactions[transactions["Account"] == ACCOUNT_NAME]
    if transactions.empty:
        raise RuntimeError("No transactions found for requested accounts.")

    transactions = transactions.sort_values("Date")
    start_date = transactions["Date"].min().date()
    end_date = transactions["Date"].max().date()

    api_key = require_api_key()
    source_currency, quotes = fetch_timeframe_quotes(start_date, end_date, api_key)
    rates = build_usd_rate_series(quotes, source_currency, ACCOUNT_CURRENCY)
    rate_lookup = lambda date_str, currency: (
        rates.get(date_str) if currency == ACCOUNT_CURRENCY else None
    )

    final = build_balance_history(
        transactions,
        [AccountConfig(ACCOUNT_NAME, ACCOUNT_CURRENCY, starting_balance)],
        rate_lookup,
    )

    output_path = output_folder / OUTPUT_CSV_NAME
    final.to_csv(output_path, index=False)
    print(f"Exported {len(final)} transactions to {output_path}")


if __name__ == "__main__":
    main()
