import pandas as pd

from revolut_utils import (
    build_usd_rates,
    fetch_timeframe_quotes,
    get_run_date,
    input_path,
    monarch_row,
    output_dir,
    parse_money_amount,
    require_api_key,
)

# === CONFIGURATION ===
INPUT_FILENAME = "savings_transactions.csv"
OUTPUT_CSV_NAME = "savings_transaction_import.csv"
ACCOUNT_NAME = "Revolut Savings"
SOURCE_CURRENCY = "GBP"
# =====================


def main() -> None:
    run_date = get_run_date()
    input_csv = input_path(run_date, INPUT_FILENAME)
    output_folder = output_dir(run_date)

    df = pd.read_csv(input_csv)
    df["Date"] = pd.to_datetime(df["Date"], format="%b %d, %Y", errors="coerce").dt.date
    df = df.dropna(subset=["Date"])

    df["Money in"] = df["Money in"].apply(lambda value: parse_money_amount(value, "£"))
    df["Money out"] = df["Money out"].apply(lambda value: parse_money_amount(value, "£"))
    df["Amount"] = df["Money in"] - df["Money out"]

    start_date = df["Date"].min()
    end_date = df["Date"].max()

    api_key = require_api_key()
    source_currency, quotes = fetch_timeframe_quotes(start_date, end_date, api_key)
    rates = build_usd_rates(quotes, source_currency, [SOURCE_CURRENCY])

    output_rows = []
    for row in df.itertuples(index=False):
        date_str = row.Date.isoformat()
        description = row.Description
        amount = row.Amount

        rate = rates.get((date_str, SOURCE_CURRENCY))
        if rate is None:
            print(f"WARN: No exchange rate for {SOURCE_CURRENCY} on {date_str}, skipping...")
            continue

        usd_amount = round(amount * rate, 2)
        output_rows.append(
            monarch_row(
                date_str=date_str,
                description=description,
                account=ACCOUNT_NAME,
                amount_usd=usd_amount,
                amount_original=amount,
            )
        )

    output_path = output_folder / OUTPUT_CSV_NAME
    output_df = pd.DataFrame(output_rows)
    output_df.to_csv(output_path, index=False)

    print(f"Exported {len(output_rows)} transactions to {output_path}")


if __name__ == "__main__":
    main()
