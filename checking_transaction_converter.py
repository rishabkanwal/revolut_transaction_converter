import pandas as pd

from revolut_utils import (
    build_usd_rates,
    fetch_timeframe_quotes,
    get_run_date,
    input_path,
    monarch_row,
    output_dir,
    require_api_key,
)

# === CONFIGURATION ===
INPUT_FILENAME = "checking_transactions.csv"
OUTPUT_CSV_NAME = "checking_transaction_import.csv"
ACCOUNT_NAME = "Revolut Checking"
# =====================


def main() -> None:
    run_date = get_run_date()
    input_csv = input_path(run_date, INPUT_FILENAME)
    output_folder = output_dir(run_date)

    df = pd.read_csv(input_csv)
    df = df[df["State"].isin(["COMPLETED", "PENDING"])]

    df["Started Date"] = pd.to_datetime(df["Started Date"], errors="coerce")
    df = df.dropna(subset=["Started Date"])
    df["Date"] = df["Started Date"].dt.date

    currencies = sorted(df["Currency"].dropna().unique())
    start_date = df["Date"].min()
    end_date = df["Date"].max()

    api_key = require_api_key()
    source_currency, quotes = fetch_timeframe_quotes(start_date, end_date, api_key)
    rates = build_usd_rates(quotes, source_currency, currencies)

    output_rows = []
    for row in df.itertuples(index=False):
        date_str = row.Date.isoformat()
        description = str(row.Description)
        amount = float(row.Amount)
        currency = row.Currency

        rate = rates.get((date_str, currency))
        if rate is None:
            raise ValueError(f"No exchange rate found for {currency} on {date_str}")

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
