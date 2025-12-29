# Revolut Transaction Converter

Converts Revolut exports into Monarch-compatible CSVs and builds balance history
imports. Scripts auto-select the newest `input/YYYY-MM-DD` folder unless
`RUN_DATE` is set.

## Requirements

- Python 3.13 via `uv`
- `EXCHANGE_RATE_API_KEY` and optionally `EXCHANGE_RATE_API_URL`

Copy `.env.example` to `.env` and fill in the values.

## Install

```bash
uv sync
```

## Run

```bash
uv run python checking_transaction_converter.py
uv run python savings_transaction_converter.py
uv run python generate_checking_balance_history.py
uv run python generate_savings_balance_history.py
```

Override the input folder:

```bash
RUN_DATE=2025-12-29 uv run python checking_transaction_converter.py
```

## Inputs/Outputs

- Inputs: `input/YYYY-MM-DD/*.csv`
- Outputs: `output/YYYY-MM-DD/*_import.csv`

## Tests

```bash
uv run pytest
```
