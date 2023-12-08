Small helper to fetch Prosperous Universe Market data that `pmrv` is getting from `FIO` every 30 minutes and putting it into CSV files with data aggregated on a daily level.

This data can be combined and used in `PRUNplanner` to initially import historic market data for its Market Exploration Charts.

# Run the Python script

1. Install dependencies from `requirements.txt`
2. Run `python ponder.py` to fetch data, create price CSVs and combine them
3. Or: run `python ponder.py --combine` to only combine existing files again