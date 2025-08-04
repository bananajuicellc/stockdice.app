# stockdice.app

Choose stocks randomly, weighted by market cap.

## Setup

Clone the repository. Alternatively, [fork this
repository](https://github.com/bananajuicellc/stockdice.app/fork) and clone your
own copy.

```
git clone https://github.com/bananajuicellc/stockdice.app.git stockdice
cd stockdice
```

Get an API key for [Financial Modeling Prep](https://site.financialmodelingprep.com/). Because this script downloads values in bulk, a paid plan is required.

1. Sign up for an account. I'm using the "Starter" plan for personal use.
2. Go to your [dashboard](https://site.financialmodelingprep.com/developer/docs/dashboard) and get an API key.
3. Create a copy of the environment "toml" file. `cp environment-EXAMPLE.toml environment.toml`
4. Replace the API key in `environment.toml` with your own.

Setup a Python development environment by installing `uv`. See:
https://docs.astral.sh/uv/getting-started/installation/ for instructions.

Initialize the local database.

```
uv run cli/initialize_db.py
```

## Usage

To use this random stock picker, first download the latest data from the FMP API.

```
uv run cli/refresh_db.py
```

Sometimes this will fail (usually because of rate limiting). Restart the command
within 24 hours and it will resume where it left off.

Pick a stock.

```
uv run cli/roll_stockdice.py
```

This will print out a symbol, as well as additional information about the stock.
Purchase a selection of this stock. For example, purchase $1,000 of each stock
chosen so that the weighting of your portfolio approaches that of the formula.
It is helpful to use a broker which sells partial shares so that you can get as
close to an even amout per stock as possible.

Other options are available. See help:

```
$ uv run cli/roll_stockdice.py -h
usage: stockdice.py [-h] [-n NUMBER] [-o OUTPUT] [-f {text,csv}] [-w]

options:
  -h, --help            show this help message and exit
  -n NUMBER, --number NUMBER
                        Number of samples.
  -o OUTPUT, --output OUTPUT
                        File path for output.
  -f {text,csv}, --format {text,csv}
                        Output format.
  -w, --weighted        Weight stocks by market capitalization instead of evenly.
```

## Disclaimer

The Content is for informational purposes only, you should not construe
any such information or other material as legal, tax, investment,
financial, or other advice. Nothing contained on our Site constitutes a
solicitation, recommendation, endorsement, or offer by me or any third
party service provider to buy or sell any securities or other financial
instruments.
