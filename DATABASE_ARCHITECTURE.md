# Database Architecture

## Overview

The application uses **two separate SQLite databases** to ensure data persistence and separation of concerns:

1. **Financial Data Database** (`stockdice.sqlite`) - Market data that gets refreshed
2. **Users Database** (`users.sqlite`) - User accounts and roll history (persistent)

## Why Separate Databases?

The financial data database is periodically refreshed with new market data from the FMP API. This refresh process could potentially affect user data if stored in the same database. By separating them:

- **User data is never affected** by financial data refreshes
- **Financial data can be reset** without losing user accounts
- **Better data isolation** and security
- **Easier backups** - user data can be backed up separately

## Database Details

### Financial Data Database (`stockdice.sqlite`)

**Location**: `third_party/financialmodelingprep.com/stockdice.sqlite`

**Tables**:
- `balance_sheet` - Company balance sheet data
- `company_profile` - Company profiles and current prices
- `forex` - Foreign exchange rates
- `income` - Company income statements
- `symbol` - Stock symbols list

**Initialization**: `uv run cli/initialize_db.py`

**Refresh**: Updated periodically via `uv run cli/refresh_db.py` or `cli/refresh_db_service.py`

**Reset**: Can be reset with `--reset` flag (drops all financial data tables)

### Users Database (`users.sqlite`)

**Location**: `third_party/users/users.sqlite`

**Tables**:
- `user` - User accounts (username, email, password_hash, roll_amount_dollars)
- `roll_history` - User roll transactions and history

**Initialization**: `uv run cli/initialize_users_db.py`

**Reset**: Requires explicit confirmation (`--reset` flag with "yes" confirmation)

**Persistence**: Never automatically reset or affected by financial data refreshes

## Usage in Code

### Accessing Financial Data Database

```python
from stockdice import config

db = config.config.db  # Financial data database
```

### Accessing Users Database

```python
from stockdice import config

users_db = config.config.users_db  # Users database
```

## Migration from Single Database

If you have existing user data in the main database, you'll need to migrate it:

1. Export user data from old database
2. Initialize users database: `uv run cli/initialize_users_db.py`
3. Import user data into new users database

## Backup Strategy

- **Financial Data**: Backed up via `refresh_db_service.py` to GCS
- **Users Database**: Should be backed up separately (not included in financial data backups)

## Thread Safety

Both databases use thread-local storage in the Config class to ensure each thread gets its own connection, preventing SQLite threading errors in multi-threaded Flask environments.
