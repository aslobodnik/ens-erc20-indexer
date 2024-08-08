# ERC-20 Indexer

## Description

ERC-20 token indexer for ENS powering [votingpower.xyz](https://www.votingpower.xyz/). When run against a local node should take less than 15 minutes from start to finish.

## Tested with:

- Python 3.12.4
- PostgreSQL 15+
- Web3.py @7.09b
- Local Reth RPC archival node

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/erc20-indexer.git
   cd erc20-indexer
   ```
2. Create venv:
   ```
   python -m venv .venv
   ```
3. Install required packages:

   ```
   pip install -r requirements.txt
   ```

4. Configure PostgreSQL db in .env
   ```
   DB_NAME=
   DB_USER=
   DB_HOST=
   DB_PORT=
   ```

_Ubutu_

On linux need the libpq required for psycopg2 to build

`sudo apt install libpq-dev`

## Usage

To run the indexer:

```
python index.py
```

## Database Schema

```
+------------------------+        +---------------------------+
|        events          |        | current_delegate_power    |
+------------------------+        +---------------------------+
| id                     |        | delegate_address          |
| event_type             |        | voting_power              |
| args                   |        | block_number              |
| log_index              |        | log_index                 |
| transaction_index      |        | last_refreshed            |
| transaction_hash       |        +---------------------------+
| address                |
| block_hash             |
| block_number           |        +---------------------------+
| block_timestamp        |        | current_delegations       |
| created_at             |        +---------------------------+
+------------------------+        | delegator                 |
                                  | delegate                  |
                                  | prior_delegate            |
+------------------------+        | last_updated_block        |
| current_token_balances |        +---------------------------+
+------------------------+
| address                |        +---------------------------+
| latest_block_number    |        | delegate_power            |
| current_balance        |        +---------------------------+
+------------------------+        | delegate_address          |
                                  | voting_power              |
                                  | block_number              |
+------------------------+        | log_index                 |
| token_balances         |        | last_refreshed            |
+------------------------+        +---------------------------+
| address                |
| block_number           |        +---------------------------+
| balance                |        | delegation_counts         |
| unique_id              |        +---------------------------+
+------------------------+        | delegate_address          |
                                  | total_delegations         |
                                  | non_zero_delegations      |
+------------------------+        +---------------------------+
| top_1000_holders       |
+------------------------+
| address                |
| balance                |
| block_number           |
| rank                   |
+------------------------+
```

Note: All structures except 'events' are materialized views.
