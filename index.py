#!/usr/bin/env python

###############################################################################
# Author: slobo.eth                                                           #
# Last Updated: September 28, 2026                                            #
# Description:                                                                #
# This script indexes the ENS token contract and stores                       #
# the data in a postgres db called voting_power.                              #
#                                                                             #
# Each run rescans everything above scan_marker (the last finalized block     #
# that was fully scanned) and replaces those rows, so reorged blocks fix      #
# themselves. Rows at or below the marker are never touched.                  #
###############################################################################


#### IMPORTS ####
from web3 import Web3
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
from psycopg2.extras import DictCursor
from contextlib import contextmanager
from queries import (
    CREATE_EVENTS_TABLE,
    CREATE_TOKEN_BALANCES_VIEW,
    CREATE_DELEGATE_POWER_VIEW,
    CREATE_CURRENT_DELEGATIONS_VIEW,
    CREATE_CURRENT_TOKEN_BALANCE_VIEW,
    CREATE_CURRENT_DELEGATE_POWER_VIEW,
    CREATE_TOKEN_BALANCES_TOP_1000_VIEW,
    CREATE_DELEGATE_POWER_TOP_100_VIEW,
    REFRESH_VIEWS,
    CREATE_STATE_TABLES,
    GET_SCAN_MARKER,
    SET_STATE,
    GET_MAX_STORED_BLOCK,
    GET_TRANSFER_ADDRESSES_ABOVE,
    DELETE_EVENTS_ABOVE,
    INSERT_EVENTS,
    RECOMPUTE_BALANCES_FOR_ADDRESSES,
    GET_MAX_TRANSFER_BLOCK,
    )

#### CONFIG ####


# Postgres connection
from dotenv import load_dotenv
load_dotenv()
dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
password = os.getenv("DB_PASSWORD")

CONNECTION_STRING = f"dbname={dbname} user={user} port={port} host={host} password={password} sslmode=require"

# CONSTANTS
CHUNK_SIZE = 1_000
MAX_BATCH = 20_000      # blocks per transaction when catching up
HEAD_BUFFER = 2         # stay this many blocks behind head
RETRIES = 4
ENS_CONTRACT = '0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72'
HTTP_PROVIDER = os.getenv("RPC_ENDPOINT")
START_BLOCK = 13_533_418
IS_LOCAL = os.getenv("IS_LOCAL") or False
EVENT_NAMES = ['Transfer', 'DelegateChanged', 'DelegateVotesChanged']

path = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(path, f'ens_abi.json')) as f:
    abi = json.load(f)

contract = Web3().eth.contract(address=ENS_CONTRACT, abi=abi)


def hex_no_prefix(value):
    """Hashes are stored as 64 hex chars without 0x."""
    h = value.hex() if hasattr(value, 'hex') else str(value)
    return h[2:] if h.startswith('0x') else h


# topic0 -> event name
TOPICS = {}
for _name in EVENT_NAMES:
    _abi = getattr(contract.events, _name)().abi
    _sig = f"{_name}({','.join(i['type'] for i in _abi['inputs'])})"
    TOPICS['0x' + hex_no_prefix(Web3.keccak(text=_sig))] = _name


#### CONTEXT ####
@contextmanager
def get_db_cursor(connection_string=None, autocommit=False, dict_cursor=False):
    conn = psycopg2.connect(connection_string or CONNECTION_STRING)
    cur = None
    try:
        if autocommit:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor(cursor_factory=DictCursor if dict_cursor else None)
        yield cur
        if not autocommit:
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cur is not None:
            cur.close()
        conn.close()

#### FUNCTIONS ####
def create_db(dbname):
    # Connect to the default 'postgres' database
    conn = psycopg2.connect(
        dbname='postgres',
        user=user,
        port=port,
        sslmode='require'
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        with conn.cursor() as cur:
            # Check if the database exists
            cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))
            exists = cur.fetchone()

            if not exists:
                # Create the database if it doesn't exist
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
                print(f"Database {dbname} created successfully.")
            else:
                print(f"Database {dbname} already exists.")
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

def create_events_table():
    with get_db_cursor() as cur:
        cur.execute("SELECT to_regclass('public.events');")
        table_exists = cur.fetchone()[0] is not None

        if table_exists:
            print("Events table already exists. Skipping creation.")
        else:
            cur.execute(CREATE_EVENTS_TABLE)
            print("Events table & indexes created successfully.")
        cur.execute(CREATE_STATE_TABLES)


def with_retry(fn, what, tries=RETRIES):
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except Exception as e:
            print(f"{what} failed (attempt {attempt}/{tries}): {e}")
            if attempt == tries:
                raise
            time.sleep(attempt)


def is_limit_error(e):
    msg = str(e).lower()
    if 'beyond' in msg or 'pruned' in msg:
        return False
    return any(word in msg for word in ('limit', 'too many', 'more than', 'exceed', 'response size', 'too large'))


def get_chain_state(w3, marker, max_stored):
    """
    Read head and finalized. The RPC is load balanced, so a lagging backend
    can answer with old numbers. Retry until the answers make sense.
    """
    problem = None
    for attempt in range(1, RETRIES + 1):
        head = w3.eth.block_number
        finalized = w3.eth.get_block('finalized')['number']
        if finalized < marker:
            problem = f"finalized {finalized:,} is behind marker {marker:,}"
        elif finalized > head:
            problem = f"finalized {finalized:,} is ahead of head {head:,}"
        elif head - HEAD_BUFFER < max_stored:
            problem = f"head {head:,} is behind stored events {max_stored:,}"
        else:
            return head, finalized
        print(f"Stale RPC answer (attempt {attempt}/{RETRIES}): {problem}")
        time.sleep(attempt)
    raise RuntimeError(f"RPC kept giving stale answers: {problem}")


def fetch_logs(w3, from_block, to_block, chunk_size=CHUNK_SIZE):
    """
    Retrieve all three event types in chunks. Ranges are inclusive.
    """
    all_logs = []
    start = from_block
    size = chunk_size

    while start <= to_block:
        end = min(start + size - 1, to_block)
        params = {
            'address': ENS_CONTRACT,
            'fromBlock': start,
            'toBlock': end,
            'topics': [list(TOPICS.keys())],
        }
        try:
            logs = with_retry(lambda: w3.eth.get_logs(params), f"get_logs {start:,}-{end:,}")
        except Exception as e:
            if size > 1 and is_limit_error(e):
                size = max(1, size // 2)
                print(f"Shrinking chunk to {size:,} blocks")
                continue
            raise
        all_logs.extend(log for log in logs if not log.get('removed'))
        start = end + 1

    all_logs.sort(key=lambda log: (log['blockNumber'], log['logIndex']))
    return all_logs


def log_key(log):
    return (
        log['blockNumber'],
        hex_no_prefix(log['blockHash']),
        hex_no_prefix(log['transactionHash']),
        log['logIndex'],
        tuple(hex_no_prefix(t) for t in log['topics']),
        hex_no_prefix(log['data']),
    )


def confirm_logs(w3, logs, from_block, lock_to):
    """
    Blocks up to lock_to are about to be locked for good. Read them a second
    time and require the same answer.
    """
    if lock_to < from_block:
        return
    first = sorted(log_key(log) for log in logs if log['blockNumber'] <= lock_to)
    second = sorted(log_key(log) for log in fetch_logs(w3, from_block, lock_to))
    if first != second:
        raise RuntimeError(
            f"Two reads of blocks {from_block:,}-{lock_to:,} disagree "
            f"({len(first)} vs {len(second)} logs). Not locking."
        )


def fetch_block_timestamps(w3, logs):
    """
    Timestamp for every block that has logs. The header hash must match the
    hash on the logs, otherwise the logs came from a different fork.
    """
    expected = {}
    for log in logs:
        block_hash = hex_no_prefix(log['blockHash'])
        if expected.setdefault(log['blockNumber'], block_hash) != block_hash:
            raise RuntimeError(f"Block {log['blockNumber']:,} has logs with two different hashes")

    def fetch(block_number):
        block = with_retry(lambda: w3.eth.get_block(block_number), f"get_block {block_number:,}")
        if hex_no_prefix(block['hash']) != expected[block_number]:
            raise RuntimeError(f"Block {block_number:,} header hash does not match its logs")
        return block_number, block['timestamp']

    with ThreadPoolExecutor(max_workers=8) as executor:
        return dict(executor.map(fetch, expected.keys()))


def prepare_rows(logs, timestamps):
    rows = []
    for log in logs:
        name = TOPICS['0x' + hex_no_prefix(log['topics'][0])]
        event = getattr(contract.events, name)().process_log(log)
        rows.append((
            event['event'],
            json.dumps(dict(event['args'])),
            event['logIndex'],
            event['transactionIndex'],
            hex_no_prefix(event['transactionHash']),
            event['address'],
            hex_no_prefix(event['blockHash']),
            event['blockNumber'],
            timestamps[event['blockNumber']],
        ))
    return rows


def get_scan_marker(connection_string=None):
    """
    Returns the marker, or None on a fresh database. Refuses to guess when
    events exist without a marker: a wrong marker would delete good rows.
    """
    with get_db_cursor(connection_string) as cur:
        cur.execute(GET_SCAN_MARKER)
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("SELECT EXISTS (SELECT 1 FROM events)")
        if cur.fetchone()[0]:
            raise RuntimeError("events has rows but scan_marker is not set. Seed scan_marker first.")
        return None


def get_max_stored_block(connection_string=None):
    with get_db_cursor(connection_string) as cur:
        cur.execute(GET_MAX_STORED_BLOCK)
        return cur.fetchone()[0]


def apply_window(marker, rows, new_marker, connection_string=None):
    """
    One transaction: replace every row above the marker, recompute balances
    for the addresses involved, move the marker.
    """
    with get_db_cursor(connection_string) as cur:
        cur.execute(GET_TRANSFER_ADDRESSES_ABOVE, (marker, marker))
        touched = {row[0] for row in cur.fetchall()}
        for row in rows:
            if row[0] == 'Transfer':
                args = json.loads(row[1])
                touched.update((args['from'], args['to']))

        cur.execute(DELETE_EVENTS_ABOVE, (marker,))
        deleted = cur.rowcount
        if rows:
            execute_values(cur, INSERT_EVENTS, rows, page_size=1000)
        if touched:
            cur.execute(RECOMPUTE_BALANCES_FOR_ADDRESSES, (new_marker, sorted(touched)))

        # kept in step so the pre-marker code can take over again without a rebuild
        cur.execute(GET_MAX_TRANSFER_BLOCK)
        cur.execute(SET_STATE, ('last_transfer_block', cur.fetchone()[0]))
        cur.execute(SET_STATE, ('scan_marker', new_marker))

    print(f"Replaced {deleted:,} rows with {len(rows):,}. Updated {len(touched):,} balances. Marker: {new_marker:,}")


def execute_queries(query_list, connection_string=None):
    with get_db_cursor(connection_string, dict_cursor=True) as cur:
        for query in query_list:
            cur.execute(query)


def check_if_view_exists(table_name):
    query = """
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
        AND c.relname = %s
        AND c.relkind IN ('r', 'v', 'm')
    );
    """

    with get_db_cursor() as cur:
        cur.execute(query, (table_name,))
        result = cur.fetchone()

    return result[0] if result else False


def scan(w3, connection_string=None):
    """
    Bring events, balances and the marker up to head. Normally one batch;
    several when catching up after downtime.
    """
    while True:
        marker = get_scan_marker(connection_string)
        if marker is None:
            marker = START_BLOCK - 1
        head, finalized = get_chain_state(w3, marker, get_max_stored_block(connection_string))

        from_block = marker + 1
        target = head - HEAD_BUFFER
        # batches only split finalized history, so every batch moves the marker
        batch_end = from_block + MAX_BATCH - 1
        to_block = batch_end if batch_end < finalized else target
        if to_block < from_block:
            print("Nothing new to scan.")
            return
        print(f"Marker: {marker:,}  finalized: {finalized:,}  head: {head:,}")

        start_time = time.time()
        logs = fetch_logs(w3, from_block, to_block)
        new_marker = min(finalized, to_block)
        confirm_logs(w3, logs, from_block, new_marker)
        timestamps = fetch_block_timestamps(w3, logs)
        rows = prepare_rows(logs, timestamps)
        print(f"Fetched {len(rows):,} events from blocks {from_block:,} to {to_block:,} in {time.time() - start_time:.2f} seconds")

        apply_window(marker, rows, new_marker, connection_string)

        if to_block >= target:
            return


def update():
    w3 = Web3(Web3.HTTPProvider(HTTP_PROVIDER, request_kwargs={'timeout': 60}))
    print("RPC Status: ", w3.is_connected())

    scan(w3)

    # Refresh remaining materialized views
    print("Refreshing views...")
    start_time = time.time()
    execute_queries([REFRESH_VIEWS])
    end_time = time.time()
    print(f"Views refreshed in {end_time - start_time:.2f} seconds")


def main():
    if IS_LOCAL:
        create_db(dbname)
    create_events_table()
    if check_if_view_exists("token_balances"):
        print("Views already exist. Skipping creation.")
    else:
        execute_queries([
        CREATE_TOKEN_BALANCES_VIEW,
        CREATE_CURRENT_TOKEN_BALANCE_VIEW,
        CREATE_DELEGATE_POWER_VIEW,
        CREATE_CURRENT_DELEGATIONS_VIEW,
        CREATE_CURRENT_DELEGATE_POWER_VIEW,
        CREATE_TOKEN_BALANCES_TOP_1000_VIEW,
        CREATE_DELEGATE_POWER_TOP_100_VIEW,
    ])
    update()

if __name__ == "__main__":
    main()
