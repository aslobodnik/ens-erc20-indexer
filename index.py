#!/usr/bin/env python

###############################################################################
# Author: slobo.eth                                                           #
# Last Updated: August 1, 2024                                                #
# Description:                                                                #
# This script indexes the ENS token contract and stores                       #
# the data in a postgres db called voting_power.                              #
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
from psycopg2.extras import execute_batch
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
    REFRESH_VIEWS
    )

#### CONFIG ####


# Postgres connection
from dotenv import load_dotenv
load_dotenv()
dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

CONNECTION_STRING = f"dbname={dbname} user={user} port={port}"

# CONSTANTS
CHUNK_SIZE = 100_000
ENS_CONTRACT = '0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72'
HTTP_PROVIDER = os.getenv("RPC_ENDPOINT")
START_BLOCK = 13_533_418
IS_LOCAL = os.getenv("IS_LOCAL") or False



w3 = Web3(Web3.HTTPProvider(HTTP_PROVIDER,request_kwargs={'timeout': 60})) 
print("RPC Status: ",w3.is_connected())

path = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(path, f'ens_abi.json')) as f: 
    abi = json.load(f)


contract = w3.eth.contract(address=ENS_CONTRACT, abi=abi)


END_BLOCK = w3.eth.block_number
print(f"End block: {END_BLOCK:,}")

#### CONTEXT ####
@contextmanager
def get_db_cursor(connection_string=CONNECTION_STRING, autocommit=False, dict_cursor=False):
    conn = psycopg2.connect(connection_string)
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
        cur.close()
        conn.close()

#### FUNCTIONS ####
def create_db(dbname):
    # Connect to the default 'postgres' database
    conn = psycopg2.connect(
        dbname='postgres',
        user=user,
        port=port
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


def get_events(from_block, to_block, chunk_size, event="Transfer"):
    """
    Retrieve events in chunks to prevent timeout.
    """
    
    start_time = time.time()
    all_events = []

    for chunk_start in range(from_block, to_block, chunk_size):

        section_start_time = time.time()
        chunk_end = min(chunk_start + chunk_size - 1, to_block)

        events_filter = eval(f"contract.events.{event}").create_filter(from_block=chunk_start, to_block=chunk_end)
        events = events_filter.get_all_entries()
        all_events.extend(events)

        print(f"Fetched {len(events):,} events from blocks {chunk_start:,} to {chunk_end:,}")
        end_time = time.time()
        print(f"Section time: {end_time - section_start_time:.2f} seconds")
    
    print(f"Total batch time: {end_time - start_time:.0f} seconds, fetched {len(all_events):,} events")

    return all_events


def add_missing_block_timestamp():
    block_numbers = fetch_block_numbers()
    batch_size = 1000  # Define the size of each batch
    start_time = time.time()

    # Process each batch in parallel
    with ThreadPoolExecutor() as executor:
        for i in range(0, len(block_numbers), batch_size):
            current_batch = list(block_numbers)[i:i + batch_size]
            # Fetch timestamps in parallel
            results = list(executor.map(fetch_timestamp, current_batch))

            update_queries = [result[0] for result in results if result[0]]
            if update_queries:
                execute_queries(update_queries)

            # Logging errors
            errors = [result[1] for result in results if result[1]]
            for error in errors:
                print(error)

    end_time = time.time()
    print(f"Added {len(block_numbers)} timestamps in {end_time - start_time:.2f} seconds")
    
def fetch_timestamp(block_number):
    try:
        timestamp = w3.eth.get_block(block_number)['timestamp']
        return (f"UPDATE events SET block_timestamp = {timestamp} WHERE block_number = {block_number}", None)
    except Exception as e:
        return (None, f"Failed to fetch timestamp for block number {block_number}: {e}")    

def fetch_block_numbers():
    fetch_query = "SELECT distinct(block_number) FROM events WHERE block_timestamp IS NULL order by block_number"
    with get_db_cursor(dict_cursor=True) as cur:
        cur.execute(fetch_query)
        block_numbers = [row['block_number'] for row in cur.fetchall()]
    return block_numbers

def insert_events(events, batch_size=1000):
    insert_query = """
    INSERT INTO events (event_type, args, log_index, transaction_index, transaction_hash, address, block_hash, block_number)
    VALUES %s
    ON CONFLICT (event_type, transaction_hash, log_index) DO NOTHING
    RETURNING id
    """

    def prepare_event(event):
        return (
            event['event'],
            json.dumps(dict(event['args'])),
            event['logIndex'],
            event['transactionIndex'],
            event['transactionHash'].hex(),
            event['address'],
            event['blockHash'].hex(),
            event['blockNumber']
        )

    with get_db_cursor() as cur:
        event_data = [prepare_event(event) for event in events]
        inserted = execute_values(cur, insert_query, event_data, page_size=batch_size, fetch=True)
        print(f"Insert operation completed. {len(inserted):,} out of {len(events):,} events were inserted.")

def execute_queries(query_list):
    with get_db_cursor(dict_cursor=True) as cur:
        for query in query_list:
            cur.execute(query)

def get_latest_block_number(event):
    query = f"""SELECT 
            block_number 
        FROM 
            events 
        WHERE
            event_type = '{event}'
        
        ORDER BY 
            block_number DESC 
        LIMIT 1"""
    
    with psycopg2.connect(CONNECTION_STRING) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query)
            result = cur.fetchone()
            
    if result:
        return result['block_number']
    else:
        return None


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

def update():
    event_types = ['Transfer', 'DelegateChanged', 'DelegateVotesChanged']
    events = []
    for event_type in event_types:
        db_block = get_latest_block_number(event_type)
        if db_block is None:
            db_block = START_BLOCK
        else:
            db_block += 1
        print(f"Latest block for {event_type}: {db_block:,}")
        events.extend(get_events(db_block, END_BLOCK, 100_000, event_type))

    insert_events(events)
    start_time = time.time()
    print("Adding timestamps...")
    add_missing_block_timestamp()
    end_time = time.time()
    print(f"Added timestamps in {end_time - start_time:.2f} seconds")
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
    ])
    update()

if __name__ == "__main__":
    main()
