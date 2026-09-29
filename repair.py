#!/usr/bin/env python

###############################################################################
# Applies an audit report: removes rows the chain says do not exist, adds     #
# rows the chain says are missing, then rebuilds balances and views.          #
# Only acts on rows confirmed on chain. Safe to run twice.                    #
#                                                                             #
#   python repair.py audit_report.json                                        #
###############################################################################

import json
import sys
import time

from psycopg2.extras import execute_values
from web3 import Web3

from index import HTTP_PROVIDER, dbname, execute_queries, get_db_cursor, hex_no_prefix, with_retry
from queries import GET_MAX_TRANSFER_BLOCK, GET_SCAN_MARKER, INSERT_EVENTS, REBUILD_ALL_BALANCES, REFRESH_VIEWS, SET_STATE

KEY = "event_type = %(event_type)s AND transaction_hash = %(transaction_hash)s AND log_index = %(log_index)s"


def main(report_path):
    with open(report_path) as f:
        report = json.load(f)

    extras = [r for r in report['extras'] if r['on_chain'] is False]
    missing = [r for r in report['missing'] if r['on_chain'] is True]
    unconfirmed = len(report['extras']) + len(report['missing']) - len(extras) - len(missing)
    print(f"Database: {dbname}")
    print(f"Removing {len(extras)}, adding {len(missing)}, leaving {unconfirmed} unconfirmed, {len(report['changed'])} changed rows not handled")

    w3 = Web3(Web3.HTTPProvider(HTTP_PROVIDER, request_kwargs={'timeout': 60}))
    timestamps = {}
    for row in missing:
        block = with_retry(lambda: w3.eth.get_block(row['block_number']), f"get_block {row['block_number']:,}")
        if hex_no_prefix(block['hash']) != row['block_hash']:
            raise RuntimeError(f"Block {row['block_number']:,} hash on chain does not match the report")
        timestamps[row['block_number']] = block['timestamp']

    with get_db_cursor() as cur:
        cur.execute(GET_SCAN_MARKER)
        marker = cur.fetchone()[0]
        for row in extras + missing:
            if row['block_number'] > marker:
                raise RuntimeError(f"Block {row['block_number']:,} is above the scan marker {marker:,}")

        removed = 0
        for row in extras:
            cur.execute(f"DELETE FROM events WHERE {KEY} AND block_hash = %(block_hash)s", row)
            removed += cur.rowcount

        to_add = []
        for row in missing:
            cur.execute(f"SELECT block_hash FROM events WHERE {KEY}", row)
            existing = cur.fetchone()
            if existing and existing[0] == row['block_hash']:
                continue
            if existing:
                raise RuntimeError(f"{row['transaction_hash']} log {row['log_index']} is stored under another block hash")
            to_add.append((
                row['event_type'], json.dumps(row['args']), row['log_index'], row['transaction_index'],
                row['transaction_hash'], row['address'], row['block_hash'], row['block_number'],
                timestamps[row['block_number']],
            ))
        if to_add:
            execute_values(cur, INSERT_EVENTS, to_add)

        cur.execute(REBUILD_ALL_BALANCES)
        cur.execute(GET_MAX_TRANSFER_BLOCK)
        cur.execute(SET_STATE, ('last_transfer_block', cur.fetchone()[0]))

        cur.execute("SELECT COUNT(*), SUM(balance), COUNT(*) FILTER (WHERE balance < 0) FROM balances")
        count, total, negatives = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM (SELECT 1 FROM events GROUP BY transaction_hash HAVING COUNT(DISTINCT block_hash) > 1) d")
        doubles = cur.fetchone()[0]
        print(f"Removed {removed}, added {len(to_add)}")
        print(f"Balances: {count:,} rows, sum {total}, negative {negatives}. Transactions under two block hashes: {doubles}")
        if total != 0 or negatives != 1 or doubles != 0:
            raise RuntimeError("Checks failed. Nothing was changed.")

    print("Refreshing views...")
    start_time = time.time()
    execute_queries(["REFRESH MATERIALIZED VIEW CONCURRENTLY token_balances;"])
    execute_queries([REFRESH_VIEWS])
    print(f"Views refreshed in {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: repair.py audit_report.json")
        sys.exit(1)
    main(sys.argv[1])
