#!/usr/bin/env python

###############################################################################
# Re-downloads the full event history into events_audit and compares it to    #
# events. Read-only against events. Only looks at blocks at or below the      #
# scan marker, which the regular run never touches.                           #
#                                                                             #
#   python audit.py fetch     download history (resumable)                    #
#   python audit.py diff      compare and confirm each difference on chain    #
###############################################################################

import json
import os
import sys
import time

from web3 import Web3

import index
from index import (
    ENS_CONTRACT, HTTP_PROVIDER, START_BLOCK, TOPICS, contract,
    fetch_logs, get_db_cursor, hex_no_prefix, with_retry,
)

AUDIT_CHUNK = 5_000
REPORT = os.path.join(index.path, 'audit_report.json')

CREATE_AUDIT_TABLE = """
CREATE TABLE IF NOT EXISTS events_audit (
    event_type VARCHAR(50) NOT NULL,
    args JSONB NOT NULL,
    log_index INTEGER NOT NULL,
    transaction_index INTEGER NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    address VARCHAR(42) NOT NULL,
    block_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL
);
"""

INSERT_AUDIT = """
INSERT INTO events_audit (event_type, args, log_index, transaction_index, transaction_hash, address, block_hash, block_number)
VALUES %s
"""

COLUMNS = "event_type, args, log_index, transaction_index, transaction_hash, address, block_hash, block_number"

# in events, not in the fresh download
EXTRAS = f"""
SELECT {COLUMNS} FROM events e
WHERE e.block_number <= %s AND NOT EXISTS (
    SELECT 1 FROM events_audit a
    WHERE a.event_type = e.event_type AND a.transaction_hash = e.transaction_hash
      AND a.log_index = e.log_index AND a.block_hash = e.block_hash
)
ORDER BY block_number, log_index;
"""

# in the fresh download, not in events
MISSING = f"""
SELECT {COLUMNS} FROM events_audit a
WHERE NOT EXISTS (
    SELECT 1 FROM events e
    WHERE a.event_type = e.event_type AND a.transaction_hash = e.transaction_hash
      AND a.log_index = e.log_index AND a.block_hash = e.block_hash
)
ORDER BY block_number, log_index;
"""

# same log on both sides, different contents
CHANGED = f"""
SELECT e.event_type, e.transaction_hash, e.log_index, e.block_number, e.args, a.args
FROM events e
JOIN events_audit a
  ON a.event_type = e.event_type AND a.transaction_hash = e.transaction_hash
 AND a.log_index = e.log_index AND a.block_hash = e.block_hash
WHERE e.block_number <= %s
  AND (e.args <> a.args OR e.block_number <> a.block_number
       OR e.transaction_index <> a.transaction_index OR e.address <> a.address)
ORDER BY e.block_number, e.log_index;
"""


def get_state(key):
    with get_db_cursor() as cur:
        cur.execute("SELECT value FROM indexer_state WHERE key = %s", (key,))
        row = cur.fetchone()
        return row[0] if row else None


def fetch(w3):
    from psycopg2.extras import execute_values
    from queries import SET_STATE

    with get_db_cursor() as cur:
        cur.execute(CREATE_AUDIT_TABLE)

    end = get_state('audit_end')
    if end is None:
        end = get_state('scan_marker')
        if end is None:
            raise RuntimeError("scan_marker is not set")
        with get_db_cursor() as cur:
            cur.execute(SET_STATE, ('audit_end', end))

    done = get_state('audit_marker') or START_BLOCK - 1
    print(f"Auditing blocks {done + 1:,} to {end:,}")
    start_time = time.time()
    total = 0

    while done < end:
        to_block = min(done + AUDIT_CHUNK, end)
        logs = fetch_logs(w3, done + 1, to_block, AUDIT_CHUNK)
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
            ))
        # rows and progress commit together, so a restart never double loads a chunk
        with get_db_cursor() as cur:
            if rows:
                execute_values(cur, INSERT_AUDIT, rows, page_size=1000)
            cur.execute(SET_STATE, ('audit_marker', to_block))
        done = to_block
        total += len(rows)
        if (done // AUDIT_CHUNK) % 50 == 0:
            pct = (done - START_BLOCK) / (end - START_BLOCK) * 100
            print(f"{done:,} ({pct:.1f}%)  {total:,} events  {time.time() - start_time:.0f}s", flush=True)

    with get_db_cursor() as cur:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_audit_key ON events_audit (transaction_hash, log_index)")
        cur.execute("ANALYZE events_audit")
    print(f"Done. {total:,} events this run in {time.time() - start_time:.0f}s")


def on_chain(w3, row):
    """
    Is this exact log in the canonical chain? True / False, or None when the
    chain gave no clear answer.
    """
    event_type, args, log_index, _, tx_hash, address, block_hash, block_number = row
    try:
        receipt = with_retry(lambda: w3.eth.get_transaction_receipt('0x' + tx_hash), f"receipt {tx_hash[:10]}")
        header = with_retry(lambda: w3.eth.get_block(receipt['blockNumber']), f"get_block {receipt['blockNumber']:,}")
    except Exception:
        return None
    if hex_no_prefix(header['hash']) != hex_no_prefix(receipt['blockHash']):
        return None
    if hex_no_prefix(receipt['blockHash']) != block_hash or receipt['blockNumber'] != block_number:
        return False
    for log in receipt['logs']:
        if log['logIndex'] != log_index:
            continue
        if log['address'] != address or '0x' + hex_no_prefix(log['topics'][0]) not in TOPICS:
            return False
        event = getattr(contract.events, TOPICS['0x' + hex_no_prefix(log['topics'][0])])().process_log(log)
        stored = args if isinstance(args, dict) else json.loads(args)
        return event['event'] == event_type and dict(event['args']) == stored
    return False


def diff(w3):
    end = get_state('audit_end')
    if end is None or get_state('audit_marker') != end:
        raise RuntimeError("fetch has not finished")

    with get_db_cursor() as cur:
        cur.execute(EXTRAS, (end,))
        extras = cur.fetchall()
        cur.execute(MISSING)
        missing = cur.fetchall()
        cur.execute(CHANGED, (end,))
        changed = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM events WHERE block_number <= %s", (end,))
        stored = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM events_audit")
        fresh = cur.fetchone()[0]

    print(f"events <= {end:,}: {stored:,}   fresh download: {fresh:,}")
    print(f"extras: {len(extras)}   missing: {len(missing)}   changed: {len(changed)}")

    def describe(row):
        return dict(zip(COLUMNS.split(', '), row))

    report = {
        'audit_end': end,
        'stored': stored,
        'fresh': fresh,
        # an extra is safe to delete only when the chain says the log is not there
        'extras': [dict(describe(r), on_chain=on_chain(w3, r)) for r in extras],
        # a missing row is safe to insert only when the chain says the log is there
        'missing': [dict(describe(r), on_chain=on_chain(w3, r)) for r in missing],
        'changed': [
            {'event_type': r[0], 'transaction_hash': r[1], 'log_index': r[2], 'block_number': r[3], 'stored_args': r[4], 'fresh_args': r[5]}
            for r in changed
        ],
    }
    with open(REPORT, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    print(f"Report: {REPORT}")
    for kind in ('extras', 'missing'):
        for r in report[kind]:
            print(f"  {kind[:-1] if kind == 'extras' else kind}: {r['event_type']} block {r['block_number']:,} tx {r['transaction_hash'][:10]} log {r['log_index']} on_chain={r['on_chain']}")


if __name__ == "__main__":
    w3 = Web3(Web3.HTTPProvider(HTTP_PROVIDER, request_kwargs={'timeout': 60}))
    command = sys.argv[1] if len(sys.argv) > 1 else ''
    if command == 'fetch':
        fetch(w3)
    elif command == 'diff':
        diff(w3)
    else:
        print(__doc__ or "usage: audit.py fetch | diff")
        sys.exit(1)
