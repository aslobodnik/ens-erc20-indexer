"""
Tests run against a throwaway database whose name must end in _test.
RPC is faked; nothing here talks to the chain or to production.

    DB_NAME=voting_power_test pytest tests/
"""
import json
import os
import sys

import psycopg2
import pytest
from hexbytes import HexBytes
from web3 import Web3
from web3.datastructures import AttributeDict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import index
from queries import CREATE_EVENTS_TABLE, CREATE_STATE_TABLES

TEST_DB = "voting_power_test"
CONN = (
    f"dbname={TEST_DB} user={index.user} port={index.port} "
    f"host={index.host} password={index.password} sslmode=require"
)

ALICE = "0x21a31Ee1afC51d94C2eFcCAa2092aD1028285549"
BOB = "0x43A26434C5b988A693212eE65892276f1D27a5be"
CAROL = "0x5F7E8408e573e934B0df49Ba5b569c30f1eBBaD4"
ZERO = "0x0000000000000000000000000000000000000000"

TOPIC = {name: topic for topic, name in index.TOPICS.items()}


def block_hash(number, fork=0):
    return Web3.keccak(text=f"block-{number}-{fork}")


def pad(address):
    return HexBytes("0x" + "00" * 12 + address[2:].lower())


def word(value):
    return value.to_bytes(32, "big")


def make_log(name, block, log_index, tx, fork=0, **args):
    if name == "Transfer":
        topics = [pad(args["frm"]), pad(args["to"])]
        data = word(args["value"])
    elif name == "DelegateChanged":
        topics = [pad(args["delegator"]), pad(args["from_delegate"]), pad(args["to_delegate"])]
        data = b""
    else:
        topics = [pad(args["delegate"])]
        data = word(args["previous"]) + word(args["new"])
    return AttributeDict({
        "address": index.ENS_CONTRACT,
        "topics": [HexBytes(TOPIC[name])] + topics,
        "data": HexBytes(data),
        "blockNumber": block,
        "blockHash": block_hash(block, fork),
        "transactionHash": Web3.keccak(text=f"tx-{tx}"),
        "transactionIndex": 0,
        "logIndex": log_index,
        "removed": False,
    })


class FakeEth:
    def __init__(self, head, finalized, logs, forks=None):
        self.head = head
        self.finalized = finalized
        self.logs = logs
        self.forks = forks or {}
        self.get_logs_calls = []

    @property
    def block_number(self):
        return self.head

    def get_block(self, ident):
        number = self.finalized if ident == "finalized" else ident
        return {"number": number, "hash": block_hash(number, self.forks.get(number, 0)), "timestamp": 1_700_000_000 + number}

    def get_logs(self, params):
        self.get_logs_calls.append((params["fromBlock"], params["toBlock"]))
        return [l for l in self.logs if params["fromBlock"] <= l["blockNumber"] <= params["toBlock"]]


class FakeW3:
    def __init__(self, *args, **kwargs):
        self.eth = FakeEth(*args, **kwargs)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(index.time, "sleep", lambda s: None)


@pytest.fixture
def db():
    assert TEST_DB.endswith("_test")
    conn = psycopg2.connect(CONN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT current_database()")
    assert cur.fetchone()[0].endswith("_test")
    cur.execute("DROP TABLE IF EXISTS events, balances, indexer_state CASCADE")
    cur.execute(CREATE_EVENTS_TABLE)
    cur.execute(CREATE_STATE_TABLES)
    cur.execute("INSERT INTO indexer_state (key, value) VALUES ('scan_marker', 1000)")
    yield cur
    cur.close()
    conn.close()


def snapshot(cur):
    cur.execute("SELECT event_type, args::text, log_index, transaction_hash, block_hash, block_number, block_timestamp FROM events ORDER BY block_number, log_index")
    events = cur.fetchall()
    cur.execute("SELECT address, balance FROM balances ORDER BY address")
    balances = cur.fetchall()
    cur.execute("SELECT key, value FROM indexer_state ORDER BY key")
    return events, balances, cur.fetchall()


def balance(cur, address):
    cur.execute("SELECT balance FROM balances WHERE address = %s", (address,))
    row = cur.fetchone()
    return None if row is None else int(row[0])


def full_recompute(cur):
    cur.execute("""
        SELECT address, SUM(change) FROM (
            SELECT args->>'from' AS address, -(args->>'value')::numeric AS change FROM events WHERE event_type = 'Transfer'
            UNION ALL
            SELECT args->>'to', (args->>'value')::numeric FROM events WHERE event_type = 'Transfer'
        ) c GROUP BY address ORDER BY address
    """)
    return [(a, int(b)) for a, b in cur.fetchall()]


# 1. chunk ranges
@pytest.mark.parametrize("start,end,chunk,expected", [
    (5, 5, 10, [(5, 5)]),
    (1, 20, 10, [(1, 10), (11, 20)]),
    (1, 21, 10, [(1, 10), (11, 20), (21, 21)]),
    (6, 5, 10, []),
])
def test_chunk_ranges(start, end, chunk, expected):
    w3 = FakeW3(100, 90, [make_log("Transfer", 21, 0, "a", frm=ALICE, to=BOB, value=1)])
    logs = index.fetch_logs(w3, start, end, chunk)
    assert w3.eth.get_logs_calls == expected
    assert len(logs) == (1 if start <= 21 <= end else 0)


# 2. stored format
def test_row_format_matches_existing_rows():
    log = make_log("Transfer", 1001, 7, "a", frm=ALICE, to=BOB, value=29770400000000000000)
    row = index.prepare_rows([log], {1001: 123})[0]
    assert row[0] == "Transfer"
    assert json.loads(row[1]) == {"from": ALICE, "to": BOB, "value": 29770400000000000000}
    assert len(row[4]) == 64 and not row[4].startswith("0x")
    assert len(row[6]) == 64 and not row[6].startswith("0x")
    assert row[5] == index.ENS_CONTRACT
    assert row[8] == 123

    votes = make_log("DelegateVotesChanged", 1001, 8, "a", delegate=BOB, previous=5, new=9)
    assert json.loads(index.prepare_rows([votes], {1001: 1})[0][1]) == {"delegate": BOB, "previousBalance": 5, "newBalance": 9}

    change = make_log("DelegateChanged", 1001, 9, "a", delegator=ALICE, from_delegate=ZERO, to_delegate=BOB)
    assert json.loads(index.prepare_rows([change], {1001: 1})[0][1]) == {"delegator": ALICE, "fromDelegate": ZERO, "toDelegate": BOB}


def test_topics_match_mainnet():
    assert TOPIC["Transfer"] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    assert TOPIC["DelegateChanged"] == "0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f"
    assert TOPIC["DelegateVotesChanged"] == "0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724"


# 3. reorg: same tx moves to a later block on a different fork
def test_window_replaces_reorged_row(db):
    first = [
        make_log("Transfer", 1001, 0, "mint", frm=ZERO, to=ALICE, value=100),
        make_log("Transfer", 1050, 3, "pay", frm=ALICE, to=BOB, value=40),
    ]
    index.scan(FakeW3(1062, 1010, first), CONN)
    assert balance(db, BOB) == 40

    second = [
        first[0],
        make_log("Transfer", 1053, 9, "pay", fork=1, frm=ALICE, to=BOB, value=40),
    ]
    index.scan(FakeW3(1072, 1020, second, forks={1053: 1}), CONN)

    events, _, state = snapshot(db)
    assert [(e[5], e[2]) for e in events] == [(1001, 0), (1053, 9)]
    assert balance(db, ALICE) == 60 and balance(db, BOB) == 40
    assert dict(state)["scan_marker"] == 1020


# 4. touched balances equal a full recompute
def test_balances_match_full_recompute(db):
    logs = [
        make_log("Transfer", 1001, 0, "mint", frm=ZERO, to=ALICE, value=1000),
        make_log("Transfer", 1002, 0, "a", frm=ALICE, to=BOB, value=300),
        make_log("Transfer", 1003, 0, "b", frm=BOB, to=CAROL, value=120),
        make_log("Transfer", 1004, 0, "c", frm=CAROL, to=CAROL, value=50),
    ]
    index.scan(FakeW3(1010, 1002, logs), CONN)
    logs.append(make_log("Transfer", 1009, 0, "d", frm=CAROL, to=ALICE, value=20))
    index.scan(FakeW3(1020, 1008, logs), CONN)

    db.execute("SELECT address, balance FROM balances ORDER BY address")
    assert [(a, int(b)) for a, b in db.fetchall()] == full_recompute(db)
    assert balance(db, ZERO) == -1000
    assert balance(db, ALICE) == 720


# 5. address whose only Transfer vanished ends at 0
def test_vanished_address_goes_to_zero(db):
    logs = [
        make_log("Transfer", 1001, 0, "mint", frm=ZERO, to=ALICE, value=100),
        make_log("Transfer", 1050, 0, "pay", frm=ALICE, to=CAROL, value=10),
    ]
    index.scan(FakeW3(1062, 1010, logs), CONN)
    assert balance(db, CAROL) == 10

    index.scan(FakeW3(1072, 1020, logs[:1]), CONN)
    assert balance(db, CAROL) == 0
    assert balance(db, ALICE) == 100


# 6. empty window still clears old rows and moves the marker
def test_empty_window(db):
    logs = [make_log("Transfer", 1050, 0, "pay", frm=ZERO, to=BOB, value=5)]
    index.scan(FakeW3(1062, 1010, logs), CONN)
    index.scan(FakeW3(1072, 1060, []), CONN)

    events, _, state = snapshot(db)
    assert events == []
    assert balance(db, BOB) == 0
    assert dict(state)["scan_marker"] == 1060


# 7. same input twice changes nothing
def test_idempotent(db):
    logs = [
        make_log("Transfer", 1001, 0, "mint", frm=ZERO, to=ALICE, value=100),
        make_log("DelegateChanged", 1001, 1, "mint", delegator=ALICE, from_delegate=ZERO, to_delegate=BOB),
        make_log("DelegateVotesChanged", 1001, 2, "mint", delegate=BOB, previous=0, new=100),
        make_log("Transfer", 1040, 0, "pay", frm=ALICE, to=BOB, value=1),
    ]
    index.scan(FakeW3(1062, 1010, logs), CONN)
    before = snapshot(db)
    index.scan(FakeW3(1062, 1010, logs), CONN)
    assert snapshot(db) == before
    assert len(before[0]) == 4


# 8. failure mid-transaction leaves everything unchanged
def test_failed_apply_changes_nothing(db):
    logs = [make_log("Transfer", 1001, 0, "mint", frm=ZERO, to=ALICE, value=100)]
    index.scan(FakeW3(1062, 1010, logs), CONN)
    before = snapshot(db)

    # duplicate (event_type, tx, log_index) inside one insert -> unique violation
    bad = logs + [
        make_log("Transfer", 1030, 4, "dup", frm=ALICE, to=BOB, value=1),
        make_log("Transfer", 1031, 4, "dup", frm=ALICE, to=BOB, value=1),
    ]
    with pytest.raises(psycopg2.errors.UniqueViolation):
        index.scan(FakeW3(1072, 1020, bad), CONN)
    assert snapshot(db) == before


# 9. stale RPC answers fail the run and change nothing
@pytest.mark.parametrize("head,finalized", [
    (1062, 900),    # finalized behind marker
    (1062, 1070),   # finalized ahead of head
    (1040, 1010),   # head behind stored events
])
def test_stale_rpc_fails(db, head, finalized):
    logs = [make_log("Transfer", 1050, 0, "pay", frm=ZERO, to=BOB, value=5)]
    index.scan(FakeW3(1062, 1010, logs), CONN)
    before = snapshot(db)

    with pytest.raises(RuntimeError):
        index.scan(FakeW3(head, finalized, logs), CONN)
    assert snapshot(db) == before


def test_double_read_mismatch_blocks_lock(db):
    logs = [make_log("Transfer", 1005, 0, "mint", frm=ZERO, to=ALICE, value=100)]
    w3 = FakeW3(1062, 1010, logs)
    real = w3.eth.get_logs
    calls = {"n": 0}

    def flaky(params):
        calls["n"] += 1
        return real(params) if calls["n"] == 1 else []

    w3.eth.get_logs = flaky
    before = snapshot(db)
    with pytest.raises(RuntimeError):
        index.scan(w3, CONN)
    assert snapshot(db) == before


def test_header_hash_mismatch_fails(db):
    logs = [make_log("Transfer", 1005, 0, "mint", frm=ZERO, to=ALICE, value=100)]
    before = snapshot(db)
    with pytest.raises(RuntimeError):
        index.scan(FakeW3(1062, 1010, logs, forks={1005: 1}), CONN)
    assert snapshot(db) == before


def test_missing_marker_with_events_refuses(db):
    index.scan(FakeW3(1062, 1010, [make_log("Transfer", 1005, 0, "m", frm=ZERO, to=ALICE, value=1)]), CONN)
    db.execute("DELETE FROM indexer_state WHERE key = 'scan_marker'")
    before = snapshot(db)
    with pytest.raises(RuntimeError):
        index.scan(FakeW3(1072, 1020, []), CONN)
    assert snapshot(db) == before


def test_catch_up_in_batches(db, monkeypatch):
    monkeypatch.setattr(index, "MAX_BATCH", 50)
    logs = [
        make_log("Transfer", 1010, 0, "a", frm=ZERO, to=ALICE, value=10),
        make_log("Transfer", 1090, 0, "b", frm=ALICE, to=BOB, value=4),
        make_log("Transfer", 1140, 0, "c", frm=BOB, to=CAROL, value=1),
    ]
    index.scan(FakeW3(1162, 1100, logs), CONN)
    events, _, state = snapshot(db)
    assert [e[5] for e in events] == [1010, 1090, 1140]
    assert dict(state)["scan_marker"] == 1100
    assert dict(state)["last_transfer_block"] == 1140
    assert (balance(db, ALICE), balance(db, BOB), balance(db, CAROL)) == (6, 3, 1)
