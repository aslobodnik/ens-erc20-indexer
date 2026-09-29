#!/usr/bin/env python

###############################################################################
# Compares the database against the chain at the scan marker block.           #
# Read-only. Exits 1 when anything differs.                                   #
#                                                                             #
#   python verify.py                                                          #
###############################################################################

import sys

from web3 import Web3

from index import ENS_CONTRACT, HTTP_PROVIDER, abi, get_db_cursor, with_retry

ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'
RANDOM_SAMPLE = 50

ADDRESSES = f"""
(SELECT delegate_address AS address FROM current_delegate_power ORDER BY voting_power DESC LIMIT 100)
UNION
(SELECT address FROM balances ORDER BY balance DESC LIMIT 100)
UNION
(SELECT address FROM balances ORDER BY random() LIMIT {RANDOM_SAMPLE});
"""

BALANCE_AT = """
SELECT
    COALESCE((SELECT SUM((args->>'value')::numeric(78,0)) FROM events
              WHERE event_type = 'Transfer' AND args->>'to' = %(a)s AND block_number <= %(b)s), 0)
  - COALESCE((SELECT SUM((args->>'value')::numeric(78,0)) FROM events
              WHERE event_type = 'Transfer' AND args->>'from' = %(a)s AND block_number <= %(b)s), 0);
"""

VOTES_AT = """
SELECT (args->>'newBalance')::numeric(78,0) FROM events
WHERE event_type = 'DelegateVotesChanged'
  AND args @> jsonb_build_object('delegate', %(a)s::text)
  AND block_number <= %(b)s
ORDER BY block_number DESC, log_index DESC
LIMIT 1;
"""


def main():
    w3 = Web3(Web3.HTTPProvider(HTTP_PROVIDER, request_kwargs={'timeout': 60}))
    token = w3.eth.contract(address=ENS_CONTRACT, abi=abi)

    with get_db_cursor() as cur:
        cur.execute("SELECT value FROM indexer_state WHERE key = 'scan_marker'")
        block = cur.fetchone()[0]
        cur.execute(ADDRESSES)
        addresses = [row[0] for row in cur.fetchall() if row[0] != ZERO_ADDRESS]

        print(f"Checking {len(addresses)} addresses at block {block:,}")
        problems = []
        for address in addresses:
            cur.execute(BALANCE_AT, {'a': address, 'b': block})
            db_balance = int(cur.fetchone()[0])
            cur.execute(VOTES_AT, {'a': address, 'b': block})
            row = cur.fetchone()
            db_votes = int(row[0]) if row else 0

            chain_balance = with_retry(
                lambda: token.functions.balanceOf(address).call(block_identifier=block), f"balanceOf {address}")
            chain_votes = with_retry(
                lambda: token.functions.getVotes(address).call(block_identifier=block), f"getVotes {address}")

            if db_balance != chain_balance:
                problems.append(f"balance {address}: db {db_balance} chain {chain_balance} diff {db_balance - chain_balance}")
            if db_votes != chain_votes:
                problems.append(f"votes   {address}: db {db_votes} chain {chain_votes} diff {db_votes - chain_votes}")

    for problem in problems:
        print(problem)
    print(f"{len(problems)} mismatches")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
