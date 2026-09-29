# ENS ERC-20 Indexer

## Overview

This indexer tracks ENS token (0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72) events and stores them in a PostgreSQL database called `voting_power`. It processes:
- **Transfer** events (token movements)
- **DelegateChanged** events (delegation changes)
- **DelegateVotesChanged** events (voting power changes)

## Related Projects

### voting-power (~/voting-power)
**GitHub:** `aslobodnik/voting-power`

A Next.js web app that queries this database. **Any schema changes here must be tested against voting-power.**

See `~/voting-power/CLAUDE.md` for:

### ens-governor-indexer (~/ens-governor-indexer)
Indexes ENS Governor proposals and votes into `ens_governor` database. The voting-power app uses both databases.

See `~/ens-governor-indexer/CLAUDE.md` for:
- Proposals and votes schema
- Vote support values (0=Against, 1=For, 2=Abstain)
- Number of executables per proposal

---

**voting-power CLAUDE.md includes:**
- API endpoint documentation
- Frontend structure
- How each endpoint uses the database

The voting-power app queries these tables/views:
- `top_100_delegates` - Top 100 delegates by voting power
- `top_1000_holders` - Top 1000 token holders
- `current_delegations` - Current delegation state for each delegator
- `current_delegate_power` - Current voting power per delegate
- `delegate_power` - Historical voting power changes
- `events` - Raw event data

**It does NOT directly query:** `token_balances`, `current_token_balances`, `balances`

## Database Schema

### Tables
- **events** - Raw blockchain events (Transfer, DelegateChanged, DelegateVotesChanged)
- **balances** - Current token balance per address (recomputed from `events` for every address touched in a run)
- **indexer_state** - `scan_marker` (see below) and `last_transfer_block` (kept current so the pre-marker code could take over again)

### Materialized Views
- **token_balances** - Historical balance at each block (refreshed daily at 10 AM)
- **delegate_power** - Historical voting power from DelegateVotesChanged events
- **delegate_power_changes** - Pre-computed voting power changes with LAG (avoids expensive window function at query time)
- **recent_activity** - Pre-computed recent activity feed (delegation changes + token movements)
- **current_delegate_power** - Latest voting power per delegate
- **current_delegations** - Latest delegation per delegator (joins with balances)
- **top_100_delegates** - Aggregated top 100 delegates
- **top_1000_holders** - Top 1000 holders from token_balances

### Views
- **current_token_balances** - Simple view over `balances` table (was previously a materialized view)

## How a run works (September 2026)

`scan_marker` is the highest block that is fully scanned and finalized. It only moves forward.

1. Read head and finalized. Retry if the load-balanced RPC answers with stale numbers.
2. Fetch all three event types with one `eth_getLogs` filter from `scan_marker + 1` to `head - 2`.
3. Read the part about to be locked a second time. Both reads must match.
4. Fetch block headers for timestamps. Header hash must match the hash on the logs.
5. One transaction: delete rows above the marker, insert the fresh rows, recompute balances for touched addresses, move the marker.
6. Refresh the materialized views.

Rows at or below the marker are never touched by a run. Rows above it are replaced every run until they finalize, so reorged blocks fix themselves. A failed run changes nothing and the next run retries.

Before this, the start block per event type was `MAX(block_number) + 1`. `DelegateChanged` is rare, so that scan reached weeks back and hit RPC nodes that had pruned those logs (about 1 in 4 runs failed).

If `events` has rows and `scan_marker` is missing, the run refuses to start. Seed the marker by hand; a wrong marker deletes good rows.

### Tools
- `verify.py` - compares balances and voting power for about 250 addresses against the chain at the marker block. Exits 1 on any mismatch.
- `audit.py fetch` / `audit.py diff` - re-downloads the full history into `events_audit`, compares it to `events`, confirms each difference by transaction receipt, writes `audit_report.json`.
- `repair.py audit_report.json` - applies a report, rebuilds `balances`, refreshes views. Safe to run twice.
- `tests/` - `pytest tests/` against a database named `voting_power_test`. RPC is faked.

### Deploy
The box (`fullrange-2`, `~/ens/ens-erc20-indexer`) has no GitHub access. Copy files from a checkout with `rsync`. `update.sh` lives only on the box.

## Performance Optimizations (January 2026)

### Problem
The original design refreshed all materialized views on every update (every 10 minutes). The `token_balances` view was particularly expensive:
- Scanned 1.4M+ Transfer events
- Doubled rows via UNION ALL
- Computed cumulative balances with window functions
- Caused 98% CPU usage for several minutes

### Solution
1. **Incremental balance updates** - Created `balances` table that updates incrementally instead of recomputing everything
2. **Expression indexes** - Added indexes on `args->>'from'` and `args->>'to'` for Transfer events
3. **Separated historical refresh** - `token_balances` now refreshes daily (10 AM) instead of every 10 minutes
4. **Simplified current_token_balances** - Changed from materialized view to regular view over `balances` table

### Results
- Balance updates: **0.01 seconds** (was minutes)
- Storage: 56 MB for `balances` vs 976 MB for `token_balances`
- No impact on voting-power app (doesn't query affected views directly)

## Recent Activity Optimization (January 2026)

### Problem
The `/api/get-recent-activity` endpoint was slow (~1+ second) because it:
1. Computed LAG window function over ALL 307k rows in delegate_power
2. Used correlated subqueries for each DelegateChanged event
3. Complex JOINs with current_delegations

### Solution
1. **delegate_power_changes** - Pre-computed materialized view with voting_power_change already calculated
2. **recent_activity** - Pre-computed materialized view with all activity classifications done

### Results
- Recent activity query: **2ms** (was 1000ms+) - **500x improvement**
- The voting-power app can query `recent_activity` directly instead of computing everything on each request

### Usage in voting-power
Update `/api/get-recent-activity/route.tsx` to use:
```sql
SELECT * FROM recent_activity
WHERE amount >= $threshold
ORDER BY block_number DESC, log_index DESC;
```

## Cron Jobs

```
# Main indexer (every 10 minutes, UTC box)
5-59/10 * * * * flock -n /tmp/ens-erc20.lock /home/ubuntu/ens/ens-erc20-indexer/update.sh

# Historical balance refresh (daily 14:00 UTC) - refreshes token_balances
0 14 * * * flock -n /tmp/ens-erc20.lock /home/ubuntu/ens/ens-erc20-indexer/refresh_historical.sh
```

## Files

- `index.py` - Main indexer script
- `queries.py` - SQL queries for table/view creation and updates
- `update.sh` - Wrapper script for cron
- `refresh_historical.sh` - Daily historical token_balances refresh
- `ens_abi.json` - ENS token contract ABI
- `verify.py`, `audit.py`, `repair.py` - data checks and repair (see above)

## Database Backup

```bash
# Create backup
pg_dump voting_power > ~/voting_power_backup_$(date +%Y%m%d).sql

# Restore backup
psql voting_power < ~/voting_power_backup_YYYYMMDD.sql
```

## Maintenance Notes

- Before making schema changes, back up the database
- After schema changes, verify voting-power app still works
- The `token_balances` view can be manually refreshed: `REFRESH MATERIALIZED VIEW CONCURRENTLY token_balances;`
- Check `indexer_state` for the scan marker: `SELECT * FROM indexer_state;`
