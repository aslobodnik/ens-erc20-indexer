# ENS ERC-20 Indexer

## Overview

This indexer tracks ENS token (0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72) events and stores them in a PostgreSQL database called `voting_power`. It processes:
- **Transfer** events (token movements)
- **DelegateChanged** events (delegation changes)
- **DelegateVotesChanged** events (voting power changes)

## Related Projects

### voting-power (aslobodnik/voting-power)
A Next.js web app that queries this database. **Any schema changes here must be tested against voting-power.**

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
- **balances** - Current token balance per address (incremental updates)
- **indexer_state** - Tracks last processed block for incremental updates

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
# Main indexer (every 10 minutes) - updates events + incremental balances
*/10 * * * * /home/slobo/ens-erc20-indexer/update.sh

# Historical balance refresh (daily at 10 AM) - refreshes token_balances
0 10 * * * /home/slobo/ens-erc20-indexer/refresh_historical.sh
```

## Files

- `index.py` - Main indexer script
- `queries.py` - SQL queries for table/view creation and updates
- `update.sh` - Wrapper script for cron
- `refresh_historical.sh` - Daily historical token_balances refresh
- `ens_abi.json` - ENS token contract ABI

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
- Check `indexer_state` table for last processed block: `SELECT * FROM indexer_state;`
