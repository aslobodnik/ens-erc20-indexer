#!/bin/bash
# Refresh historical token_balances view (runs daily during off-peak hours)
cd /home/slobo/ens-erc20-indexer

echo "$(date): Starting historical token_balances refresh..."
psql -d voting_power -c "REFRESH MATERIALIZED VIEW CONCURRENTLY token_balances;"
echo "$(date): Historical token_balances refresh complete."
