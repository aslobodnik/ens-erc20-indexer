
ONE_ENS = 10**18 #1000000000000000000
# ******* TABLES *******

CREATE_EVENTS_TABLE = """
    CREATE TABLE events (
        id BIGSERIAL PRIMARY KEY,
        event_type VARCHAR(50) NOT NULL,
        args JSONB NOT NULL,
        log_index INTEGER NOT NULL,
        transaction_index INTEGER NOT NULL,
        transaction_hash VARCHAR(66) NOT NULL,
        address VARCHAR(42) NOT NULL,
        block_hash VARCHAR(66) NOT NULL,
        block_number BIGINT NOT NULL,
        block_timestamp BIGINT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (event_type, transaction_hash, log_index)
    );

    CREATE INDEX idx_events_event_type ON events(event_type);
    CREATE INDEX idx_events_block_number ON events(block_number);
    CREATE INDEX idx_events_address ON events(address);
    CREATE INDEX idx_events_args ON events USING GIN (args);
"""

# ******* MATERIALIZED VIEWS *******
CREATE_TOKEN_BALANCES_VIEW = """
CREATE MATERIALIZED VIEW token_balances AS
WITH transfers AS (
    SELECT 
        (args->>'from')::varchar(42) AS from_address,
        (args->>'to')::varchar(42) AS to_address,
        (args->>'value')::numeric(78,0) AS value,
        block_number
    FROM events
    WHERE event_type = 'Transfer'
),
balance_changes AS (
    SELECT from_address AS address, -value AS change, block_number FROM transfers
    UNION ALL
    SELECT to_address AS address, value AS change, block_number FROM transfers
),
block_balances AS (
    SELECT 
        address,
        block_number,
        SUM(change) AS block_change
    FROM balance_changes
    GROUP BY address, block_number
),
cumulative_balances AS (
    SELECT 
        address,
        block_number,
        SUM(block_change) OVER (
            PARTITION BY address 
            ORDER BY block_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance
    FROM block_balances
)
SELECT 
    address,
    block_number,
    balance,
    ROW_NUMBER() OVER (ORDER BY address, block_number) AS unique_id
FROM cumulative_balances
ORDER BY block_number, address;

-- Create indexes for better query performance
CREATE UNIQUE INDEX ON token_balances (address, block_number);
CREATE INDEX ON token_balances (block_number);

-- Create a unique index for concurrent refresh
-- CREATE UNIQUE INDEX token_balances_unique_idx ON token_balances (unique_id);
"""

CREATE_TOKEN_BALANCES_TOP_1000_VIEW = """
CREATE MATERIALIZED VIEW top_1000_holders AS
WITH latest_balances AS (
  SELECT DISTINCT ON (address)
    tb.address,
    tb.balance,
    tb.block_number,
    (SELECT block_timestamp FROM events WHERE block_number = tb.block_number LIMIT 1) as block_timestamp
  FROM token_balances tb
  ORDER BY address, block_number DESC
),
balances_with_rank AS (
  SELECT 
    address,
    balance AS current_balance,
    block_number,
    block_timestamp,
    ROW_NUMBER() OVER (ORDER BY balance DESC) AS rank
  FROM latest_balances
  WHERE balance > 0
  ORDER BY balance DESC
  LIMIT 1000
)
SELECT 
  b.rank,
  b.address,
  b.current_balance as balance,
  COALESCE(old_b.balance, 0) AS balance_30d_ago,
  b.block_number,
  b.block_timestamp,
  (b.current_balance - COALESCE(old_b.balance, 0)) AS balance_change_30d
FROM balances_with_rank b
LEFT JOIN LATERAL (
  SELECT tb.balance
  FROM token_balances tb
  JOIN events e ON e.block_number = tb.block_number
  WHERE tb.address = b.address
    AND e.block_timestamp <= (EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '30 days')))::bigint
  ORDER BY tb.block_number DESC
  LIMIT 1
) old_b ON TRUE
ORDER BY b.current_balance DESC;

-- Create indexes
CREATE UNIQUE INDEX ON top_1000_holders (rank);
CREATE INDEX ON top_1000_holders (address);
CREATE INDEX ON top_1000_holders (balance DESC);
CREATE INDEX ON top_1000_holders (balance_change_30d);
"""


# Delegate Voting Power Materialized View
CREATE_CURRENT_DELEGATE_POWER_VIEW = """
CREATE MATERIALIZED VIEW current_delegate_power AS
SELECT DISTINCT ON (delegate_address)
    delegate_address,
    voting_power,
    block_number,
    block_timestamp,
    log_index,
    last_refreshed
FROM delegate_power
ORDER BY delegate_address, block_number DESC, log_index DESC;

CREATE UNIQUE INDEX idx_current_delegate_power_address ON current_delegate_power(delegate_address);
"""

CREATE_DELEGATE_POWER_VIEW = """
CREATE MATERIALIZED VIEW delegate_power AS
SELECT
    (events.args ->> 'delegate')::character varying(42) AS delegate_address,
    (events.args ->> 'newBalance')::numeric(78,0) AS voting_power,
    events.block_number,
    events.block_timestamp,
    events.log_index,
    CURRENT_TIMESTAMP AS last_refreshed
FROM
    events
WHERE
    events.event_type = 'DelegateVotesChanged'
ORDER BY
    (events.args ->> 'delegate')::character varying(42),
    events.block_number,
    events.log_index;

CREATE UNIQUE INDEX idx_delegate_power_address_block
ON delegate_power (delegate_address, block_number, log_index);"""




CREATE_CURRENT_DELEGATIONS_VIEW ="""
CREATE MATERIALIZED VIEW current_delegations AS
WITH ranked_delegations AS (
    SELECT 
        args->>'delegator' as delegator,
        args->>'toDelegate' as delegate,
        args->>'fromDelegate' as prior_delegate,
        e.block_number,
        e.block_timestamp,
        ROW_NUMBER() OVER (PARTITION BY args->>'delegator' ORDER BY e.block_number DESC, e.log_index desc) as rn,
        COALESCE(b.current_balance, 0) as delegator_balance
    FROM 
        events e
    LEFT JOIN
        current_token_balances b
        ON b.address = args->>'delegator'
    WHERE
        event_type = 'DelegateChanged'
)
SELECT 
    delegator,
    delegator_balance,
    delegate,
    prior_delegate,
    block_timestamp as delegated_timestamp
FROM 
    ranked_delegations
WHERE 
    rn = 1;
CREATE UNIQUE INDEX idx_current_delegations_delegator ON current_delegations(delegator);
CREATE INDEX idx_current_delegations_delegate ON current_delegations(delegate);
"""

CREATE_CURRENT_TOKEN_BALANCE_VIEW ="""
CREATE MATERIALIZED VIEW current_token_balances AS
WITH ranked_balances AS (
    SELECT 
        address,
        block_number,
        balance,
        ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_number DESC) as rn
    FROM 
        token_balances
)
SELECT 
    address,
    block_number as latest_block_number,
    balance as current_balance
FROM 
    ranked_balances
WHERE 
    rn = 1
    --AND balance != 0
ORDER BY 
    address;

-- Create indexes on the materialized view for faster lookups
CREATE UNIQUE INDEX idx_current_token_balances_address ON current_token_balances(address);

CREATE INDEX idx_current_token_balances_balance ON current_token_balances(current_balance);
"""

CREATE_DELEGATE_POWER_TOP_100_VIEW = """
CREATE MATERIALIZED VIEW top_100_delegates AS
WITH top_100 AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SUM(delegator_balance) DESC) AS rank,
        delegate AS delegate_address,
        SUM(delegator_balance) AS voting_power,
        COUNT(DISTINCT delegator) AS delegations,
        COUNT(DISTINCT CASE WHEN delegator_balance >= 1000000000000000000 THEN delegator END) AS non_zero_delegations
    FROM current_delegations
    WHERE lower(delegate) != '0x0000000000000000000000000000000000000000'
    GROUP BY delegate
    ORDER BY SUM(delegator_balance) DESC
    LIMIT 100
)
SELECT 
    t.rank,
    t.delegate_address,
    t.voting_power,
    dp_30.voting_power AS voting_power_30d_ago,
    t.delegations,
    t.non_zero_delegations,
    (t.voting_power - COALESCE(dp_30.voting_power, 0)) AS power_change_30d
FROM top_100 t
LEFT JOIN LATERAL (
    SELECT dp.voting_power
    FROM delegate_power dp
    WHERE dp.delegate_address = t.delegate_address
      AND dp.block_timestamp <= (EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '30 days')))::bigint
    ORDER BY dp.block_timestamp DESC
    LIMIT 1
) dp_30 ON TRUE
ORDER BY t.voting_power DESC;
-- Create indexes to speed up queries on this view
CREATE UNIQUE INDEX ON top_100_delegates (rank);
CREATE INDEX ON top_100_delegates (delegate_address);
"""

REFRESH_VIEWS = """
REFRESH MATERIALIZED VIEW CONCURRENTLY current_delegations;
REFRESH MATERIALIZED VIEW CONCURRENTLY delegate_power;
REFRESH MATERIALIZED VIEW CONCURRENTLY current_delegate_power;
REFRESH MATERIALIZED VIEW CONCURRENTLY delegate_power_changes;
REFRESH MATERIALIZED VIEW CONCURRENTLY recent_activity;
REFRESH MATERIALIZED VIEW CONCURRENTLY top_1000_holders;
REFRESH MATERIALIZED VIEW CONCURRENTLY top_100_delegates;
"""

# Incremental balance update - processes new Transfer events only
UPDATE_BALANCES_FROM_EVENTS = """
WITH new_transfers AS (
    SELECT
        (args->>'from')::varchar(42) AS from_address,
        (args->>'to')::varchar(42) AS to_address,
        (args->>'value')::numeric(78,0) AS value,
        block_number
    FROM events
    WHERE event_type = 'Transfer'
      AND block_number > %s
),
balance_changes AS (
    SELECT from_address AS address, -value AS change, block_number FROM new_transfers
    UNION ALL
    SELECT to_address AS address, value AS change, block_number FROM new_transfers
),
aggregated_changes AS (
    SELECT
        address,
        SUM(change) AS total_change,
        MAX(block_number) AS max_block
    FROM balance_changes
    GROUP BY address
)
INSERT INTO balances (address, balance, last_updated_block)
SELECT address, total_change, max_block
FROM aggregated_changes
ON CONFLICT (address) DO UPDATE SET
    balance = balances.balance + EXCLUDED.balance,
    last_updated_block = EXCLUDED.last_updated_block;
"""

GET_LAST_PROCESSED_BLOCK = """
SELECT COALESCE(value, 0) FROM indexer_state WHERE key = 'last_transfer_block';
"""

UPDATE_LAST_PROCESSED_BLOCK = """
INSERT INTO indexer_state (key, value) VALUES ('last_transfer_block', %s)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
"""

# Pre-computed voting power changes (avoids expensive LAG at query time)
CREATE_DELEGATE_POWER_CHANGES_VIEW = """
CREATE MATERIALIZED VIEW delegate_power_changes AS
SELECT
    delegate_address,
    block_number,
    block_timestamp,
    log_index,
    voting_power,
    LAG(voting_power, 1, 0) OVER (
        PARTITION BY delegate_address
        ORDER BY block_number, log_index
    ) AS previous_power,
    voting_power - LAG(voting_power, 1, 0) OVER (
        PARTITION BY delegate_address
        ORDER BY block_number, log_index
    ) AS voting_power_change
FROM delegate_power;

CREATE UNIQUE INDEX idx_dpc_lookup ON delegate_power_changes (delegate_address, block_number, log_index);
CREATE INDEX idx_dpc_block_log ON delegate_power_changes (block_number, log_index);
CREATE INDEX idx_dpc_timestamp ON delegate_power_changes (block_timestamp DESC);
"""

# Pre-computed recent activity feed (500x faster than computing on each request)
CREATE_RECENT_ACTIVITY_VIEW = """
CREATE MATERIALIZED VIEW recent_activity AS
WITH
constants AS (
  SELECT '0x0000000000000000000000000000000000000000' AS zero_address
),
with_change AS (
  SELECT *
  FROM delegate_power_changes
  WHERE block_timestamp >= EXTRACT(EPOCH FROM NOW() - INTERVAL '90 days')
),
delegation_changes AS (
  SELECT DISTINCT ON (dc.transaction_hash, dc.log_index)
    dc.block_number,
    dc.block_timestamp,
    dc.log_index,
    dc.args->>'delegator' AS delegator_address,
    dc.args->>'fromDelegate' AS from_delegate,
    dc.args->>'toDelegate' AS to_delegate,
    NULL::text AS delegate_address,
    CASE
      WHEN LOWER(dc.args->>'fromDelegate') = c.zero_address
       AND LOWER(dc.args->>'toDelegate') = LOWER(dc.args->>'delegator')
        THEN 'self_delegation_initiated'
      WHEN LOWER(dc.args->>'fromDelegate') = c.zero_address
        THEN 'delegation_initiated'
      WHEN LOWER(dc.args->>'toDelegate') = c.zero_address
        THEN 'delegation_removed'
      WHEN LOWER(dc.args->>'toDelegate') = LOWER(dc.args->>'delegator')
        THEN 'delegation_to_self'
      ELSE 'delegation_changed'
    END AS activity_type,
    ABS(COALESCE(
      wc_to.voting_power_change,
      wc_from.voting_power_change
    )) AS amount
  FROM events dc
  CROSS JOIN constants c
  LEFT JOIN with_change wc_to
    ON wc_to.delegate_address = dc.args->>'toDelegate'
    AND wc_to.block_number = dc.block_number
    AND wc_to.log_index = dc.log_index + 1
  LEFT JOIN with_change wc_from
    ON wc_from.delegate_address = dc.args->>'fromDelegate'
    AND wc_from.block_number = dc.block_number
    AND wc_from.log_index = dc.log_index + 1
    AND LOWER(dc.args->>'toDelegate') = c.zero_address
  WHERE dc.event_type = 'DelegateChanged'
    AND dc.block_timestamp >= EXTRACT(EPOCH FROM NOW() - INTERVAL '90 days')
),
other_activities AS (
  SELECT
    c.block_number,
    c.block_timestamp,
    c.log_index,
    cd.delegator AS delegator_address,
    NULL::text AS from_delegate,
    NULL::text AS to_delegate,
    c.delegate_address,
    CASE
      WHEN delegator_init.event_type IS NOT NULL AND c.voting_power_change > 0
        THEN 'tokens_received_and_delegated'
      WHEN LOWER(cd.delegator) = LOWER(c.delegate_address) AND c.voting_power_change > 0
        THEN 'self_tokens_received'
      WHEN LOWER(cd.delegator) = LOWER(c.delegate_address) AND c.voting_power_change < 0
        THEN 'self_tokens_sent'
      WHEN cd.delegator IS NOT NULL AND c.voting_power_change > 0
        THEN 'tokens_received'
      WHEN cd.delegator IS NOT NULL AND c.voting_power_change < 0
        THEN 'tokens_sent'
      ELSE NULL
    END AS activity_type,
    ABS(c.voting_power_change) AS amount
  FROM with_change c
  CROSS JOIN constants const
  LEFT JOIN events dc_check
    ON dc_check.event_type = 'DelegateChanged'
    AND dc_check.block_number = c.block_number
    AND dc_check.log_index = c.log_index - 1
    AND (dc_check.args->>'toDelegate' = c.delegate_address OR dc_check.args->>'fromDelegate' = c.delegate_address)
  LEFT JOIN LATERAL (
    SELECT cd_inner.delegator, cd_inner.delegator_balance
    FROM events t
    INNER JOIN current_delegations cd_inner
      ON cd_inner.delegate = c.delegate_address
      AND (t.args->>'to' = cd_inner.delegator OR t.args->>'from' = cd_inner.delegator)
    WHERE t.event_type = 'Transfer'
      AND t.block_number = c.block_number
      AND t.log_index BETWEEN c.log_index - 3 AND c.log_index - 1
    ORDER BY t.log_index DESC
    LIMIT 1
  ) cd ON TRUE
  LEFT JOIN events delegator_init
    ON delegator_init.event_type = 'DelegateChanged'
    AND delegator_init.block_number = c.block_number
    AND delegator_init.args->>'delegator' = cd.delegator
    AND LOWER(delegator_init.args->>'fromDelegate') = const.zero_address
    AND delegator_init.log_index < c.log_index
  WHERE dc_check.event_type IS NULL
)
combined AS (
  SELECT * FROM delegation_changes
  UNION ALL
  SELECT * FROM other_activities WHERE activity_type IS NOT NULL
)
SELECT
  ROW_NUMBER() OVER (ORDER BY block_number DESC, log_index DESC) AS row_id,
  block_number,
  block_timestamp,
  log_index,
  activity_type,
  delegator_address,
  amount,
  from_delegate,
  to_delegate,
  delegate_address
FROM combined
WHERE amount IS NOT NULL;

CREATE UNIQUE INDEX idx_recent_activity_row_id ON recent_activity (row_id);
CREATE INDEX idx_recent_activity_timestamp ON recent_activity (block_timestamp DESC);
CREATE INDEX idx_recent_activity_amount ON recent_activity (amount DESC);
CREATE INDEX idx_recent_activity_type ON recent_activity (activity_type);
"""