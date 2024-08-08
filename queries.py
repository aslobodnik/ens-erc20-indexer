
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
        address,
        balance,
        block_number
    FROM token_balances
    ORDER BY address, block_number DESC
)
SELECT 
    address,
    balance,
    block_number,
    ROW_NUMBER() OVER (ORDER BY balance DESC) AS rank
FROM latest_balances
WHERE balance > 0
ORDER BY balance DESC
LIMIT 1000;

-- Create indexes to speed up queries on this view
CREATE UNIQUE INDEX ON top_1000_holders (rank);
CREATE INDEX ON top_1000_holders (address);
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



# Delegate Counts Materialized View
CREATE_DELEGATION_COUNTS_VIEW = f"""
CREATE MATERIALIZED VIEW delegation_counts AS
SELECT 
    ((e.args ->> 'toDelegate'::text))::character varying(42) AS delegate_address,
    count(DISTINCT ((e.args ->> 'delegator'::text))::character varying(42)) AS total_delegations,
    count(DISTINCT CASE
        WHEN tb.balance > '1000000000000000000'::bigint::numeric THEN ((e.args ->> 'delegator'::text))::character varying(42)
        ELSE NULL::character varying
    END) AS non_zero_delegations
FROM events e
 LEFT JOIN token_balances tb ON tb.address::text = ((e.args ->> 'delegator'::text))::character varying(42)::text
WHERE e.event_type::text = 'DelegateChanged'::text
GROUP BY (((e.args ->> 'toDelegate'::text))::character varying(42));

CREATE UNIQUE INDEX idx_delegation_counts_address ON delegation_counts(delegate_address);"""

CREATE_CURRENT_DELEGATIONS_VIEW ="""
CREATE MATERIALIZED VIEW current_delegations AS
WITH ranked_delegations AS (
    SELECT 
        args->>'delegator' as delegator,
        args->>'toDelegate' as delegate,
        args->>'fromDelegate' as prior_delegate,
        e.block_number,
        ROW_NUMBER() OVER (PARTITION BY args->>'delegator' ORDER BY e.block_number DESC) as rn
    FROM 
        events e
    WHERE
        event_type = 'DelegateChanged'
)
SELECT 
    delegator,
    delegate,
    prior_delegate,
    block_number as last_updated_block
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

REFRESH_VIEWS = """
REFRESH MATERIALIZED VIEW CONCURRENTLY token_balances;
REFRESH MATERIALIZED VIEW CONCURRENTLY current_delegations;
REFRESH MATERIALIZED VIEW CONCURRENTLY current_token_balances;
REFRESH MATERIALIZED VIEW CONCURRENTLY delegate_power;
REFRESH MATERIALIZED VIEW CONCURRENTLY delegation_counts;
REFRESH MATERIALIZED VIEW CONCURRENTLY current_delegate_power;
REFRESH MATERIALIZED VIEW CONCURRENTLY top_1000_holders;
"""