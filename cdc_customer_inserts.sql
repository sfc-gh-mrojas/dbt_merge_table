-- CDC demonstration for customer_clean
-- Run the statements step-by-step and execute dbt between steps as indicated.
use database dbt_merge_table;
-- STEP 0: Reset sample data (optional for a clean slate)
truncate table STAGING.STG_CUSTOMER;
truncate table STAGING.TOMBSTONE_CUSTOMER;
truncate table MARTS.CUSTOMER_CLEAN;

-- STEP 1: Initial inserts (no deletes yet)
insert into STAGING.STG_CUSTOMER (CUSTOMER_ID, NAME, EMAIL, UPDATED_AT) values
  (1, 'Alice', 'alice@example.com', '2025-01-01 00:00:00'::timestamp_ntz),
  (2, 'Bob',   'bob@example.com',   '2025-01-01 00:00:00'::timestamp_ntz),
  (4, 'Dana',  'dana@example.com',  '2025-01-01 00:00:00'::timestamp_ntz);

-- Now run a full-refresh to initialize the mart table:
--   dbt run --full-refresh --select marts.customer_clean


-- STEP 2: Delete events (including a tombstone-only delete) and an update
-- Delete Alice (1), Bob (2), and emit a tombstone-only delete for id 3
insert into STAGING.TOMBSTONE_CUSTOMER (CUSTOMER_ID, DELETED_AT) values
  (1, '2025-01-02 00:00:00'::timestamp_ntz),
  (2, '2025-01-02 00:00:00'::timestamp_ntz),
  (3, '2025-01-02 00:00:00'::timestamp_ntz);

-- Update Dana (4) to simulate a new version (keep only one row per id in STG)
update STAGING.STG_CUSTOMER
set EMAIL = 'dana+v2@example.com', UPDATED_AT = '2025-01-02 00:05:00'::timestamp_ntz
where CUSTOMER_ID = 4;

-- Run incremental to apply deletes and updates:
--   dbt run --select marts.customer_clean

-- STEP 3: Reinsertion after delete + multiple deletes for the same id
-- Reinsert Alice (1) with a newer UPDATED_AT than last delete -> should be kept
update STAGING.STG_CUSTOMER
set NAME = 'Alice R', EMAIL = 'alice.r@example.com', UPDATED_AT = '2025-01-03 00:00:00'::timestamp_ntz
where CUSTOMER_ID = 1;

-- Emit an additional delete for Bob (2) later than the first (multiple deletes)
insert into STAGING.TOMBSTONE_CUSTOMER (CUSTOMER_ID, DELETED_AT) values
  (2, '2025-01-03 01:00:00'::timestamp_ntz);

-- Run incremental again to reflect reinsertion and latest deletes:
--   dbt run --select marts.customer_clean

-- STEP 4: Inspect results
select * from MARTS.CUSTOMER_CLEAN order by CUSTOMER_ID;


