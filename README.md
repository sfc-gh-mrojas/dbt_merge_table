## CDC ingestion pattern with dbt + Snowflake

This project demonstrates a simple CDC pattern where new/updated rows land in a staging table and deletes are captured in a separate tombstone table. The mart model performs an incremental MERGE and removes rows flagged as deleted, while allowing reinserts after a delete.

### Objects
- **staging tables**:
  - `STAGING.STG_CUSTOMER`: current facts (inserts/updates) with `UPDATED_AT`
  - `STAGING.TOMBSTONE_CUSTOMER`: delete events with `DELETED_AT`
- **mart table**:
  - `MART.CUSTOMER_CLEAN`: SCD0-like table with hard deletes

### Model logic (high level)
- **Join base to tombstones** to compute `deleted_flag` when `last_deleted_at > updated_at`.
- **Emit tombstone-only rows** (no corresponding base record) with `deleted_flag = true` so the incremental MERGE can target and delete already existing rows.
- **On full-refresh**, filter out deleted rows; **on incrementals**, allow both active and tombstone-only records so the MERGE can apply deletes.
- A `post_hook` removes deleted rows after merge when not running full-refresh.

### Setup
1. Create Snowflake objects locally for testing:
   ```sql
   -- see tables.sql
   create database dbt_merge_table;
   create schema if not exists staging;
   create schema if not exists mart;
   create or replace table STAGING.STG_CUSTOMER (
       CUSTOMER_ID         number        not null,
       NAME                varchar(100),
       EMAIL               varchar(200),
       UPDATED_AT          timestamp_ntz not null
   );
   create or replace table STAGING.TOMBSTONE_CUSTOMER (
       CUSTOMER_ID   number        not null,
       DELETED_AT    timestamp_ntz not null
   );
   create or replace table MART.CUSTOMER_CLEAN (
       CUSTOMER_ID     number        not null,
       NAME            varchar(100),
       EMAIL           varchar(200),
       UPDATED_AT      timestamp_ntz,
       DELETED_FLAG    boolean,
       primary key (CUSTOMER_ID)
   );
   ```

2. Configure your Snowflake credentials in `dbt_merge_table/profiles.yml`.

### Run
- First run (full refresh):
  ```bash
  dbt run --full-refresh --select marts.customer_clean
  ```
- Incremental runs as new data arrives:
  ```bash
  dbt run --select marts.customer_clean
  ```

### Behavioral notes
- **Multiple deletes**: the model uses the latest `DELETED_AT` per `CUSTOMER_ID`.
- **Reinsertion after delete**: any new base row with `UPDATED_AT > last_deleted_at` will be retained (not deleted).
- **Tombstone-only deletes**: if a delete event arrives without a base row, the model still produces a `deleted_flag=true` record so the MERGE removes it from the target.

### Extending
- Add additional keys/columns to `STG_CUSTOMER` and propagate to `CUSTOMER_CLEAN`.
- Convert to soft-delete by keeping `deleted_flag` rows and removing the post-hook.
