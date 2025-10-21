{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    database='dbt_merge_table',
    schema='mart',
    post_hook=[
      "{% if not flags.FULL_REFRESH %} \
      delete from {{ this }} \
      where deleted_flag = true; \
      {% endif %}"
    ]
)}}

with base as (
    select
        customer_id,
        name,
        email,
        updated_at
    from {{ source('staging', 'stg_customer') }}
),
tombstone as (
    select
        customer_id,
        max(deleted_at) as last_deleted_at
    from {{ source('staging', 'tombstone_customer') }}
    group by customer_id
),
base_with_delete_flag as (
    select
        b.customer_id,
        b.name,
        b.email,
        b.updated_at,
        case
            when t.last_deleted_at > b.updated_at then true else false
        end as deleted_flag
    from base b
    left join tombstone t
        on b.customer_id = t.customer_id
),
-- Include tombstone-only rows so incremental MERGE can delete existing targets
tombstone_only as (
    select
        t.customer_id,
        cast(null as varchar) as name,
        cast(null as varchar) as email,
        cast(null as timestamp_ntz) as updated_at,
        true as deleted_flag
    from tombstone t
    left join base b
        on t.customer_id = b.customer_id
    where b.customer_id is null
),
merged as (
    select * from base_with_delete_flag
    union all
    select * from tombstone_only
)
{% if flags.FULL_REFRESH %}
select
    customer_id,
    name,
    email,
    updated_at,
    deleted_flag
from merged
where deleted_flag = false
{% else %}
select * from merged
{% endif %}