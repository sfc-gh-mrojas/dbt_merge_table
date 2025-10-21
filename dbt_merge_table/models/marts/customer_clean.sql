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
merged as (
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
)
select * from merged