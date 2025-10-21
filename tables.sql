create database dbt_merge_table;

create schema if not exists staging;

create schema if not exists marts; 

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

create or replace table MARTS.CUSTOMER_CLEAN (
    CUSTOMER_ID     number        not null,
    NAME            varchar(100),
    EMAIL           varchar(200),
    UPDATED_AT      timestamp_ntz,
    DELETED_FLAG    boolean,
    primary key (CUSTOMER_ID)
);


select * from marts.customer_clean;