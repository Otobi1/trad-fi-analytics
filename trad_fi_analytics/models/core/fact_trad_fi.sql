{{ config(materialized="table") }}

with
    sp_full as (select * from {{ ref("stg_raw_sp_500") }}),
    dim_sp as (select * from {{ ref("dim_sp_500") }})
select 
    date 
    , ticker
    , open
    , high 
    , low
    , close 
    , volume 
    , dividends
    , stock_splits
    , exchange
    , long_name
    , sector 
    , industry
    , market_cap
    , ebitda
    , revenue_growth
    , city
    , state
    , total_employees
    , weight
from sp_full sf
inner join dim_sp ds 
    on sf.ticker = ds.symbol












trad_fi_analytics
/
models
/
core
/
fact_trad_fi.sql

Save
123456789101112
{{ config(materialized="table") \}\}

with
    sp_full as (select * from {{ ref("stg_raw_sp_500") \}\}),
    dim_sp as (select * from {{ ref("dim_sp_500") \}\})
select *
from sp_full sf
inner join dim_sp ds 
    on sf.ticker = ds.symbol



Preview Selection

Compile Selection

Build

Format
Results
Code quality
Compiled code
Lineage$0