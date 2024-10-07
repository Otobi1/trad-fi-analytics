{{ config(materialized="table") }}


select
    exchange as exchange,
    symbol as symbol,
    shortname as short_name,
    longname as long_name,
    sector as sector,
    industry as industry,
    marketcap as market_cap,
    ebitda as ebitda,
    revenuegrowth as revenue_growth,
    city as city,
    state as state,
    country as country,
    fulltimeemployees as total_employees,
    round(weight, 3) as weight
from
    {{ ref("sp_500_lookup") }}

    
    
