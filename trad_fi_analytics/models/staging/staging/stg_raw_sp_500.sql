{{ config(materialized="view") }}

with 

source as (

    select * from {{ source('staging', 'raw_sp_500_check') }}

),

renamed as (

    select
        format_timestamp('%Y-%m-%d', timestamp_seconds(CAST(CAST(date AS int64) / 1000000000 AS int64))) AS date,
        ticker,
        round(open, 3) as open,
        round(high, 3) as high,
        round(low, 3) as low,
        round(close, 3) as close,
        cast(volume as int64) as volume,
        dividends,
        stock_splits,
        year,
        month,
    from source

)

select * from renamed


  -- Open FLOAT	NULLABLE 
    -- High FLOAT	NULLABLE 
    -- Low FLOAT	NULLABLE 
    -- Close FLOAT	NULLABLE 
    -- Volume INTEGER	NULLABLE 
    -- Dividends FLOAT	NULLABLE 
    -- Stock_Splits FLOAT	NULLABLE 
    -- Year INTEGER	NULLABLE 
    -- Month INTEGER	NULLABLE 
    -- Ticker STRING	NULLABLE 
    -- Date INTEGER NULLABLE 