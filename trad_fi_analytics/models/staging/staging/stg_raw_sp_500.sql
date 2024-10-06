{{ config(materialized="view") }}

with 

source as (

    select * from {{ source('staging', 'raw_sp_500_check') }}

),

renamed as (

    select
        date,
        FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_SECONDS(CAST(CAST(date AS INT64) / 1000000000 AS INT64))) AS formatted_date,
        ticker,
        open,
        high,
        low,
        close,
        volume,
        dividends,
        stock_splits,
        year,
        month,
    from source

)

select * from renamed
