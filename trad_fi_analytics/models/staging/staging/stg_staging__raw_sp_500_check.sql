{{ config(materialized="view") }}

with 

source as (

    select * from {{ source('staging', 'raw_sp_500_check') }}

),

renamed as (

    select
        date, 
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
