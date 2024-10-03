
with 
src as (

    select 
        *
    from 
        {{ source('certification', 'RAW_CUSTOMER') }}

)
select * from src