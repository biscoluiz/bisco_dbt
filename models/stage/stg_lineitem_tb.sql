
with 
src as (

    select 
        *
    from 
        {{ source('certification', 'RAW_LINEITEM') }}

)
select * from src