
with 
src as (

    select 
        *
    from 
        {{ ref('RAW_NATION') }}

)
select * from src