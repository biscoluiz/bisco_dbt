{{
    config(
        materialized='table',
        alias='rw_nat'
    )
}}

with
todays as 
(
     
    select sysdate()::timestamp as load_date

),

src as 
(

    select
        *    
    from {{ source("jaffle_shop", "NATION") }}

),

tgt as 
(

    select * from src
    cross join todays

)

select * from tgt