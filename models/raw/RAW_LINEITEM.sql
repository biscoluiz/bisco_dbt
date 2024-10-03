{{
    config(
        materialized='table',
        tags='snap'
    )
}}

with
todays as 
(
     
    select dateadd(day, -1, sysdate())::timestamp as load_date

),

src as 
(

    select
        *    
    from {{ source("jaffle_shop", "LINEITEM") }}

),

tgt as 
(

    select * from src
    cross join todays

)

select * from tgt
where L_SHIPMODE = '{{ var('L_SHIPMODE') }}'