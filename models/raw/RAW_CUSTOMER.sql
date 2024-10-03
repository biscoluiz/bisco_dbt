with
todays as 
(
     
    select dateadd(day, -1, sysdate())::timestamp as load_date

),

src as 
(

    select
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment        
    from {{ source("jaffle_shop", "CUSTOMER") }}

),

tgt as 
(

    select * from src
    cross join todays

)

select * from tgt