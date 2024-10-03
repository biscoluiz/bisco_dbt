{{
    config(
        materialized='table'
    )
}}

with tgt as (

select 
'A1' as pk,
1000 as value

union all

select 
'A2' as pk,
2000 as value

union all 

select 
'A2' as pk,
3000 as value

union all 

select 
'A2' as pk,
4000 as value

union all 

select 
'A1' as pk,
9000 as value

union all 

select 
cast(null as varchar(255)) as pk,
4000 as value

)

select * from tgt