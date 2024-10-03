{{
    config(
        materialized='table'
    )
}}

with source as (

    select * from {{ source('jaffle_shop', 'ORDERS') }}

),

renamed as (

    select
        {{
            dbt_utils.generate_surrogate_key(['o_orderkey','o_custkey'])
        }} as uuid,
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority::varchar(255) as el_orderpriority,
        o_clerk as el_clerk,
        o_shippriority,
        o_comment

    from source

)

select * from renamed