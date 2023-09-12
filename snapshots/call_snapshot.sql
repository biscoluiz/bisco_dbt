{% snapshot call_snapshot %}

{{
    config(
      unique_key='CC_CALL_CENTER_SK',
      target_schema='snapshots',
      strategy='timestamp',
      updated_at='data_rely',  
      post_hook=[   
      "DELETE from {{ this }} where date_trunc('day',cast(DBT_UPDATED_AT as date)) not in (
      SELECT distinct
      date_trunc('day',cast(DBT_UPDATED_AT as date)) AS max_value
      FROM
      snapshots.call_snapshot
      ORDER BY
      max_value DESC
      LIMIT
      2
      )"
      ],
    )
}}

select * from {{ ref('stg_call_center_tb') }}

{% endsnapshot %}