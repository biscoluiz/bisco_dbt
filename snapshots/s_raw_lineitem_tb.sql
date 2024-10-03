--depends on: RAW_LINEITEM

{% snapshot s_raw_lineitem_tb %}

{{
    config(      
      target_schema='snapshots',
      unique_key='uid',

      strategy='timestamp',
      updated_at = 'LOAD_DATE',
      invalidate_hard_deletes=True,

      tags='snap'
    )
}}

select md5(L_ORDERKEY|| L_PARTKEY|| L_SUPPKEY ) as uid, a.* from {{ ref('RAW_LINEITEM') }} a

{% endsnapshot %}