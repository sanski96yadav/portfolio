{% snapshot dim_customer_snapshot %}
{{ 
    config(
      target_schema= 'snapshots',
      strategy= 'check',
      unique_key=['customer_id'],
      check_cols=['country']
    )
}}   
select 
customer_id,
country
from {{ ref('prep_customers') }}
where customer_id is not null
{% endsnapshot %} 