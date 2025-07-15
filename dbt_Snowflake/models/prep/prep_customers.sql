with duplicate as (
   select
   {{ dbt_utils.generate_surrogate_key([ 'customer_id','country'])}} as pk_customer_key,
   customer_id,
   country,  
    {{duplicate_rows(['customer_id'],['country'])}} --micro for: row_number()over(partition by customer_id order by country) as rownumber
    from {{ref('stg_invoice_lines')}}
)
Select
pk_customer_key,
customer_id,
country,
current_timestamp() as load_dts
from duplicate
where rownumber<2 and customer_id is not null