with afterduplicates as (
    Select 
    *,
    {{duplicate_rows(['customer_id','invoice_id','stock_id','unit_price'], ['invoice_id'])}} --micro used duplicate_rows(partition_cols, order_cols)
    from {{ref('stg_invoice_lines')}}
)
Select
pk_invoice_line_key,
invoice_id,
to_timestamp(invoice_date,'MM/DD/YY HH24:MI') as invoice_date,
{{ dbt_utils.generate_surrogate_key([ 'customer_id','country'])}} as customer_key,
{{ dbt_utils.generate_surrogate_key([ 'stock_id','product_description','unit_price'])}} as product_key,
quantity * unit_price as revenue,
current_timestamp() as load_dts
from afterduplicates
where quantity>0 and rownumber<2

