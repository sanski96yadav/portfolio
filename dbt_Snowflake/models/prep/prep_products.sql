with product_cte as(
    select
    distinct
    {{ dbt_utils.generate_surrogate_key(['stock_id', 'product_description','unit_price'])}} as pk_product_key,
    stock_id,
    product_description,
    unit_price
    from {{ref('stg_invoice_lines')}}
    where stock_id is not null
        and unit_price>0
)
Select 
*,
current_timestamp() as load_dts
from product_cte