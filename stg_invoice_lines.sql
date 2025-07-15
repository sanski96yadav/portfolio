with source as (
select * from {{source('raw', 'online_retail') }}  
)
select
{{ dbt_utils.generate_surrogate_key(['invoiceno', 'customerid','stockcode','unitprice'])}} as pk_invoice_line_key,
invoiceno as invoice_id,
customerid as customer_id,
date as invoice_date,
upper(stockcode) as stock_id,
description as product_description,
quantity,
unitprice as unit_price,
case 
    when country='EIRE' then 'Ireland'
    when country='RSA' then 'South Africa'
    else country
    end as country,
current_timestamp() as load_dts
from source
