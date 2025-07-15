Select 
pk_product_key,
stock_id,
product_description,
unit_price,
current_timestamp() as load_dts
from {{ref('prep_products')}}