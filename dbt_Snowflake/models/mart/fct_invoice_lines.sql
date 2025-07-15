Select 
pk_invoice_line_key,
invoice_id,
invoice_date,
customer_key,
product_key,
revenue,
current_timestamp() as load_dts
from {{ref('prep_invoice_lines')}}