select
  {{ dbt_utils.generate_surrogate_key(['stock_id', 'product_description'])}} as pk_product,
  p.stock_id,
  p.product_description,
  sum(fi.revenue) AS total_revenue
from {{ ref('fct_invoice_lines') }} fi
join {{ ref('dim_products') }} p ON fi.product_key = p.pk_product_key
group by pk_product,p.stock_id, p.product_description

