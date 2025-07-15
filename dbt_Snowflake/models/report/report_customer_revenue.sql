select
  {{ dbt_utils.generate_surrogate_key(['customer_id', 'country'])}} as pk_customer,
  c.customer_id,
  c.country,
  sum(fi.revenue) AS total_revenue
from {{ ref('fct_invoice_lines') }} fi
join {{ ref('dim_customers') }} c ON fi.customer_key = c.pk_customer_key
group by pk_customer, c.customer_id, c.country