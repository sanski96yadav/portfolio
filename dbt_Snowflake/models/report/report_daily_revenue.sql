select
  d.date,
  d.year,
  d.year_month,
  d.year_quarter,
  d.year_week,
  sum(fi.revenue) AS total_revenue
from {{ ref('fct_invoice_lines') }} fi
join {{ ref('dim_date') }} d ON fi.invoice_date= d.datetime_id
group by  d.date, d.year, d.year_month, d.year_quarter, d.year_week