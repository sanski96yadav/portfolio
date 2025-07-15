with datetime_cte as (
    Select distinct
    to_date(to_timestamp(invoice_date,'MM/DD/YY HH24:MI')) as date,
    case
      when length(invoice_date) = 16 then
        to_timestamp(invoice_date, 'MM/DD/YYYY HH24:MI')
      when length(invoice_date) <= 14 then 
        to_timestamp(invoice_date, 'MM/DD/YY HH24:MI')
      else null 
        end as datetime_id
    from {{ ref('stg_invoice_lines') }}
  where invoice_date is not null
)
Select
datetime_id,
date,
year (date) AS year,
year (date)|| 'Q'|| quarter(date) AS year_quarter,
to_char(date, 'YYYY-MM') AS year_month,
year (date)|| 'W'|| weekiso(date) AS year_week
from datetime_cte