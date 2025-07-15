Select 
*,
case 
when dbt_valid_to is null 
then true
else false 
end as is_current,
current_timestamp() as load_dts
from {{ref('dim_customer_snapshot')}}
where customer_id is not null