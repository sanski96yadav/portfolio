{% macro duplicate_rows(partition_cols, order_cols)%}
   {% set partition_str = partition_cols | join(', ') %}
  {% set order_str = order_cols | join(', ') %}
  {{ return("row_number() over (partition by " ~ partition_str ~ " order by " ~ order_str ~ ") as rownumber") }}
{% endmacro %}