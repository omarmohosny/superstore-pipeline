-- same expression on both sides, so the fact table can find the dimension row
{% macro location_key(alias) -%}
    md5(concat_ws('|', {{ alias }}.country, {{ alias }}.state, {{ alias }}.city, {{ alias }}.postal_code))
{%- endmacro %}
