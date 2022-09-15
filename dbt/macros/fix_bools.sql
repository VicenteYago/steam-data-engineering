{% macro fix_bools(column_json) %}
    
    REPLACE(
        REPLACE({{column_json}}, 'True', 'true')
    , 'False', 'false')

{% endmacro %}