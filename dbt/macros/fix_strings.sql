{% macro fix_strings(col_str) %}
    
-- regexp_replace(normalize_and_casefold({{col_str}}), r'\W+','')
regexp_replace(normalize_and_casefold({{col_str}}),
               r'llc|inc\.|inc|s\.a\.|ltd\.|co\.|\.|\,|\s|\(mac\)|\(linux\)|\(mac/linux\)|™|®',
               '')
{% endmacro %}