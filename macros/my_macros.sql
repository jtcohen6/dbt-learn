{% macro is_it_pancake_day() %}

    {% set today = modules.datetime.datetime.now().strftime("%Y-%m-%d") %}
    {# or dbt.py_current_timestring() #}
    
    {{ {{today}} == '2019-02-25' }}

{% endmacro %}
