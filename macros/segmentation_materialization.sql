{% materialization segmentation, adapter='snowflake', supported_languages=['sql', 'python']%}

  {%- set segmentation_type = config.require('segmentation_type') -%}
  {% if segmentation_type is none %}
   {{ exceptions.raise_compiler_error('Required parameter "segmentation_type" is not set!') }}
  {% endif %}

  {%- set unique_key = config.require('unique_key') -%}
  {% if unique_key is none %}
   {{ exceptions.raise_compiler_error('Required parameter "unique_key" is not set!') }}
  {% endif %}

  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

 
  {% set config = model['config'] %}


  
  {% set grant_config = config.get('grants') %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}

  

  {#-- Drop the relation if it was a view to "convert" it in a table. This may lead to
    -- downtime, but it should be a relatively infrequent occurrence  #}
  {% if old_relation is not none  %}
    {{ log("Dropping relation " ~ old_relation ) }}
    {{ drop_relation_if_exists(old_relation) }}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% if segmentation_type|upper == 'RFM' or  'SQL' in segmentation_type|upper %}

     

      {% call statement('main', language='sql') -%}

      {% if segmentation_type|upper == 'RFM' %}

       {% set build_sql = dbt_segmentation_tools.RFM_sql(config = config, sql = compiled_code) %}

      {% else %}

       {% set build_sql = context[segmentation_type](config = config, sql = compiled_code) %}      

      {% endif %}

      {{ create_table_as(False, target_relation, build_sql, 'sql') }}

      {%- endcall %}

    

  {% elif segmentation_type|upper in ('Gaussian'|upper, 'KMeans'|upper, 'DBSCAN', 'AgglomerativeClustering'|upper) or  'PYTHON' in segmentation_type|upper  %}
  
  


  {% do config.update({'packages': ['snowflake-ml-python','scipy','scikit-learn']}) %}

  {% call statement('main', language='python') -%}


  {% if segmentation_type|upper == 'KMeans'|upper %}

   {% set python_compiled_code = dbt_segmentation_tools.KMeans_python(config = config, sql = compiled_code) %}

  {% elif segmentation_type|upper == 'Gaussian'|upper %}

   {% set python_compiled_code = dbt_segmentation_tools.Gaussian_python(config = config, sql = compiled_code) %}  

  {% elif segmentation_type|upper == 'DBSCAN' %}

   {% set python_compiled_code = dbt_segmentation_tools.DBSCAN_python(config = config, sql = compiled_code) %}

  {% elif segmentation_type|upper == 'AgglomerativeClustering'|upper %}

   {% set python_compiled_code = dbt_segmentation_tools.AgglomerativeClustering_python(config = config, sql = compiled_code) %}

	{% else %}

   {% set python_compiled_code = context[segmentation_type](config = config, sql = compiled_code) %}

  {% endif %}

{{ build_ref_function(model) }}
{{ build_source_function(model) }}
{{ build_config_dict(model) }}
{{ py_script_postfix(model) }}

  {{ create_table_as(False, target_relation, python_compiled_code, 'python') }}

  {%- endcall %}

  {% endif %}
  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

