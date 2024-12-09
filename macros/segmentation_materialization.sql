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

  {% if segmentation_type == 'RFM' %}


      {% call statement('main', language='sql') -%}
      {% set build_sql = RFM_sql(config = config, sql = compiled_code) %}
      {{ create_table_as(False, target_relation, build_sql, 'sql') }}

  {%- endcall %}

  {%- else -%}

  


  {% do config.update({'packages': ['snowflake-ml-python','scipy','scikit-learn']}) %}

  {% call statement('main', language='python') -%}

  {# set sql_compiled_code = "\"" ~ compiled_code| replace("\n"," ") | trim ~ "\""  #}



  {%- if segmentation_type == 'KMeans' -%}

  {% set python_compiled_code = KMeans_python(config = config, sql = compiled_code) %}

  {%- elif segmentation_type == 'Gaussian' -%}

  {% set python_compiled_code = Gaussian_python(config = config, sql = compiled_code) %}  

  {%- elif segmentation_type == 'DBSCAN' -%}

  {% set python_compiled_code = DBSCAN_python(config = config, sql = compiled_code) %}

  {%- elif segmentation_type == 'AgglomerativeClustering' -%}

  {% set python_compiled_code = AgglomerativeClustering_python(config = config, sql = compiled_code) %}

	{%- else -%}

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

