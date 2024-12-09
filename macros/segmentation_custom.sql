{% macro custom_python(config, sql) %}

{% set unique_key = config['unique_key'] %}
#
{% set n_clusters = config['n_clusters']|default(3, true) %}

#
{% set model_name = config['model_name'] %}
{% set model_version = config['model_version']|default('v0', true) %}
{% set model_database = config['model_database']|default(target.database, true) %}
{% set model_schema = config['model_schema']|default(target.schema, true) %}
#
{% set segment_col_name = config['segment_col_name']|default('SEGMENT', true) %}



def model(dbt, session):

   import numpy as np
   #

   dbt.config(materialized = 'table')
    
    #get data from provided SQL
   df = session.sql(
    """
    {{ sql }}
    
    """
    ).to_pandas()

   df["{{segment_col_name}}".upper()] = np.random.randint(1, {{ n_clusters }}, df.shape[0])

   final_sdf=session.create_dataframe(df)

   return final_sdf

{% endmacro %}