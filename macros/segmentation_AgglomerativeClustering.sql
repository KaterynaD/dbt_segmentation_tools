{% macro AgglomerativeClustering_python(config, sql) %}

{% set unique_key = config['customer_key'] %}
{% set n_clusters = config['n_clusters']|default(3, true) %}
{% set linkage = config['linkage']|default('average', true) %}
#
{% set model_name = config['model_name'] %}
{% set model_version = config['model_version']|default('v0', true) %}
{% set model_database = config['model_database']|default(target.database, true) %}
{% set model_schema = config['model_schema']|default(target.schema, true) %}
#
{% set segment_col_name = config['segment_col_name']|default('SEGMENT', true) %}


def model(dbt, session):

    import numpy as np
    import pandas as pd

    from sklearn.cluster import AgglomerativeClustering
    import gower as gower

    from snowflake.ml.registry import registry

    np.random.seed(42)

    #

    dbt.config(materialized = 'table')
    
    #get data from provided SQL
    df = session.sql(
    """
    {{ sql }}
    
    """
    ).to_pandas()

    #features to cluster
    features = df.columns.tolist()

    #removing customer_key (unique id) from features
    if '{{ unique_key }}'.upper() in features:
        features.remove('{{ unique_key }}'.upper()) 

    #should not contain Nulls
    df.dropna(inplace=True)        

    #Gower distance matrix
    distance_matrix = gower.gower_matrix(np.asarray(df))  

    #Clustering

 {% if model_name|length < 1 %}

    model = AgglomerativeClustering(n_clusters={{ n_clusters }}, linkage='{{ linkage }}', metric='precomputed')
    segments = model.fit_predict(distance_matrix)
    

    df["{{segment_col_name}}".upper()] = segments

 {% else %}


    # create a model registry a
    model_registry = registry.Registry(session=session, database_name="{{model_database}}", schema_name="{{model_schema}}")

    #Getting model from registry    
    clusterer = model_registry.get_model("{{model_name}}").version("{{model_version}}")

    df["{{segment_col_name}}".upper()] = clusterer.run(distance_matrix, function_name="predict")

 {% endif %}        
    
    final_sdf=session.create_dataframe(df)

    return final_sdf

{% endmacro %}