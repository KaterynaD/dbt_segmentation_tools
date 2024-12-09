{% macro KMeans_python(config, sql) %}

{% set unique_key = config['unique_key'] %}
#
{% set n_clusters = config['n_clusters']|default(3, true) %}
{% set max_iter = config['max_iter']|default(100, true) %}
{% set init = config['init']|default('random', true) %}
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
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
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

    #Scaling
    zcols = ['z_'+c for c in features]

    scaler=StandardScaler()
    scaled_data = scaler.fit_transform(df[features])
    scaled_df = pd.DataFrame(scaled_data, index=df.index, columns=zcols)

    #Clustering
 {% if model_name|length < 1 %}

    #Building model
    clusterer = KMeans(n_clusters= {{ n_clusters }}, max_iter = {{ max_iter }}, init='k-means++', n_init=42, random_state=42)
    cluster_labels = clusterer.fit_predict(scaled_df[zcols].to_numpy())

    df["{{segment_col_name}}".upper()] = cluster_labels.tolist()


 {% else %}


    # create a model registry a
    model_registry = registry.Registry(session=session, database_name="{{model_database}}", schema_name="{{model_schema}}")

    #Getting model from registry    
    clusterer = model_registry.get_model("{{model_name}}").version("{{model_version}}")

    df["{{segment_col_name}}".upper()] = clusterer.run(scaled_df[zcols], function_name="predict")

 {% endif %}

    final_sdf=session.create_dataframe(df)

    return final_sdf

{% endmacro %}