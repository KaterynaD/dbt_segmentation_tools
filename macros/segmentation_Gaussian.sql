{% macro Gaussian_python(config, sql) %}

{% set unique_key = config['unique_key'] %}
{% set n_components = config['n_components']|default(3, true) %}
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
    from sklearn.preprocessing import StandardScaler, OneHotEncoder
    from sklearn.compose import ColumnTransformer
    from scipy.stats import boxcox, anderson
    from sklearn.base import BaseEstimator, TransformerMixin
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.mixture import GaussianMixture
    from snowflake.ml.registry import registry

    class BoxCoxTransformer(BaseEstimator, TransformerMixin):
        def __init__(self, alpha=0.05):
            self.alpha = alpha

        def fit(self, X, y=None):
            self.lambdas_ = {}
            self.shifts_ = {}
            for col in X.columns:
                # Shift data if necessary
                min_val = X[col].min()
                shift = 0 if min_val > 0 else -min_val + 1
                self.shifts_[col] = shift
                shifted_data = X[col] + shift

                # Apply Anderson-Darling test
                ad_test_result = anderson(shifted_data.dropna())
                if ad_test_result.statistic > ad_test_result.critical_values[0]:  # Comparing with the critical value at 15% significance level
                    _, maxlog = boxcox(shifted_data.dropna())
                    self.lambdas_[col] = maxlog
            return self

        def transform(self, X):
            X_transformed = pd.DataFrame(index=X.index)
            for col in X.columns:
                shifted_data = X[col] + self.shifts_.get(col, 0)
                if col in self.lambdas_:
                    transformed_col = boxcox(shifted_data, lmbda=self.lambdas_[col])
                    X_transformed[col] = transformed_col
                else:
                    X_transformed[col] = shifted_data
            return X_transformed   

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

    #Scaling and Normalizing
    
    # Create a pipeline for numerical features
    numerical_pipeline = Pipeline([
        ('boxcox', BoxCoxTransformer()),
        ('scaler', StandardScaler())
    ])

    # Create a column transformer
    preprocessor = ColumnTransformer(
        transformers=[('num', numerical_pipeline, features)]
    )

    transformed_data = preprocessor.fit_transform(df[features])


    # Create the final DataFrame
    transformed_df = pd.DataFrame(transformed_data,index = df.index)


    #Clustering
 {% if model_name|length < 1 %}
    
    gmm = GaussianMixture(n_components={{ n_components }}, random_state=42).fit(transformed_df)

    # Assigning the segments to the original DataFrame

    df["{{segment_col_name}}".upper()] = gmm.predict(transformed_df)

 {% else %}


    # create a model registry a
    model_registry = registry.Registry(session=session, database_name="{{model_database}}", schema_name="{{model_schema}}")

    #Getting model from registry    
    clusterer = model_registry.get_model("{{model_name}}").version("{{model_version}}")

    df["{{segment_col_name}}".upper()] = clusterer.run(transformed_df, function_name="predict")

 {% endif %}    

    final_sdf=session.create_dataframe(df)

    return final_sdf

{% endmacro %}