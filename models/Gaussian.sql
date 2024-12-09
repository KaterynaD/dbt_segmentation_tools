{{ config(

    materialized='segmentation',
    segmentation_type='Gaussian',
    unique_key = 'o_custkey',   
    model_name = 'GMM_Customer_Segmentation',
    model_version = 'SOUR_FIREANT_3',    
    model_database='CONTROL_DB',
    model_schema='MODELS'
    ) }}


select
o_custkey ,
RECENCY,
FREQUENCY,
MONETARY
from {{ ref('RFM') }}