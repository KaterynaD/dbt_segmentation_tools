{{ config(

    materialized='segmentation',
    segmentation_type='Gaussian',
    unique_key = 'o_custkey',
    n_components= 4   

    ) }}


select
o_custkey ,
RECENCY,
FREQUENCY,
MONETARY
from {{ ref('RFM') }}