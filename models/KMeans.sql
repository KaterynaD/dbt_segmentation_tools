{{ config(

    materialized='segmentation',
    segmentation_type='KMeans',
    unique_key = 'o_custkey',   
    segment_col_name='KMeans_segment', 
    n_clusters = 4
    ) }}


select
o_custkey ,
RECENCY,
FREQUENCY,
MONETARY
from {{ ref('RFM') }}