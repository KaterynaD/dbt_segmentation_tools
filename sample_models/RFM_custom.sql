{{ config(

    materialized='segmentation',
    segmentation_type='RFM_custom_SQL',

    unique_key = 'o_custkey',

    recency = 'Recency',
    frequency = 'Frequency',
    monetary = 'Monetary_Avg',
    
    segment_col_name='RFM_custom_segment',


    ) }}


select
o_custkey ,
Recency,
Frequency,
Monetary_Avg,
from {{ ref('RFM') }}

