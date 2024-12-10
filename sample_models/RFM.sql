{{ config(

    materialized='segmentation',
    segmentation_type='RFM',
    unique_key = 'o_custkey',

    segment_col_name='RFM_segment',
    segments = ['Champion','Loyal','Potential','Need Attention'],
    segments_limits = [13,10,6,0],
    quintile = 5,
    method = '+',


    orderdate= 'o_orderdate',
    ordertotal = 'o_totalprice',
    period_of_interest_in_days = 180

    ) }}


select
o_custkey ,
o_orderdate ,
o_totalprice 
from {{ source('tpch_sf1', 'orders') }}