{{ config(

    materialized='segmentation',
    segmentation_type='custom_python',
    unique_key = 'o_custkey',   
    segment_col_name='random_segment', 
    n_clusters = 25
    ) }}

with raw_data as
(
select
o_custkey ,
o_orderdate ,
o_totalprice 
from {{ source('tpch_sf1', 'orders') }}
)
,most_recent_purchase as (
select 
dateadd(day,1, max( o_orderdate )) dt
from raw_data
)
,data as (
select
o_custkey,
datediff(day, max(o_orderdate),most_recent_purchase.dt) Recency,
count(o_orderdate) Frequency,
sum(o_totalprice) Monetary
from raw_data
join most_recent_purchase
group by o_custkey, most_recent_purchase.dt
having datediff(day, max(o_orderdate),most_recent_purchase.dt)<=180
)
select 
o_custkey,
Recency,
Frequency,
Monetary
from data
