select
o_custkey ,
o_orderdate ,
o_totalprice 
from {{ source('test', 'orders') }}