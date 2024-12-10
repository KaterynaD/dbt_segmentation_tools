select
O_CUSTKEY,
RECENCY,
FREQUENCY,
MONETARY
from {{ source('test', 'data_for_clustering') }}