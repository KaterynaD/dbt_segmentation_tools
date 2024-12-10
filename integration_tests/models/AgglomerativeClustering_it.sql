select
customer_id,
age,
gender,
civil_status,
salary,
has_children,
purchaser_type
from {{ source('test', 'customer_demo_data') }}