{% macro RFM_custom_SQL(config, sql) %}


{% set unique_key = config['unique_key'] %}

{% set segment_col_name = config['segment_col_name']|default('SEGMENT', true) %}


{% set recency = config['recency']|default('recency', true) %}
{% set frequency = config['frequency']|default('frequency', true) %}
{% set monetary = config['monetary']|default('monetary', true) %}





with data as
(
/*original model select*/
     {{ sql }}
)
,segmented_data as (
select 
{{ unique_key }},
{{ recency }},
{{ frequency }},
{{ monetary }},
NTILE(5) OVER (ORDER BY {{ recency }})  R,
NTILE(5) OVER (ORDER BY {{ frequency }})  F,
NTILE(5) OVER (ORDER BY {{ monetary }})  M,
R + F + M RFM,
case
    when RFM > 13 then 'Champion'
    when RFM > 10 then 'Loyal'
    when RFM > 6 then 'Potential'
    else 'Need Attention'
end segment
from data
)
select
{{ unique_key }},
{{ recency }},
{{ frequency }},
{{ monetary }},
R::integer as R,
F::integer as F,
M::integer as M,
RFM::integer as RFM,
segment::varchar(20) as {{ segment_col_name }}
from segmented_data

{% endmacro %}