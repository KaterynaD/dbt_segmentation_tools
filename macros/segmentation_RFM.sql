{% macro RFM_sql(config, sql) %}


{% set unique_key = config['unique_key'] %}

{% set segment_col_name = config['segment_col_name']|default('SEGMENT', true) %}

{% set segments = config['segments']|default(['Champion','Loyal','Potential','Need Attention'], true) %}
{% set segments_limits = config['segments_limits']|default([13,10,6,0], true) %}

{% if  segments_limits|length != segments|length %}
 {{ exceptions.raise_compiler_error('The number of segments must be the same as the number of segments_limits!') }}
{% endif %}

{% set quintile = config['quintile']|default(5, true) %}

{% if  not quintile|int != 0  %}
 {{ exceptions.raise_compiler_error('Quantile must be Number!') }}
{% endif %}


{% set method = config['method']|default('+', true) %}

{% if  not method in ('+','*') %}
 {{ exceptions.raise_compiler_error('Method must be only "+" or "*"!') }}
{% endif %}

{% set orderdate = config['orderdate'] %}
  {% if orderdate is none %}
   {{ exceptions.raise_compiler_error('Required parameter "orderdate" is not set!') }}
  {% endif %}

{% set ordertotal = config['ordertotal'] %}
  {% if ordertotal is none %}
   {{ exceptions.raise_compiler_error('Required parameter "ordertotal" is not set!') }}
  {% endif %}

{% set period_of_interest_in_days = config['period_of_interest_in_days']|default(3650, true) %}

{% if  not period_of_interest_in_days|int != 0  %}
 {{ exceptions.raise_compiler_error('period_of_interest_in_days must be Number!') }}
{% endif %}

with raw_data as
(
/*original model select*/
     {{ sql }}
)
,most_recent_purchase as (
select 
dateadd(day,1, max( {{ orderdate }} )) dt
from raw_data
)
,data as (
select
{{ unique_key }},
datediff(day, max({{ orderdate }}),most_recent_purchase.dt) Recency,
count({{ orderdate }}) Frequency,
sum({{ ordertotal }}) Monetary,
sum({{ ordertotal }})/count({{ orderdate }}) Monetary_Avg
from raw_data
join most_recent_purchase
group by {{ unique_key }}, most_recent_purchase.dt
having datediff(day, max({{ orderdate }}),most_recent_purchase.dt)<={{ period_of_interest_in_days }}
)
,segmented_data as (
select 
{{ unique_key }},
Recency,
Frequency,
Monetary,
Monetary_Avg,
NTILE({{ quintile }}) OVER (ORDER BY Recency)  R,
NTILE({{ quintile }}) OVER (ORDER BY Frequency)  F,
NTILE({{ quintile }}) OVER (ORDER BY Monetary)  M,
R {{ method }} F {{ method }} M RFM,
case

{% for item1, item2 in zip(segments_limits,segments) %}
    when RFM > {{ item1 }} then '{{ item2 }}'
{% endfor %}

end segment
from data
)
select
{{ unique_key }},
Recency::integer as Recency,
Frequency::integer as Frequency,
Monetary::numeric(38,5) as Monetary,
Monetary_Avg::numeric(38,5) as Monetary_Avg,
R::integer as R,
F::integer as F,
M::integer as M,
RFM::integer as RFM,
segment::varchar(20) as {{ segment_col_name }}
from segmented_data

{% endmacro %}