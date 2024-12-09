{{ config(

    materialized='segmentation',
    segmentation_type='DBSCAN',
    unique_key = 'c_customer_sk',
    segment_col_name='DBScan_segment', 
    imports=['@control_db.external_stages.my_python_library/gower.zip'],    
    eps  = 0.1,
    min_samples=5

    ) }}

select 
c.c_customer_sk,
2003 - c.c_birth_year age,
d.cd_gender,
d.cd_marital_status,
d.cd_education_status
from  {{ source('tpcds_sf100tcl', 'customer') }} c sample(1000 rows) 
join  {{ source('tpcds_sf100tcl', 'customer_demographics') }} d
on d.cd_demo_sk=c.c_current_cdemo_sk
