DROP TABLE IF EXISTS science.dfm_pred_v2
;														
CREATE TABLE science.dfm_pred_v2
(
id_master_style BIGINT
,henry_id_product BIGINT
,color VARCHAR(50)
,size VARCHAR(50)
,warehouse VARCHAR(50)
,dfm_online BIGINT
,dfm_offline BIGINT
,id_batch BIGINT
,id_dfm_version FLOAT
,num_of_stores BIGINT
)
distkey (id_master_style)
SORTKEY (id_master_style)
;
copy science.dfm_pred_v2
from 's3://hal-bi-bucket/data_science/dfm/excel_files/DFM_batches_results.csv'
iam_role 'arn:aws:iam::207606013102:role/Business_Intelligence'
delimiter ',' csv
ACCEPTINVCHARS
TIMEFORMAT 'auto'
DATEFORMAT AS 'yyyy-mm-dd'
IGNOREBLANKLINES
IGNOREHEADER 1
;

-- run this on time after create table
GRANT USAGE ON SCHEMA science TO GROUP data_viewers;
GRANT SELECT ON science.dfm_pred_v2 TO GROUP data_viewers;
GRANT USAGE ON SCHEMA science TO GROUP data_analytics;
GRANT SELECT ON science.dfm_pred_v2 TO GROUP data_analytics;


-- check errors
select *
from stl_load_errors

