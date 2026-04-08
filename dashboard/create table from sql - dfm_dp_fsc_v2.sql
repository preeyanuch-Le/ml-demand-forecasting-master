DROP TABLE IF EXISTS science.dfm_dp_fsc_v2
;														
CREATE TABLE science.dfm_dp_fsc_v2
(
id_product BIGINT
,id_product_attribute BIGINT
,henry_id_product BIGINT
,henry_id_product_attribute BIGINT
,release_collection_name VARCHAR(50)
,henry_category_1 VARCHAR(50)
,henry_category_2 VARCHAR(50)
,henry_category_3 VARCHAR(50)
,id_master_style BIGINT
,color VARCHAR(50)
,size VARCHAR(50)
,warehouse VARCHAR(50)
,dfm_online BIGINT
,dfm_offline BIGINT
,id_batch BIGINT
,id_dfm_version FLOAT
,num_of_stores BIGINT
,new_or_reorder VARCHAR(50)
,otb_qty_online BIGINT
,otb_qty_offline BIGINT
)
distkey (id_product_attribute)
SORTKEY (id_product_attribute)
;
INSERT INTO science.dfm_dp_fsc_v2
select a.*
from science.dfm_dp_fsc_v2_temp a
left join (select distinct dp.henry_id_product_attribute, dfm.id_batch
FROM science.dfm_pred_v2 dfm
LEFT JOIN dwh.dim_product dp
ON dfm.henry_id_product = dp.henry_id_product   
left join science.fact_supply_chain_dfm fscdfm 
on dp.henry_id_product_attribute = fscdfm.id_product_attribute
and dfm.warehouse = fscdfm.warehouse
where fscdfm.new_or_reorder = 'Reorder') b
on a.henry_id_product_attribute = b.henry_id_product_attribute
and a.id_batch = b.id_batch 
where b.henry_id_product_attribute is null
;

-- this table select from dfm_dp_fsc_v2_temp where only has id_product_attribute that has no reorder at all 
-- (will drop ) all produtc that has reorder and new order row

-- run this on time after create table
GRANT USAGE ON SCHEMA science TO GROUP data_viewers;
GRANT SELECT ON science.dfm_dp_fsc_v2 TO GROUP data_viewers;


GRANT USAGE ON SCHEMA science TO GROUP data_analytics;
GRANT SELECT ON science.dfm_dp_fsc_v2 TO GROUP data_analytics;






