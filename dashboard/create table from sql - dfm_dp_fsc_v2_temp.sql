DROP TABLE IF EXISTS science.dfm_dp_fsc_v2_temp
;														
CREATE TABLE science.dfm_dp_fsc_v2_temp
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
INSERT INTO science.dfm_dp_fsc_v2_temp
SELECT  dp.id_product, dp.id_product_attribute, dp.henry_id_product,
 dp.henry_id_product_attribute,  
dp.release_collection_name ,dp.henry_category_1,dp.henry_category_2,dp.henry_category_3, 
dfm.id_master_style, dfm.color, dfm.size, dfm.warehouse, dfm.dfm_online,
dfm.dfm_offline,dfm.id_batch,dfm.id_dfm_version,dfm.num_of_stores
, fscdfm.new_or_reorder, fscdfm.otb_qty_online,fscdfm.otb_qty_offline
FROM science.dfm_pred_v2 dfm
LEFT JOIN dwh.dim_product dp
ON dfm.henry_id_product = dp.henry_id_product 
--ON dfm.id_master_style = dp.id_master_style 
AND dfm.size = dp.size
--AND dfm.color = dp.color 
left join science.fact_supply_chain_dfm fscdfm 
on dp.henry_id_product_attribute = fscdfm.id_product_attribute
and dfm.warehouse = fscdfm.warehouse
;

-- run this on time after create table
GRANT USAGE ON SCHEMA science TO GROUP data_viewers;
GRANT SELECT ON science.dfm_dp_fsc_v2_temp TO GROUP data_viewers;


GRANT USAGE ON SCHEMA science TO GROUP data_analytics;
GRANT SELECT ON science.dfm_dp_fsc_v2_temp TO GROUP data_analytics;






