DROP TABLE IF EXISTS science.fact_supply_chain_dfm
;														
CREATE TABLE science.fact_supply_chain_dfm
(
id_product_attribute BIGINT
,warehouse VARCHAR(50)
,merchandise_collection_name VARCHAR(100)
,new_or_reorder VARCHAR(50)
,otb_qty_online BIGINT
,otb_qty_offline BIGINT
)
distkey (id_product_attribute)
SORTKEY (id_product_attribute)
;

INSERT INTO science.fact_supply_chain_dfm
select id_product_attribute, 'TH' as warehouse, merchandise_collection_name, new_or_reorder,
case when AVG(mer_qty_th) is null then 0 else AVG(mer_qty_th) end as  otb_qty_online,
case when AVG(mer_qty_retail_th) is null then 0 else AVG(mer_qty_retail_th) end as  otb_qty_offline
from dwh.fact_supply_chain 
WHERE merchandise_collection_status <> 'Cancelled'
AND shipping_to NOT LIKE '%Samples/Marketing'
AND (merchandise_collection_name  NOT LIKE '%_PR%' 
AND merchandise_collection_name NOT LIKE 'PR_%')
OR merchandise_collection_name LIKE '%APR%' 
group by id_product_attribute, merchandise_collection_name, new_or_reorder
union all
select id_product_attribute, 'MY' as warehouse,merchandise_collection_name, new_or_reorder,
case when AVG(mer_qty_my) is null then 0 else AVG(mer_qty_my) end as  otb_qty_online,
case when AVG(mer_qty_retail_my) is null then 0 else AVG(mer_qty_retail_my) end as  otb_qty_offline
from dwh.fact_supply_chain 
WHERE merchandise_collection_status <> 'Cancelled'
AND shipping_to NOT LIKE '%Samples/Marketing'
AND (merchandise_collection_name  NOT LIKE '%_PR%' 
AND merchandise_collection_name NOT LIKE 'PR_%')
OR merchandise_collection_name LIKE '%APR%' 
group by id_product_attribute, merchandise_collection_name, new_or_reorder
union all
select id_product_attribute, 'ID' as warehouse,merchandise_collection_name, new_or_reorder,
case when AVG(mer_qty_id) is null then 0 else AVG(mer_qty_id) end as  otb_qty_online,
case when AVG(mer_qty_retail_id) is null then 0 else AVG(mer_qty_retail_id) end as  otb_qty_offline
from dwh.fact_supply_chain 
WHERE merchandise_collection_status <> 'Cancelled'
AND shipping_to NOT LIKE '%Samples/Marketing'
AND (merchandise_collection_name  NOT LIKE '%_PR%' 
AND merchandise_collection_name NOT LIKE 'PR_%')
OR merchandise_collection_name LIKE '%APR%' 
group by id_product_attribute,merchandise_collection_name, new_or_reorder
;

-- run this on time after create table
GRANT USAGE ON SCHEMA science TO GROUP data_viewers;
GRANT SELECT ON  science.fact_supply_chain_dfm TO GROUP data_viewers;

GRANT USAGE ON SCHEMA science TO GROUP data_analytics;
GRANT SELECT ON science.fact_supply_chain_dfm TO GROUP data_analytics;

