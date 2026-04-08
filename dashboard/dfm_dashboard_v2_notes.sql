-- 1. create dfm_pred_v2 table from s3
select * from science.dfm_pred_v2 dpv 

-- need to be id ms style|color|size|warehouse|dfm_online|dfm_offline|dfm_version|batches

-- 2. create fact_supply_chain_dfm table : prep OTB qty in the format ready to join the science.dfm_pred_v2
select * from science.fact_supply_chain_dfm

-- 3. create table science.dfm_dp_fsc_v2 joining fsc (from 2) and dfm (from1) and dim_product (dp)--
--3.1 create science.dfm_dp_fsc_v2_temp
--3.2 create science.dfm_dp_fsc_v2 from  science.dfm_dp_fsc_v2_temp but remove reorder product
select * from  science.dfm_dp_fsc_v2_temp
select * from  science.dfm_dp_fsc_v2

-- 4. add release collection date --- and create a view in looker DFM Prediction +OTB "drv_dfm_dp_fsc"**** using below query
select df.id_product
      ,df.id_product_attribute
      ,df.henry_id_product
      ,df.henry_id_product_attribute
      ,df.id_master_style
      ,df.size
      ,df.color
      ,dp.simple_color
      ,dp.product_cost_usd_first_order_date as product_cost_usd
      ,dp.brand
      ,dp.parent_product_line  as product_line
      ,dp.product_line as sub_product_line
      ,dp.henry_category_1
      ,dp.henry_category_2
      ,dp.henry_category_3
      ,dp.fabric_custom_name as fabric_type
      ,CASE WHEN dp.hscode_id_fabric_name IS NULL THEN 'No Fabric' ELSE dp.hscode_id_fabric_name END as fabric_name
      ,dp.release_collection_name,
            case
              when df.warehouse = 'TH' then dp.date_released
              when df.warehouse = 'MY' then dp.date_released_my
              when df.warehouse = 'ID' then dp.date_released_id
              end as release_date
      ,df.warehouse
      ,df.dfm_online
      ,df.dfm_offline
      ,df.id_batch
      ,df.id_dfm_version
      ,df.num_of_stores
      ,df.otb_qty_online
      ,df.otb_qty_offline
      from science.dfm_dp_fsc_v2 df
      left join dwh.dim_product dp
      on df.id_product_attribute = dp.id_product_attribute
      where dp.release_collection_name NOT IN ('Aug20 Drop3 Barbie + Denim China','Apr20 Drop7','June20 Drop5','Sep20 - Drop 4: Additional Denim','Sep20 Drop4: Denim, PlayTrend + T-shirt')
	  AND df.id_master_style = 90564

-- 5. create a view for fact sale in looker "drv_dfm_fact_sale" ****

-- 6. create a view for fact sale offline in looker "drc_dfm_fact_sale_offline" ****
	  
--** FROM step 7 is done in Looker **--	  
	  
-- 7. create 3 views : drv_dfm_dp_fsc_v2,drv_dfm_fact_sale,drv_dfm_fact_sale_offline
	  
-- 8. create drv_dfm_performance_v2 explore in pomelo.model 