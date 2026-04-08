import awswrangler as wr


def query_main_data_id():
    sql = """
SELECT
-- product attributes
    dp.id_product_attribute
    ,dp.id_product
    ,dp.henry_id_product_attribute
    ,dp.henry_id_product
    ,dp.date_released_id as date_released
    ,date_format(dp.date_released_id, '%M') as released_month
    ,date_format(dp.date_released_id, '%W') as day_of_week
    ,date_format(dp.date_released_id, '%e') as day_number
    ,date_format(dp.date_released_id, '%Y') as year
    ,CASE WHEN cast(date_format(dp.date_released_id, '%e') as bigint) <  8 THEN 1 ELSE 0 END as first_week_of_month
    ,CASE WHEN cast(date_format(dp.date_released_id, '%e') as bigint) >21 THEN 1 ELSE 0 END as last_week_of_month
    ,dp.product_cost_usd_first_order_date as product_cost_usd
    ,dp.size
    ,dp.brand
    ,dp.parent_product_line  as product_line
    ,dp.product_line as sub_product_line
    ,dp.henry_category_1 as old_henry_category_1
    ,dp.henry_category_2 as old_henry_category_2
    ,dp.henry_category_3 as old_henry_category_3
    ,dp.new_category_1 as henry_category_1
    ,dp.new_category_2 as henry_category_2
    ,dp.new_category_3 as henry_category_3
    ,dp.simple_color
    ,dp.color
--Deep tagging data
    ,ld."style"
    ,ld.sleeve
    ,ld.pattern
    ,ld.sleevestyle
    ,ld.neckline
    ,ld.shape
    ,ld.rise
    ,dp.original_price_th_to_usd as original_price_usd
    ,dp.fabric_custom_name
    ,CASE WHEN dp.hscode_id_fabric_name IS NULL THEN 'No Fabric' ELSE dp.hscode_id_fabric_name END as hscode_id_fabric_name
-- collection
 ---Remove extra spaces
    ,case when dp.release_collection_name = 'CNY  Jan2021'
          then 'CNY Jan2021'
          when dp.release_collection_name = 'Wedding Collection 19 Oct19 Drop1 '
          then 'Wedding Collection 19 Oct19 Drop1'
          when dp.release_collection_name is null
          then 'none'
          --when dp.release_collection_name like '%drop%'
          --then 'normal_drop'
          --when dp.release_collection_name like '%Drop%'
          --then 'normal_drop'
          else dp.release_collection_name
          end as release_collection_name
---Lookbook collection considered based on Lookbook collection (has data from 2018-04-01 onwards and doesn't have data for 2020 Holiday collection)
    ,case when (release_collection_name like '%CNY%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Wedding Collection%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Ramadan%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Fall Campaign%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Songkran Collection%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Holiday%' and extract(year from dp.date_released_id) not in (2017,2020))
         or (release_collection_name like '%Spring Campaign%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Spring Summer%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '% Barbie%' and dp.date_released_id > date('2020-09-01'))
         or (release_collection_name like '%Milin%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Haribo%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%TikTok%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%LooneyTunes%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%June Lookbook%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Esther Bunny%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Smiley%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%PEANUTS%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Disney%' and dp.date_released_id > date('2018-04-01'))
         or (release_collection_name like '%Jane Suda%' and dp.date_released_id > date('2018-04-01'))
         then 1
         else 0
     end as is_lookbook_collection
-- collection_sizes > S,M,L
    ,case when release_collection_name like '%CNY%' then 'l'
         when release_collection_name like '%Wedding Collection%' then 's'
         when release_collection_name like '%Ramadan%' then 's'
         when release_collection_name like '%Fall Campaign%' then 'l'
         when release_collection_name like '%Songkran Collection%' then 's'
         when release_collection_name like '%Holiday%' then 'm'
         when release_collection_name like '%Spring Campaign%' then 'l'
         when release_collection_name like '%Spring Summer%' then 'l'
         when release_collection_name like '% Barbie%' then 'm'
         when release_collection_name like '%Milin%' then 'm'
         when release_collection_name like '%Haribo%' then 's'
         when release_collection_name like '%TikTok%' then 'm'
         when release_collection_name like '%LooneyTunes%' then 's'
         when release_collection_name like '%June Lookbook%' then 'm'
         when release_collection_name like '%Esther Bunny%' then 's'
         when release_collection_name like '%Smiley%' then 's'
         when release_collection_name like '%PEANUTS%' then 's'
         when release_collection_name like '%Disney%' then 'm'
         when release_collection_name like '%Jane Suda%' then 'l'
         else 'normal_drop'
     end as collection_sizes

-- tie
    ,CASE WHEN dp.product_name LIKE '%Tie%' THEN 1 ELSE 0 END as tie

--  day in stock : use for feature engineering --> extrapolate potential sales
    ,fis.days_in_stock_lt

-- warehouse
    ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse

-- license product : we don't use anymore -> as a will be in the dp.product_line - > as "Licensing"
    ,CASE WHEN dp.release_collection_name LIKE ('%Smiley%') OR
                dp.release_collection_name LIKE ('%Barbie%') OR
                dp.release_collection_name LIKE ('%Milin%') THEN 1 ELSE 0 END as license_product

-- covid lockdown : not use
    ,CASE WHEN dp.date_released_id BETWEEN cast('2020-03-01' as date) AND cast('2020-05-31' as date) THEN 1 ELSE 0 END as covid_lockdown

-- in house vs 3P sales
    ,tp_ih.in_house_sales
    ,tp_ih.third_party_sales

-- inventory
    ,inv.inventory_level
    ,inv.count_products_in_group

-- campaign
    ,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=0 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=6 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week1
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=7 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=13 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week2
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=14 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=20 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week3
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=21 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=27 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week4
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=28 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=34 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week5
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=35 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=41 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week6
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=42 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=48 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week7
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=49 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=55 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week8
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=56 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=62 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week9
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=63 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=69 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week10
	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=70 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=76 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week11
 	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=77 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=83 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week12
  	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=84 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=90 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week13
  	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=91 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=97 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week14
  	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=98 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=104 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week15
  	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=105 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=111 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week16
  	,case when SUM(case when DATE_DIFF('day',dp.date_released_id,fs.order_date)>=112 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=118 AND
                               (fs.cart_rule_name like '%4/4%'
                                        or fs.specific_price_rule_name like '%44Promo%'
                                        or fs.specific_price_rule_name LIKE '%44Clear%'
                                        or fs.cart_rule_name like '%5/5%'
                                        or fs.cart_rule_name like '%6/6 %' or fs.specific_price_rule_name like '%6/6%'
                                        or fs.cart_rule_name like '%7/7%'
                                        or fs.cart_rule_name like '%8/8%'
                                        or fs.cart_rule_name like '%9/9%'
                                        or fs.cart_rule_name like '%10/10%'
                                        or fs.cart_rule_name like '%11/11%'
                                        or fs.cart_rule_name like '%12/12%'
                                        or fs.cart_rule_name like '%4.4%' or fs.specific_price_rule_name like '%4.4%'
                                        or fs.cart_rule_name LIKE '%5.5%' or fs.specific_price_rule_name like '%5.5%'
                                        or fs.cart_rule_name LIKE '%6.6%' or fs.specific_price_rule_name like '%6.6%'
                                        or fs.cart_rule_name LIKE '%7.7%' or fs.specific_price_rule_name like '%7.7%'
                                        or fs.cart_rule_name LIKE '%8.8%' or fs.specific_price_rule_name like '%8.8%'
                                        or fs.cart_rule_name like '%9.9%' or fs.specific_price_rule_name like '%9.9%'
                                        or lower(fs.cart_rule_name) like '%99pomelo%'
                                        or fs.cart_rule_name like '%10.10%' or fs.specific_price_rule_name like '%10.10%'
                                        or fs.cart_rule_name like '%11.11%' or fs.specific_price_rule_name like '%11.11%'
                                        or fs.cart_rule_name like '%12.12%' or fs.specific_price_rule_name like '%12.12%'
                                        or lower(fs.specific_price_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%birthday%'
                                        or lower(fs.cart_rule_name) LIKE '%bday%'
                                        or lower(fs.specific_price_rule_name) LIKE '%bday%'
                                        or lower(fs.cart_rule_name) like '%bd%'
                                        or lower(fs.cart_rule_name) LIKE '%black friday%'
                                        or lower(fs.cart_rule_name) LIKE '%blackfriday%'
                                        or lower(fs.cart_rule_name) LIKE '%july%'
                                        or lower(fs.cart_rule_name) like '%jul%'
                                        or lower(fs.cart_rule_name) like '%halloween%')
                                 then fs.gross_units_sold
                                 else 0 end) >= 1 then 1 else 0 end
                                 as is_mega_campaign_order_week17
-- lifetime rev
    --,SUM(fs.revenue_usd) as revenue_usd_lt
    -- didn't use but it is overall lifetime rev for that product (It = lifetime)

-- PR giveaway (if it is a giveaway -> sale should be higher)
    ,CASE WHEN SUM(ga.giveaway) is null then 0 else SUM(ga.giveaway) end as giveaway


-- transaction data
    -- week1
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=0 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=6 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week1
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=0 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=6 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week1
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=0 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=6 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week1
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=0 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=6 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week1
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=0 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=6 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week1
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=0 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=6
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week1
    -- week2
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=7 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=13 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week2
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=7 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=13 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week2
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=7 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=13 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week2
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=7 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=13 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week2
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=7 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=13 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week2
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=7 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=13
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week2
    -- week3
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=14 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=20 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week3
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=14 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=20 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week3
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=14 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=20 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week3
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=14 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=20 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week3
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=14 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=20 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week3
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=14 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=20
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week3
    -- week4
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=21 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=27 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week4
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=21 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=27 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week4
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=21 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=27 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week4
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=21 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=27 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week4
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=21 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=27 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week4
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=21 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=27
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week4
    -- week5
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=28 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=34 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week5
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=28 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=34 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week5
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=28 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=34 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week5
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=28 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=34 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week5
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=28 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=34 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week5
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=28 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=34
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week5
    -- week6
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=35 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=41 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week6
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=35 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=41 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week6
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=35 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=41 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week6
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=35 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=41 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week6
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=35 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=41 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week6
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=35 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=41
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week6
    -- week7
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=42 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=48 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week7
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=42 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=48 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week7
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=42 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=48 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week7
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=42 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=48 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week7
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=42 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=48 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week7
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=42 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=48
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week7
    -- week8
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=49 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=55 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week8
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=49 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=55 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week8
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=49 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=55 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week8
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=49 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=55 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week8
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=49 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=55 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week8
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=49 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=55
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week8
    -- week9
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=56 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=62 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week9
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=56 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=62 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week9
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=56 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=62 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week9
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=56 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=62 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week9
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=56 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=62 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week9
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=56 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=62
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week9
    -- week10
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=63 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=69 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week10
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=63 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=69 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week10
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=63 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=69 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week10
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=63 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=69 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week10
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=63 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=69 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week10
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=63 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=69
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week10
    -- week11
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=70 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=76 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week11
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=70 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=76 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week11
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=70 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=76 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week11
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=70 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=76 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week11
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=70 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=76 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week11
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=70 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=76
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week11
    -- week12
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=77 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=83 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week12
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=77 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=83 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week12
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=77 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=83 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week12
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=77 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=83 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week12
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=77 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=83 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week12
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=77 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=83
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week12
    -- week13
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=84 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=90 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week13
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=84 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=90 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week13
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=84 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=90 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week13
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=84 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=90 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week13
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=84 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=90 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week13
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=84 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=90
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week13
    -- week14
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=91 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=97 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week14
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=91 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=97 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week14
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=91 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=97 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week14
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=91 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=97 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week14
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=91 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=97 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week14
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=91 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=97
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week14
    -- week15
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=98 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=104 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week15
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=98 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=104 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week15
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=98 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=104 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week15
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=98 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=104 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week15
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=98 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=104 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week15
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=98 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=104
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week15
    -- week16
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=105 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=111 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week16
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=105 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=111 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week16
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=105 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=111 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week16
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=105 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=111 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week16
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=105 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=111 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week16
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=105 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=111
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week16
    -- week17
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=112 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=118 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_week17
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=112 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=118 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_week17
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=112 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=118 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week17
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=112 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=118 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_week17
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=112 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=118 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_week17
    ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released_id,fs.order_date)>=112 AND DATE_DIFF('day',dp.date_released_id,fs.order_date)<=118
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_week17
FROM dwh.dim_product dp

-- Deep Tagging data
left join dwh.dim_product_lexicon_data ld
on dp.id_product = ld.id_product

-- fact sales fs
LEFT JOIN dwh.fact_sales fs
    ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product

-- fact_inventory_snapshot fis
LEFT JOIN
    (SELECT
        i.id_product
        ,i.id_product_attribute
        ,COUNT(DISTINCT (
            CASE
              WHEN DATE_DIFF('day', i.snapshot_date, CURRENT_DATE) >= 0
                AND DATE_DIFF('day', prod.new_date_released, i.snapshot_date) > 0
                AND i.id_shop = 1
                THEN DATE (snapshot_timestamp)
              ELSE NULL
              END)) AS days_in_stock_lt
        FROM dwh.fact_inventory_snapshot i

        LEFT JOIN
            (SELECT
                dp2.id_product,dp2.id_product_attribute,dp2.date_released,fs3.first_order_date,
                CASE WHEN fs3.first_order_date < dp2.date_released THEN fs3.first_order_date ELSE dp2.date_released END as new_date_released
            FROM dwh.dim_product dp2
            LEFT JOIN
            (SELECT
                id_product,id_product_attribute,MIN(order_date) as first_order_date
                FROM dwh.fact_sales fs2
                GROUP BY 1,2) fs3
                ON fs3.id_product_attribute = dp2.id_product_attribute
            ) prod
        ON prod.id_product_attribute = i.id_product_attribute
        GROUP BY 1,2
    ) fis
ON fis.id_product = dp.id_product AND fis.id_product_attribute = dp.id_product_attribute

-- give away ga
LEFT JOIN
    (SELECT
        id_product, id_shop, SUM(net_units_sold) as giveaway from dwh.fact_sales
     WHERE order_type = 'Giveaway'
     GROUP BY 1,2
     ) ga
ON ga.id_product = dp.id_product and ga.id_shop = fs.id_shop

-- add third party sale and in house sale
-- in the past third party sale only account for 1% of total sale but now it account for about 10%
-- so we inculded to monitor the proportion -> if third party sale increase the forcast should decrese
LEFT JOIN
    (SELECT
        date(order_date) as date_released,
        SUM(CASE WHEN brand IN('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio') THEN fs.gross_units_sold ELSE 0 END) as in_house_sales,
        SUM(CASE WHEN brand NOT IN ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio') THEN fs.gross_units_sold ELSE 0 END) as third_party_sales
    FROM dwh.fact_sales fs
    LEFT JOIN dwh.dim_product dp
    ON dp.id_product_attribute=fs.id_product_attribute
    WHERE date_released >= cast('2017-01-01' as date)
    AND dp.parent_product_line NOT IN ('Free Gift')
    AND dp.brand IN ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio','Blackdog BKK')
    AND fs.order_type IN ('Marketplace','Web','Android','iOS','Partner','Site to Store')
    AND dp.henry_category_1 not in ('Accessories', 'Bags', 'Cosmetics', 'Miscellaneous', 'Intimates')
    AND dp.product_line NOT IN ('Bags','Shoes')
    AND (lower(fs.cart_rule_name) not like '%employee%'
                  and lower(fs.cart_rule_name) not like '%test%'
                  and lower(fs.cart_rule_name) not like '%vm office%'
                  and lower(fs.cart_rule_name) not like '%uniform%'
                  and lower(fs.cart_rule_name) not like '%influencer%'
                  and lower(fs.cart_rule_name) not like '%pr %'
                  or fs.cart_rule_name is null)
    GROUP BY 1) tp_ih
ON tp_ih.date_released = dp.date_released_id

-- fact inventory snapshot
LEFT JOIN
    (SELECT
        date(fis.snapshot_date) as snapshot_date
        ,dp.product_line
        ,dp.henry_category_1
        ,dp.henry_category_2
        ,dp.henry_category_3
        ,dp.simple_color
        ,CASE WHEN fis.id_shop = 5 THEN 'ID'
                          WHEN fis.id_shop = 2 THEN 'MY'
                          WHEN fis.id_shop = 11 THEN 'MY'
                          ELSE 'TH' END as warehouse
        ,SUM(fis.inventory_level) as inventory_level
        ,COUNT(distinct fis.id_product) as count_products_in_group from dwh.fact_inventory_snapshot fis
    LEFT JOIN dwh.dim_product dp on dp.id_product_attribute = fis.id_product_attribute
    WHERE dp.date_released_id > cast('2017-01-01' as date)
    GROUP BY 1,2,3,4,5,6,7
    ) inv
ON inv.snapshot_date = dp.date_released_id
AND inv.warehouse = (CASE WHEN fs.id_shop = 5 THEN 'ID'
                          WHEN fs.id_shop = 2 THEN 'MY'
                          WHEN fs.id_shop = 11 THEN 'MY'
                          ELSE 'TH' END)
    AND inv.product_line = dp.product_line
    AND inv.henry_category_1 = dp.henry_category_1
    AND inv.henry_category_2 = dp.henry_category_2
    AND inv.henry_category_3 = dp.henry_category_3
    AND inv.simple_color = dp.simple_color

WHERE dp.date_released_id BETWEEN cast('2017-01-01' as date) AND DATE_TRUNC('day', NOW() - INTERVAL '119' DAY)
AND fs.id_shop = 5
AND dp.parent_product_line NOT IN ('Free Gift')
AND dp.product_line NOT IN('Bags','3P Apparel','Shoes')
AND dp.henry_category_1 NOT IN ('Accessories','Bags','Bath&Body','Beverage Container','Cosmetics','Hair','Miscellaneous','Shoes','Skin Care','Stationery','Intimates')
AND dp.product_cost_usd_first_order_date > 0
AND fis.days_in_stock_lt >= 0
AND dp.brand IN ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio','Blackdog BKK')
AND dp.release_collection_name IS NOT NULL
AND fs.order_type IN ('Marketplace','Web','Android','iOS','Partner','Site to Store')
AND (lower(fs.cart_rule_name) not like '%employee%'
                  and lower(fs.cart_rule_name) not like '%test%'
                  and lower(fs.cart_rule_name) not like '%vm office%'
                  and lower(fs.cart_rule_name) not like '%uniform%'
                  and lower(fs.cart_rule_name) not like '%influencer%'
                  and lower(fs.cart_rule_name) not like '%pr %'
                  or fs.cart_rule_name is null)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46
-- ORDER BY week_id,warehouse
ORDER BY 1
        """
    main_data_id = wr.athena.read_sql_query(sql, database="dwh")
    return main_data_id
