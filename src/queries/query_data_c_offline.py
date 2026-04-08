import awswrangler as wr
from src.utils.decorators import timeit

@timeit
def query_data():
    sql = """
        SELECT 
        dp.id_product_attribute,
        dp.id_product,
        dp.henry_id_product_attribute,
        dp.henry_id_product,
        dp.date_released,
        dp.product_cost_usd_first_order_date as product_cost_usd,
        dp.size_range,
        dp.size,
        dp.brand,
        dp.parent_product_line as product_line,
        dp.product_line as sub_product_line,
        dp.henry_category_1 as old_henry_category_1,
        dp.henry_category_2 as old_henry_category_2,
        dp.henry_category_3 as old_henry_category_3,
        dp.new_category_1 as henry_category_1,
        dp.new_category_2 as henry_category_2,
        dp.new_category_3 as henry_category_3,
        dp.simple_color,
        dp.color,

--Deep tagging data
        ld."style", 
        ld.sleeve, 
        ld.pattern, 
        ld.sleevestyle, 
        ld.neckline, 
        ld.shape, 
        ld.rise, 

        dp.original_price_th_to_usd as original_price_usd,
        dp.fabric_custom_name,
        dp.hscode_id_fabric_name,
        dp.release_collection_name,
        
        -- store
        fsoo.id_store,
        fsoo.store_name,
        fsoo.id_shop,
        CASE WHEN cast(fsoo.id_shop as bigint) = 1 THEN 'TH'
                                    WHEN cast(fsoo.id_shop as bigint) = 2 THEN 'SG'
                                    WHEN cast(fsoo.id_shop as bigint) = 5 THEN 'ID'
                                    WHEN cast(fsoo.id_shop as bigint) = 11 THEN 'MY'
                                    ELSE null
                                    END as id_shop_name,
        CASE WHEN cast(fsoo.id_shop as bigint) = 5 THEN 'ID' 
        WHEN cast(fsoo.id_shop as bigint) = 2 THEN 'MY'
        WHEN cast(fsoo.id_shop as bigint) = 11 THEN 'MY'
        ELSE 'TH' END as warehouse,

        
        -- fist availble date
        fa.first_available_date,
        date_format(fa.first_available_date, '%M') as first_available_month,
        date_format(fa.first_available_date, '%W') as first_available_dow,
        date_format(fa.first_available_date, '%e') as day_number,
        date_format(fa.first_available_date, '%Y') as first_available_year,
        CASE WHEN cast(date_format(fa.first_available_date, '%e') as bigint) <  8 THEN 1 ELSE 0 END as first_week_of_month,
        CASE WHEN cast(date_format(fa.first_available_date, '%e') as bigint) >21 THEN 1 ELSE 0 END as last_week_of_month,
        
        -- give away
        ga.giveaway

        
        -- min max transaction date
        ,COALESCE(MAX(CASE when fsoo.transaction_type = 'Sale' THEN fsoo.transaction_date else null end)) as max_transaction_date
        ,COALESCE(MIN(CASE when fsoo.transaction_type = 'Sale' THEN fsoo.transaction_date else null end)) as min_transaction_date

        --net unit sold
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=0 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=6 THEN net_units_sold ELSE 0 END) as net_units_sold_week1
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=7 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=13 THEN net_units_sold ELSE 0 END) as net_units_sold_week2
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=14 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=20 THEN net_units_sold ELSE 0 END) as net_units_sold_week3
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=21 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=27 THEN net_units_sold ELSE 0 END) as net_units_sold_week4
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=28 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=34 THEN net_units_sold ELSE 0 END) as net_units_sold_week5
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=35 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=41 THEN net_units_sold ELSE 0 END) as net_units_sold_week6
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=42 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=48 THEN net_units_sold ELSE 0 END) as net_units_sold_week7

        -- gross revenue
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=0 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=6 THEN gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week1
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=7 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=13 THEN gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week2
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=14 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=20 THEN gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week3
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=21 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=27 THEN gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week4
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=28 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=34 THEN gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week5
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=35 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=41 THEN gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week6
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=42 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=48 THEN gross_revenue_usd ELSE 0 END) as gross_revenue_usd_week7

        -- revenue
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=0 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=6 THEN revenue_usd ELSE 0 END) as revenue_usd_week1
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=7 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=13 THEN revenue_usd ELSE 0 END) as revenue_usd_week2
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=14 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=20 THEN revenue_usd ELSE 0 END) as revenue_usd_week3
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=21 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=27 THEN revenue_usd ELSE 0 END) as revenue_usd_week4
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=28 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=34 THEN revenue_usd ELSE 0 END) as revenue_usd_week5
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=35 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=41 THEN revenue_usd ELSE 0 END) as revenue_usd_week6
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=42 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=48 THEN revenue_usd ELSE 0 END) as revenue_usd_week7

        -- item discount
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=0 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=6 AND 
(fsoo.promotion_name like '%Item Discount%' or fsoo.promotion_name like '%itemdiscount%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as item_discount_usd_week1
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=7 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=13 AND 
(fsoo.promotion_name like '%Item Discount%' or fsoo.promotion_name like '%itemdiscount%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as item_discount_usd_week2
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=14 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=20 AND 
(fsoo.promotion_name like '%Item Discount%' or fsoo.promotion_name like '%itemdiscount%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as item_discount_usd_week3
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=21 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=27 AND 
(fsoo.promotion_name like '%Item Discount%' or fsoo.promotion_name like '%itemdiscount%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as item_discount_usd_week4
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=28 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=34 AND 
(fsoo.promotion_name like '%Item Discount%' or fsoo.promotion_name like '%itemdiscount%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as item_discount_usd_week5
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=35 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=41 AND 
(fsoo.promotion_name like '%Item Discount%' or fsoo.promotion_name like '%itemdiscount%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as item_discount_usd_week6
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=42 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=48 AND 
(fsoo.promotion_name like '%Item Discount%' or fsoo.promotion_name like '%itemdiscount%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as item_discount_usd_week7
            
        -- voucher discount
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=0 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=6 AND 
(fsoo.promotion_name like '%Voucher%' or fsoo.promotion_name like '%voucher%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as voucher_discount_usd_week1
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=7 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=13 AND 
(fsoo.promotion_name like '%Voucher%' or fsoo.promotion_name like '%voucher%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as voucher_discount_usd_week2
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=14 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=20 AND 
(fsoo.promotion_name like '%Voucher%' or fsoo.promotion_name like '%voucher%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as voucher_discount_usd_week3
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=21 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=27 AND 
(fsoo.promotion_name like '%Voucher%' or fsoo.promotion_name like '%voucher%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as voucher_discount_usd_week4
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=28 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=34 AND 
(fsoo.promotion_name like '%Voucher%' or fsoo.promotion_name like '%voucher%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as voucher_discount_usd_week5
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=35 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=41 AND 
(fsoo.promotion_name like '%Voucher%' or fsoo.promotion_name like '%voucher%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as voucher_discount_usd_week6
        ,SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=42 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=48 AND 
(fsoo.promotion_name like '%Voucher%' or fsoo.promotion_name like '%voucher%') THEN fsoo.gross_revenue_usd - fsoo.revenue_usd ELSE 0 END) as voucher_discount_usd_week7

        
        
        
        
        
        
        ---mega campaign
        ,CASE WHEN SUM(CASE WHEN 
            DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=0 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=6 AND
            (fsoo.promotion_name LIKE '%9.9%' OR
            fsoo.promotion_name LIKE '%10.10%' OR
            fsoo.promotion_name LIKE '%11.11%' OR
            fsoo.promotion_name LIKE '%12.12%' OR
            fsoo.promotion_name LIKE '%EOSS%' OR
            fsoo.promotion_name LIKE '%4.4%' OR
            fsoo.promotion_name LIKE '%Birthday%' OR
            fsoo.promotion_name LIKE '%Birdthday%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%BD%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%5.5%' OR
            fsoo.promotion_name LIKE '%6.6%')
            THEN gross_units_sold ELSE 0 END)  >=1 then 1 else 0 end as is_mega_campaign_order_week1
        ,CASE WHEN SUM(CASE WHEN 
            DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=7 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=13 AND
            (fsoo.promotion_name LIKE '%9.9%' OR
            fsoo.promotion_name LIKE '%10.10%' OR
            fsoo.promotion_name LIKE '%11.11%' OR
            fsoo.promotion_name LIKE '%12.12%' OR
            fsoo.promotion_name LIKE '%EOSS%' OR
            fsoo.promotion_name LIKE '%4.4%' OR
            fsoo.promotion_name LIKE '%Birthday%' OR
            fsoo.promotion_name LIKE '%Birdthday%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%BD%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%5.5%' OR
            fsoo.promotion_name LIKE '%6.6%')
            THEN gross_units_sold ELSE 0 END)  >=1 then 1 else 0 end as is_mega_campaign_order_week2
        ,CASE WHEN SUM(CASE WHEN 
            DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=14 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=20 AND
            (fsoo.promotion_name LIKE '%9.9%' OR
            fsoo.promotion_name LIKE '%10.10%' OR
            fsoo.promotion_name LIKE '%11.11%' OR
            fsoo.promotion_name LIKE '%12.12%' OR
            fsoo.promotion_name LIKE '%EOSS%' OR
            fsoo.promotion_name LIKE '%4.4%' OR
            fsoo.promotion_name LIKE '%Birthday%' OR
            fsoo.promotion_name LIKE '%Birdthday%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%BD%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%5.5%' OR
            fsoo.promotion_name LIKE '%6.6%')
            THEN gross_units_sold ELSE 0 END)  >=1 then 1 else 0 end as is_mega_campaign_order_week3
        ,CASE WHEN SUM(CASE WHEN 
            DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=21 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=27 AND
            (fsoo.promotion_name LIKE '%9.9%' OR
            fsoo.promotion_name LIKE '%10.10%' OR
            fsoo.promotion_name LIKE '%11.11%' OR
            fsoo.promotion_name LIKE '%12.12%' OR
            fsoo.promotion_name LIKE '%EOSS%' OR
            fsoo.promotion_name LIKE '%4.4%' OR
            fsoo.promotion_name LIKE '%Birthday%' OR
            fsoo.promotion_name LIKE '%Birdthday%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%BD%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%5.5%' OR
            fsoo.promotion_name LIKE '%6.6%')
            THEN gross_units_sold ELSE 0 END)  >=1 then 1 else 0 end as is_mega_campaign_order_week4
        ,CASE WHEN SUM(CASE WHEN 
            DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=28 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=34 AND
            (fsoo.promotion_name LIKE '%9.9%' OR
            fsoo.promotion_name LIKE '%10.10%' OR
            fsoo.promotion_name LIKE '%11.11%' OR
            fsoo.promotion_name LIKE '%12.12%' OR
            fsoo.promotion_name LIKE '%EOSS%' OR
            fsoo.promotion_name LIKE '%4.4%' OR
            fsoo.promotion_name LIKE '%Birthday%' OR
            fsoo.promotion_name LIKE '%Birdthday%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%BD%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%5.5%' OR
            fsoo.promotion_name LIKE '%6.6%')
            THEN gross_units_sold ELSE 0 END)  >=1 then 1 else 0 end as is_mega_campaign_order_week5
        ,CASE WHEN SUM(CASE WHEN 
            DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=35 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=41 AND
            (fsoo.promotion_name LIKE '%9.9%' OR
            fsoo.promotion_name LIKE '%10.10%' OR
            fsoo.promotion_name LIKE '%11.11%' OR
            fsoo.promotion_name LIKE '%12.12%' OR
            fsoo.promotion_name LIKE '%EOSS%' OR
            fsoo.promotion_name LIKE '%4.4%' OR
            fsoo.promotion_name LIKE '%Birthday%' OR
            fsoo.promotion_name LIKE '%Birdthday%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%BD%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%5.5%' OR
            fsoo.promotion_name LIKE '%6.6%')
            THEN gross_units_sold ELSE 0 END)  >=1 then 1 else 0 end as is_mega_campaign_order_week6
        ,CASE WHEN SUM(CASE WHEN 
            DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=42 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=48 AND
            (fsoo.promotion_name LIKE '%9.9%' OR
            fsoo.promotion_name LIKE '%10.10%' OR
            fsoo.promotion_name LIKE '%11.11%' OR
            fsoo.promotion_name LIKE '%12.12%' OR
            fsoo.promotion_name LIKE '%EOSS%' OR
            fsoo.promotion_name LIKE '%4.4%' OR
            fsoo.promotion_name LIKE '%Birthday%' OR
            fsoo.promotion_name LIKE '%Birdthday%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%BD%' OR
            fsoo.promotion_name LIKE '%Black%' OR
            fsoo.promotion_name LIKE '%5.5%' OR
            fsoo.promotion_name LIKE '%6.6%')
            THEN gross_units_sold ELSE 0 END)  >=1 then 1 else 0 end as is_mega_campaign_order_week7


        FROM dwh.dim_product dp
        ---Deep Tagging data
        left join dwh.dim_product_lexicon_data ld 
        on dp.id_product = ld.id_product

        -----------------------------------JOIN fsoo ---------------------------------------------
        --  net unit sold, gross unit sold, gross rev, rev by id_product_attribute by store  --
        LEFT JOIN
            (SELECT id_product_attribute,
                id_product,
                fso.id_store,
                date(fso.transaction_time) as transaction_date,
                fso.id_shop,
                fso.promotion_name as promotion_name,
                fso.transaction_type,
                CASE WHEN ds.store_name = 'CentralWorld' THEN 'Central World'
                     WHEN ds.store_name = 'Interchange21' THEN 'Interchange 21' 
                     WHEN ds.store_name = 'All Seasons' THEN 'All Seasons Place' 
                     ELSE ds.store_name END as store_name,
                COALESCE(SUM(CASE WHEN transaction_type ='Return' THEN -product_quantity ELSE product_quantity END),0) as net_units_sold,
                COALESCE(SUM(CASE WHEN transaction_type ='Sale' THEN product_quantity ELSE 0 END),0) as gross_units_sold,
                COALESCE(SUM(CASE WHEN transaction_type ='Sale' THEN gross_revenue_usd ELSE 0 END),0) as gross_revenue_usd,
                COALESCE(SUM(CASE WHEN transaction_type ='Sale' THEN revenue_usd ELSE 0 END),0) as revenue_usd
            FROM dwh.fact_sales_offline fso
            LEFT JOIN dwh.dim_store ds on fso.id_store = ds.id_store
            WHERE store_name NOT IN ('Pomelo Men pop up','Bangna Warehouse','Silom Complex','Werk Ari','Siam Center (closed)')
            GROUP BY 1,2,3,4,5,6,7,8) fsoo 
        ON fsoo.id_product_attribute = dp.id_product_attribute 


        -------------------------------------- join fa -----------------------------------
        -- transfer product between stores and first day availiable date in the store --
        LEFT JOIN 
            (SELECT henry_id_product,
                    henry_id_product_attribute,
                    to_id,
                    to_location_name,
                    ds.id_store,
                    CASE WHEN ds.store_name = 'CentralWorld' THEN 'Central World'
                        WHEN ds.store_name = 'Interchange21' THEN 'Interchange 21'
                        WHEN ds.store_name = 'All Seasons' THEN 'All Seasons Place'
                        ELSE ds.store_name END as store_name_3,
                    date(MIN(date_received)) as first_available_date
            FROM dwh.fact_ns_transfer_order nto
            LEFT JOIN dwh.dim_product dp 
            ON dp.sku_complete = nto.sku_complete
            LEFT JOIN (select id_store, id_shop,
                        CASE WHEN store_name = 'Siam Center (closed)' THEN 'Siam Center'
                            WHEN store_name = 'Central World' THEN 'CentralWorld'
                            WHEN store_name = 'Interchange 21' THEN 'Interchange21'
                            WHEN store_name = 'All Seasons Place' THEN 'All Seasons'
                            WHEN store_name = 'Icon Siam' THEN 'Icon Siam (PML)'
                            WHEN store_name = 'Mega Banga' THEN 'Mega BangNa (PML)'
                            WHEN store_name = 'Central Pinklao' THEN 'Central Plaza Pinklao' ELSE store_name END as store_name,
                CASE WHEN store_name = 'Siam Center (closed)' THEN 131
                            WHEN store_name = 'Central World' THEN 72
                            WHEN store_name = 'Interchange 21' THEN 70
                            WHEN store_name = 'All Seasons Place' THEN 103
                            WHEN store_name = 'Icon Siam' THEN 14
                            WHEN store_name = 'Mega Bangna' THEN 29
                            WHEN store_name = 'EmQuartier' THEN 71
                            WHEN store_name = 'Central Pinklao' THEN 31 ELSE netsuite_location_id END as netsuite_location_id
                        FROM dwh.dim_store ds) ds 
            ON nto.to_id = ds.netsuite_location_id
            WHERE netsuite_location_id IS NOT NULL
            AND store_name NOT IN ('Pomelo Men pop up','Bangna Warehouse','Silom Complex','Werk Ari','Siam Center (closed)')
            GROUP BY 1,2,3,4,5,6) fa 
        ON fa.henry_id_product_attribute = dp.henry_id_product_attribute and fa.id_store = fsoo.id_store

        ---------------------------------join ga ------------------------------------------------------
        -- no of product give away and retail staff bought---
        LEFT JOIN 
            (SELECT id_product, 
                    SUM(CASE WHEN order_type ='Giveaway' THEN gross_units_sold ELSE 0 END) as giveaway,
                    SUM(CASE WHEN cart_rule_name like '%Retail Staff%' THEN gross_units_sold ELSE 0 END) as retail_staff
            FROM dwh.fact_sales GROUP BY 1) ga 
        ON ga.id_product = dp.id_product


        WHERE date_released BETWEEN cast('2017-01-01' as date) AND DATE_TRUNC('day', NOW() - INTERVAL '49' DAY)
        --and dp.id_product_attribute = 119095
        AND fsoo.id_store NOT IN ('39-1')
        AND fa.first_available_date IS NOT NULL 
        AND dp.parent_product_line != 'Free Gift'
        AND dp.henry_category_1 NOT IN ('Accessories','Bags','Bath&Body','Beverage Container','Cosmetics','Hair','Miscellaneous','Shoes','Skin Care','Stationery')
        AND dp.brand IN ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio','Blackdog BKK')
        AND dp.product_cost_usd_first_order_date > 0 
        AND dp.size !=  '33'
        AND dp.release_collection_name IS NOT NULL
        AND dp.fabric_custom_name IS NOT NULL
        AND dp.original_price_th_to_usd IS NOT NULL
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43
        """
    raw = wr.athena.read_sql_query(sql, database="dwh")
    return raw
