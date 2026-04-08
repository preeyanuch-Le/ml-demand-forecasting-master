import awswrangler as wr
from src.utils.decorators import timeit

@timeit
def get_product_sold():
    sql = """
----/* PRODUCT SOLD FOR EXISTING STYLES */----                           
select distinct t.year_week_sold
        ,t.year_month_id
        ,t.id_shop
        ,t.id_warehouse
        ,t.id_store
        --- new & old - normal & lookbook ---
        ,case when product_age = 'new' then sum(t.tot_net_units_sold) else 0 end as new_pd_sold
        ,case when product_age = 'old' then sum(t.tot_net_units_sold) else 0 end as old_pd_sold
        ,sum(t.tot_net_units_sold) as net_units_sold
        from 
        (--- fact sales ---
        select dp2.id_product 
        ,fso2.id_shop
        --- case erply_store_id ---
        ,case when fso2.id_store = '5b989110a9afac06f24b80ce' then '231'
            when fso2.id_store = '5b55a681868e636a8ab1eaa2' then '261'
            when fso2.id_store = '5bb2e6ac1e5abf2252df039c' then '251'
            when fso2.id_store = '5bbedd0bc3cef17af5b1a3ab' then '11'
            when fso2.id_store = '5b9890f4d49bab0743f5689e' then '241'
            when fso2.id_store = '5a7c011b6878b592402d76bd' then '371'
            else fso2.id_store end as id_store
        ,case when id_shop = '2' and year_week_id < '202017' then '1'
            when id_shop = '2' and year_week_id >= '202017' then '11'
            when id_shop = '11' and year_week_id < '202038' then '1'
            when id_shop = '11' and year_week_id >= '202038' then '11'
            when id_shop = '10' then '1'
            when id_shop = '12' then '1'
            when id_shop = '4' then '1'
            when id_shop = '14' then '11'
            else id_shop end as id_warehouse
        ,ca.year_week_id as year_week_sold
        ,ca.year_month_id
        --- first date available ---
        ,min(pd_avl.year_week_available) as year_week_avl
        --- product age ---
        ,case when min(pd_avl.year_week_available) = ca.year_week_id then 'new'
        --when ca.year_week_id - year_week_avl = 1 then 'new' 
        --when ca.year_week_id - year_week_avl = 2 then 'new' 
        else 'old' end as product_age
        ,coalesce (sum(CASE WHEN fso2.transaction_type ='Return' THEN -fso2.product_quantity ELSE fso2.product_quantity END),0) as tot_net_units_sold
        from dwh.fact_sales_offline fso2
        ---/* LOOKBOOK COLLECTION */---
        left join (select distinct id_product
        ,sku_complete
        ,date_released
        from dwh.dim_product
        where date_released between date('2016-12-26') and DATE_TRUNC('day', NOW() - INTERVAL '1' DAY)
        and id_product is not null
        and original_price_th is not NULL 
        and date_released is not NULL 
        and parent_product_line is not null
        and product_name is not null
        and product_line is not null
        and original_price_th != 0
        and henry_category_1 not in ('Accessories', 'Bags', 'Cosmetics', 'Miscellaneous')
        and parent_product_line not in ('Cosmetics','Accessories','Free Gift','3rd Party')
        and brand in ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio')
        and id_product not in (19, 41, 14196, 14197, 14198, 14199, 14200, 14201, 14202, 14203, 14204, 14205, 14206, 14207, 14208, 17131, 17132, 17654,17182)
        and active = 1
        ) dp2
        on fso2.id_product = dp2.id_product
        ----/* Available date from NS */----
        left join (select sku_complete
        ,to_location_name
        ,case when to_id = 14 then 11
            when to_id = 29 then 211
            when to_id = 31 then 221
            when to_id = 69 then 231
            when to_id = 70 then 241
            when to_id = 71 then 251
            when to_id = 72 then 261
            when to_id = 73 then 281
            when to_id = 81 then 12
            when to_id = 92 then 311
            when to_id = 93 then 341
            when to_id = 95 then 321
            when to_id = 96 then 291
            when to_id = 99 then 301
            when to_id = 102 then 361
            when to_id = 103 then 331
            when to_id = 115 then 351
            when to_id = 116 then 381
            when to_id = 130 then 15
            when to_id = 131 then 371
            when to_id = 132 then 32
            when to_id = 159 then 391
            when to_id = 163 then 35
            when to_id = 165 then 42
            when to_id = 168 then 401
            when to_id = 175 then 411
            when to_id = 177 then 431
            when to_id = 178 then 111
            when to_id = 182 then 391
            when to_id = 186 then 441
            else null
            end as id_store
        ,min(ca.year_week_id) as year_week_available
        ,min(date(date_received)) as first_available_date
        FROM dwh.fact_ns_transfer_order ns
        ----/* YEAR WEEK ID FOR FIRST AVAILABLE DATE  */----
        left join (select full_date,
		case when ca.week_of_year_number  in (1,2,3,4,5,6,7,8,9) 
        then concat(cast(ca.year_id as varchar),'0',CAST(ca.week_of_year_number AS VARCHAR))
             when ca.week_of_year_number = 53 or ca.week_of_year_number = 54
              then concat(cast(ca.year_id+1 as varchar),'01')
            else concat(cast(ca.year_id as varchar),cast(ca.week_of_year_number as varchar)) 
            end as year_week_id
        from dwh.dim_calendar ca) ca
        on  date(ns.date_received) = ca.full_date 
        where 3 is not null
        GROUP BY 1,2,3
        order by sku_complete
        ) pd_avl
        on cast(dp2.sku_complete as varchar) = pd_avl.sku_complete and cast(pd_avl.id_store as varchar) = fso2.id_store 
        ----/* YEAR WEEK ID FOR TRANSACTION  */----
        left join (select full_date
        ,case when ca.month_id in (1,2,3,4,5,6,7,8,9) 
            then concat(cast(ca.year_id as varchar),'0',CAST(ca.month_id AS VARCHAR))
            else concat(cast(ca.year_id as varchar),cast(ca.week_of_year_number as varchar)) end as year_month_id
        , 
			case when ca.week_of_year_number  in (1,2,3,4,5,6,7,8,9) 
        then concat(cast(ca.year_id as varchar),'0',CAST(ca.week_of_year_number AS VARCHAR))
             when ca.week_of_year_number = 53 or ca.week_of_year_number = 54
              then concat(cast(ca.year_id+1 as varchar),'01')
            else concat(cast(ca.year_id as varchar),cast(ca.week_of_year_number as varchar)) 
            end as year_week_id
        from dwh.dim_calendar ca) ca
        on  date(fso2.transaction_time) = ca.full_date
        where fso2.revenue_usd >0
        ---- NOTE: Marketing spend started 2019 only ----
        and transaction_time >= date('2019-01-01')
        and sales_id is not null
        and dp2.id_product is not null
        and pd_avl.year_week_available is not null
        group by dp2.id_product
            ,fso2.id_shop
            ,fso2.id_store
            ,ca.year_week_id
            ,ca.year_month_id
 		having ca.year_week_id >= min(pd_avl.year_week_available)) t
        group by t.year_week_sold
            ,t.id_shop
            ,t.year_month_id
            ,t.id_warehouse
            ,t.id_store
            ,product_age
"""
    product_sold = wr.athena.read_sql_query(sql, database="dwh")
    return product_sold
