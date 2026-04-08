import awswrangler as wr
from src.utils.decorators import timeit

@timeit
def get_product_lauched():
    sql = """
----/* NEW PRODUCT LAUNCHED */----
---- get total product launched in that week ----
select distinct year_week_available
        ,id_store as id_store
        ,sum(tot_product_launched) as product_launched 
        from (select distinct avl_store.year_week_available
        ,avl_store.id_store as id_store
        ,count(distinct avl_store.id_product) as tot_product_launched
        from (select dp2.id_product
        ,dp2.sku_complete 
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
        from dwh.fact_ns_transfer_order ns
        left join (select full_date
        , case when ca.week_of_year_number  in (1,2,3,4,5,6,7,8,9) 
        then concat(cast(ca.year_id as varchar),'0',CAST(ca.week_of_year_number AS VARCHAR))
             when ca.week_of_year_number = 53 or ca.week_of_year_number = 54
              then concat(cast(ca.year_id+1 as varchar),'01')
            else concat(cast(ca.year_id as varchar),cast(ca.week_of_year_number as varchar))
            end as year_week_id
        from dwh.dim_calendar ca) ca
        on  cast(ns.date_received as date) = cast(ca.full_date as date)
        left join (select id_product
        ,sku_complete 
        from dwh.dim_product dp
        where dp.date_released between date('2016-12-26') and DATE_TRUNC('day', NOW() - INTERVAL '49' DAY)
            and dp.id_product is not null
            and dp.original_price_th is not NULL 
            and dp.date_released is not NULL 
            and dp.parent_product_line is not null
            and dp.product_name is not null
            and dp.product_line is not null
            and dp.original_price_th != 0
            and dp.henry_category_1 not in ('Accessories', 'Bags', 'Cosmetics', 'Miscellaneous')
            and dp.parent_product_line not in ('Cosmetics','Accessories','Free Gift','3rd Party')
            and brand in ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio')
            and dp.id_product not in (19, 41, 14196, 14197, 14198, 14199, 14200, 14201, 14202, 14203, 14204, 14205, 14206, 14207, 14208, 17131, 17132, 17654,17182)
            and dp.active = 1) dp2
        on ns.sku_complete = dp2.sku_complete 
        where id_product is not null
        GROUP BY 1,2,3,4
         ) avl_store
        where year_week_available is not null 
        and id_store is not null
        group by avl_store.year_week_available
            ,avl_store.id_store)
        group by 1,2
"""
    product_lauched = wr.athena.read_sql_query(sql, database="dwh")
    return product_lauched
