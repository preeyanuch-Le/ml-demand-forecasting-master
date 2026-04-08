import awswrangler as wr
from src.utils.decorators import timeit

@timeit
def get_existing_styles():
    sql = """
-- EXITING STYLES --
        select ca.year_week_id
        ,case when fisom.id_shop is null then '1' else fisom.id_shop end as id_shop
        ,case when id_store = '5b989110a9afac06f24b80ce' then '231'
            when id_store = '5b55a681868e636a8ab1eaa2' then '261'
            when id_store = '5bb2e6ac1e5abf2252df039c' then '251'
            when id_store = '5bbedd0bc3cef17af5b1a3ab' then '11'
            when id_store = '5b9890f4d49bab0743f5689e' then '241'
            when id_store = '5a7c011b6878b592402d76bd' then '371'
            else id_store
            end as id_store
        ,count(distinct dp2.id_product) as active_product
        from dwh.fact_inventory_snapshot_offline_master fisom 
        left join (select distinct id_product
        from dwh.dim_product dp
        where dp.date_released between date('2016-12-26') and DATE_TRUNC('day', NOW() - INTERVAL '1' DAY)
        and dp.id_product is not null
        and dp.original_price_th is not NULL 
        and dp.date_released is not NULL 
        and dp.parent_product_line is not null
        and dp.product_name is not null
        and dp.product_line is not null
        and dp.original_price_th != 0
        and dp.henry_category_1 not in ('Accessories', 'Bags', 'Cosmetics', 'Miscellaneous')
        --,'Lingerie Bodysuit', 'Sportswear Bottoms', 'Sportswear Outerwear', 'Sportswear Tops', 'Swimwear')
        and dp.parent_product_line not in ('Cosmetics','Accessories','Free Gift','3rd Party')
        and brand in ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio')
        and dp.id_product not in (19, 41, 14196, 14197, 14198, 14199, 14200, 14201, 14202, 14203, 14204, 14205, 14206, 14207, 14208, 17131, 17132, 17654,17182)
        and dp.active = 1) dp2
        on fisom.id_product = dp2.id_product	
        left join (select full_date
        , case when ca.week_of_year_number  in (1,2,3,4,5,6,7,8,9) 
        then concat(cast(ca.year_id as varchar),'0',CAST(ca.week_of_year_number AS VARCHAR))
             when ca.week_of_year_number = 53 or ca.week_of_year_number = 54
              then concat(cast(ca.year_id+1 as varchar),'01')
            else concat(cast(ca.year_id as varchar),cast(ca.week_of_year_number as varchar)) 
            end as year_week_id
        from dwh.dim_calendar ca) ca
        on fisom.snapshot_date = ca.full_date
        where active = 1
        and fisom.snapshot_date between date('2018-06-22') and DATE_TRUNC('day', NOW() - INTERVAL '49' DAY)
        and id_store not in ('1','201', '25', '271')	
        group by id_shop
            ,id_store
            ,ca.year_week_id
"""
    existing_styles = wr.athena.read_sql_query(sql, database="dwh")
    return existing_styles
