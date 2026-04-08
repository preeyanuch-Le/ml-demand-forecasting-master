import awswrangler as wr
from src.utils.decorators import timeit

@timeit
def get_tot_product_style():
    sql = """
-- inv by store            
select distinct t.id_product
                , t.id_shop
                , t.id_store
                , date(t.snapshot_date) as full_date
                        from dwh.fact_inventory_snapshot_offline_master t
                        left join dwh.dim_product dp 
                        on t.id_product = dp.id_product
                        where dp.henry_category_1 not in ('Accessories', 'Bags', 'Cosmetics', 'Miscellaneous')
                        and dp.parent_product_line not in ('Cosmetics','Accessories','Free Gift','3rd Party')
                        and dp.brand in ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio') 
"""
    tot_product_style = wr.athena.read_sql_query(sql, database="dwh")
    return tot_product_style
