import pandas as pd
import numpy as np
from src.utils.decorators import timeit
import awswrangler as wr

@timeit
def get_size_dist_offline():
    sql = """
 SELECT 
 distinct
        dp.size,
        dp.product_line as sub_product_line,
        dp.new_category_1 as henry_category_1,
        fsoo.id_shop,
        SUM(CASE WHEN DATE_DIFF('day',dp.date_released,fsoo.transaction_date)>=0 AND DATE_DIFF('day',dp.date_released,fsoo.transaction_date)<=44 THEN net_units_sold ELSE 0 END) as net_sales_f45
FROM dwh.dim_product dp
LEFT JOIN
            (SELECT id_product_attribute,
                fso.id_shop,
                fso.id_store,
                date(fso.transaction_time) as transaction_date,
                fso.transaction_type,
                COALESCE(SUM(CASE WHEN transaction_type ='Return' THEN -product_quantity ELSE product_quantity END),0) as net_units_sold
            FROM dwh.fact_sales_offline fso
            LEFT JOIN dwh.dim_store ds on fso.id_store = ds.id_store
            WHERE store_name NOT IN ('Pomelo Men pop up','Bangna Warehouse','Silom Complex','Werk Ari','Siam Center (closed)')
            GROUP BY 1,2,3,4,5) fsoo 
        ON fsoo.id_product_attribute = dp.id_product_attribute 
WHERE dp.date_released BETWEEN DATE_TRUNC('day', NOW() - INTERVAL '180' DAY) AND DATE_TRUNC('day', NOW())
AND fsoo.id_store NOT IN ('39-1')
        AND dp.parent_product_line != 'Free Gift'
        AND dp.henry_category_1 NOT IN ('Accessories','Bags','Bath&Body','Beverage Container','Cosmetics','Hair','Miscellaneous','Shoes','Skin Care','Stationery')
        AND dp.brand IN ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio','Blackdog BKK')
        AND dp.product_cost_usd_first_order_date > 0 
        AND dp.size !=  '33'
        AND dp.release_collection_name IS NOT NULL
        AND dp.fabric_custom_name IS NOT NULL
        AND dp.original_price_th_to_usd IS NOT NULL
        GROUP BY 1,2,3,4
    
    """
    size_dist_offline = wr.athena.read_sql_query(sql, database="dwh")
    return size_dist_offline

@timeit
def resize_dist(size_dist_offline, final_data: pd.DataFrame) -> pd.DataFrame:
    """
    - take the size distribution of the past 6 months then re size the result from the model 
    - size distrituion is calculated by using shop, henry cat1, and sub product line
    
    """
    size_dist_offline['id_shop'] = size_dist_offline['id_shop'].astype(int)
    
    size_dist_offline["id_shop_name"] = np.select(
        [
            size_dist_offline["id_shop"] == 1,
            size_dist_offline["id_shop"] == 2,
            size_dist_offline["id_shop"] == 5,
            size_dist_offline["id_shop"] == 11,
        ],
        ["TH", "SG", "ID", "MY"],
    )

    size_dist_offline = size_dist_offline[
        ["sub_product_line", "henry_category_1", "size", "id_shop_name", "net_sales_f45"]
    ]
#     size_dist_offline["net_sales_f45"] = size_dist_offline["net_sales_f45"].str.replace(
#         ",", ""
#     )
#     size_dist_offline["net_sales_f45"] = size_dist_offline["net_sales_f45"].astype(int)

    size_dist_offline2 = size_dist_offline.groupby(
        ["sub_product_line", "henry_category_1", "id_shop_name"]
    )["net_sales_f45"].sum()

    size_dist_offline2 = size_dist_offline2.reset_index()
    size_dist_offline2.rename(columns={"net_sales_f45": "total_sale_by_size"}, inplace=True)

    size_dist_offline3 = size_dist_offline.merge(
        size_dist_offline2,
        how="left",
        on=["sub_product_line", "henry_category_1", "id_shop_name"],
    )

    size_dist_offline3["size_dist"] = (
        size_dist_offline3["net_sales_f45"] / size_dist_offline3["total_sale_by_size"]
    )
    size_dist_offline3.dropna(inplace=True)
    size_dist_offline3["size"] = size_dist_offline3["size"].astype(str)
    
    final_data["size"] = final_data["size"].astype(str)
    final_data = final_data.merge(
        size_dist_offline,
        how="left",
        on=["sub_product_line", "henry_category_1", "size", "id_shop_name"],
    )

    final_data_group = (
        final_data.groupby(
            [
                "id_product",
                "id_shop_name",
                "store_cluster",
                "sub_product_line",
                "henry_category_1",
                "week_id",
            ]
        )["pred"]
        .sum()
        .reset_index()
    )

    size_temp = final_data[["id_product", "id_product_attribute", "size"]].drop_duplicates()
    final_data_group2 = final_data_group.merge(size_temp, how="left", on=["id_product"])
    size_dist_offline3 = size_dist_offline3[
        ["sub_product_line", "henry_category_1", "size", "id_shop_name", "size_dist"]
    ]
    final_data_group3 = final_data_group2.merge(
        size_dist_offline3,
        how="left",
        on=["sub_product_line", "henry_category_1", "id_shop_name", "size"],
    )

    # final_data_group4 = final_data_group3.merge(size_dist_offline3,how='left',
    #                                         on=['sub_product_line','id_shop_name','size'])

    size_dist_offline4 = (
        size_dist_offline3.groupby(["sub_product_line", "size", "id_shop_name"])[
            "size_dist"
        ]
        .mean()
        .reset_index()
    )

    final_data_group3.loc[final_data_group3["size_dist"].isna(), "size_dist"] = (
        final_data_group3[final_data_group3["size_dist"].isna()]
        .merge(
            size_dist_offline4, how="left", on=["sub_product_line", "id_shop_name", "size"]
        )["size_dist_y"]
        .values
    )

    final_data_group3[
        (final_data_group3["id_shop_name"] == "MY")
        & (final_data_group3["size"].isin(["27", "28"]))
    ]

    final_data_group3["adjusted_pred"] = (
        final_data_group3["pred"] * final_data_group3["size_dist"]
    )
    # Don't adjust for slash sizes
    final_data_group3.loc[
        final_data_group3['size'].isin(['XXS/XS', 'S/M', 'L/XL', 'XS/S', 'M/L', 'XL/XXL']), 'adjusted_pred'] = final_data_group3['pred']

    re_columns =[
            "master_style_id",
            "id_product",
            "id_product_attribute",
            "warehouse",
            "id_shop_name",
            "store_cluster",
            "week_id",
            "released_collection_name",
            "product_cost_usd",
            "size",
            "sub_product_line",
            "henry_category_1",
            "henry_category_2",
            "henry_category_3",
            "color",
            "simple_color",
            "original_price_usd",
            "fabric_custom_name",
            "hscode_id_fabric_name",
            "giveaway",
            "style",
            "sleeve",
            "pattern",
            "sleevestyle",
            "neckline",
            "shape",
            "no_store",
            "rise"]
    final_list_products = final_data[re_columns].drop_duplicates()

    final_list = final_list_products.merge(
        final_data_group3,
        how="left",
        on=[
            "id_product_attribute",
            "id_product",
            "size",
            "sub_product_line",
            "henry_category_1",
            "id_shop_name",
            "store_cluster",
            "week_id"
        ],
    )
    return final_list


