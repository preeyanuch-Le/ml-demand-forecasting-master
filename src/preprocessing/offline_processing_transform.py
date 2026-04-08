import argparse
import os
import warnings
from config.project_config import STORE_CLUSTER_FILE,STORE_CLUSTER_SHEET_NAME,STORE_CLUSTER_FILE_DEPLOY
import pandas as pd
import numpy as np
from src.utils.decorators import timeit
from sklearn.preprocessing import LabelEncoder
import pickle


from sklearn.exceptions import DataConversionWarning
warnings.filterwarnings(action='ignore', category=DataConversionWarning)

import boto3


# new store cluster
new_store_cluster = pd.read_excel(STORE_CLUSTER_FILE,sheet_name=STORE_CLUSTER_SHEET_NAME,engine='openpyxl',skiprows=1,
                                 usecols = "C:D,H,P:U")
new_store_cluster = new_store_cluster.dropna(subset=['Store'], how='all')
new_store_cluster_unpivot = pd.melt(new_store_cluster, id_vars=['Store','id_store','id_shop_name'], value_vars=['Weekend', 'Workwear', 'Collaboration','Collection','Basic','Denims'])
new_store_cluster_unpivot.rename(columns = {'Store':'store_name','variable':'sub_product_line','value':'cluster'},inplace=True)
new_store_cluster_unpivot = new_store_cluster_unpivot[['store_name','sub_product_line','id_shop_name','cluster']]
# save unpivot file store cluster here to be used in deployment later (so we have the same copy of store cluster used in model training and deployment)
new_store_cluster_unpivot.to_csv(STORE_CLUSTER_FILE_DEPLOY,index=False)


@timeit
def transform_data(raw, store_clusters=new_store_cluster_unpivot ):
    raw['date_released'] = pd.to_datetime(raw['date_released'])
    raw['max_transaction_date'] = pd.to_datetime(raw['max_transaction_date'])
    raw['first_available_date'] = pd.to_datetime(raw['first_available_date'])
    raw['min_transaction_date'] = pd.to_datetime(raw['min_transaction_date'])

    # fill missing value
    raw['inventory_per_store'].fillna(0, inplace=True)
    raw['count_products_in_group'].fillna(0, inplace=True)

    raw["traffic_dist"].fillna(0, inplace=True)
    raw["retail_mkt_spend_dist"].fillna(0, inplace=True)

    # if min_transaction_date before first available date, then that will be the new first availble date
    raw["first_available_date"] = np.where((raw["first_available_date"] < raw["min_transaction_date"])
                                           & (~raw["min_transaction_date"].isna())
                                           , raw["first_available_date"], raw["min_transaction_date"])

    raw["datediff_first_max"] = (raw["max_transaction_date"] - raw["first_available_date"]).dt.days
    # before datediff_first_max is calculate before replacing first_available_date with min_transaction_date -> make datediff_first_max has negative value

    # fill days_in_stock_lt with datediff_first_max
    raw["days_in_stock_lt"] = np.where(raw["days_in_stock_lt"].isna(), raw["datediff_first_max"],
                                       raw["days_in_stock_lt"])

    # cleaning id_store
    # ex. EmQuartier has many id_store, this is clean and standize it
    raw.loc[raw['store_name'] == 'EmQuartier', 'id_store'] = 251
    raw.loc[raw['store_name'] == 'Central World', 'id_store'] = 261
    raw.loc[raw['store_name'] == 'Mega BangNa (PML)', 'id_store'] = 211
    raw.loc[raw['store_name'] == 'Icon Siam (PML)', 'id_store'] = 11
    raw.loc[raw['store_name'] == 'Central Plaza Pinklao', 'id_store'] = 221
    raw.loc[raw['store_name'] == 'Interchange 21', 'id_store'] = 241
    raw.loc[raw['store_name'] == 'All Seasons Place', 'id_store'] = 231
    raw.loc[raw['store_name'] == 'Central Rama 3', 'id_store'] = 321
    raw.loc[raw['store_name'] == 'Central Rama 9', 'id_store'] = 311
    raw.loc[raw['store_name'] == 'Central Phuket', 'id_store'] = 301
    raw.loc[raw['store_name'] == 'Zpell', 'id_store'] = 331
    raw.loc[raw['store_name'] == 'Central Ladprao', 'id_store'] = 341
    raw.loc[raw['store_name'] == '313 Somerset', 'id_store'] = 12
    raw.loc[raw['store_name'] == 'The Mall Ngamwongwan (PML)', 'id_store'] = 351
    raw.loc[raw['store_name'] == 'Terminal 21 Asoke (PML)', 'id_store'] = 361
    raw.loc[raw['store_name'] == 'Siam Center', 'id_store'] = 371
    raw.loc[raw['store_name'] == 'Fashion Island', 'id_store'] = 381
    raw.loc[raw['store_name'] == 'SG NEX', 'id_store'] = 32
    raw.loc[raw['store_name'] == 'ID Central Park', 'id_store'] = 15
    raw.loc[raw['store_name'] == 'SG Jem', 'id_store'] = 42
    raw.loc[raw['store_name'] == 'ID Kota Kasablanka', 'id_store'] = 35
    raw.loc[raw['store_name'] == 'Central Festival Eastville', 'id_store'] = 411
    raw.loc[raw['store_name'] == 'Seacon Bangkae', 'id_store'] = 401

    raw = raw[~raw.id_store.isna()]

    raw['id_store'] = raw["id_store"].astype(str).astype(int)

    # merge store cluster
    raw = raw.merge(store_clusters[['store_name','sub_product_line','cluster']], how='left', on=['store_name','sub_product_line'])
    
    # add spl_cluster index
    raw['spl_cluster'] = raw['sub_product_line'] +'_'+ raw['cluster']

    # remove negative net_units_sold
    raw = raw[raw['net_units_sold'] >= 0]

    # did not extrapolate
    raw['adjusted_net_units_sold'] = raw['net_units_sold'].apply(np.round)
    # it actually equal just want to use the same name as dfm1.0

    # discount
    raw['discount_utilization'] = np.where(raw['gross_revenue_usd'] == 0,
                                           0, 1 - (raw['revenue_usd'] / raw['gross_revenue_usd']))

    raw['item_discount_percent'] = np.where(raw['gross_revenue_usd'] == 0,
                                            0, raw['item_discount_usd'] / raw['gross_revenue_usd'])

    raw['voucher_discount_percent'] = np.where(raw['gross_revenue_usd'] == 0,
                                               0, raw['voucher_discount_usd'] / raw['gross_revenue_usd'])

    raw['giveaway'] = raw['giveaway'].fillna(0)

    # clean fabric name
    raw['hscode_id_fabric_name'] = np.where(raw.hscode_id_fabric_name.isnull(), 'No Fabric', raw.hscode_id_fabric_name)

    raw.loc[raw['old_henry_category_1'] == 'Dresses', 'old_henry_category_3'] = raw.old_henry_category_2
    raw.loc[raw['old_henry_category_1'] == 'Dresses', 'old_henry_category_2'] = raw.old_henry_category_1


    # add week_id,id_shop_name
    # rm license product
    # drop col:
    # min_transaction_date,max_transaction_date,release_collection_name,id_store,days_in_stock_lt,id_shop,date_released,first_available_date,store_name,
    # day_number,brand,net_units_sold,gross_revenue_usd,revenue_usd,start_date_weekly,datediff_first_max

    # drop some columns and rearrage
    columns_3 = ['henry_id_product_attribute', 'henry_id_product', 'id_product_attribute', 'id_product',
                 'product_cost_usd', 'original_price_usd', 'size', 'product_line'
        , 'sub_product_line', 'henry_category_1', 'henry_category_2', 'henry_category_3', 
        'old_henry_category_1', 'old_henry_category_2', 'old_henry_category_3','simple_color'
        , 'fabric_custom_name', 'hscode_id_fabric_name', 'is_mega_campaign_order', 'giveaway', 'cluster'
        , 'adjusted_net_units_sold', 'first_available_month', 'first_available_dow', 'first_available_year'
        , 'discount_utilization', 'item_discount_percent', 'voucher_discount_percent',
                 'first_week_of_month', 'last_week_of_month', 'color', 'warehouse', 'style', 'sleeve'
        , 'pattern', 'sleevestyle', 'neckline', 'shape', 'rise', 'release_collection_name', 'covid_lockdown'
        , 'count_products_in_group', 'inventory_per_store', 'week_id', 'id_shop_name', 'traffic_dist',
                 'retail_mkt_spend_dist','date_released','first_available_date','store_name','id_store','spl_cluster']

    ready_data = raw[columns_3]

   
    return ready_data





