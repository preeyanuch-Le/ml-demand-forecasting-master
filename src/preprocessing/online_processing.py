import pandas as pd
import numpy as np



def transform_data(raw, lookbook_collection_spend, lookbook_pv_dist, pv_dist_train):

    raw['inventory_level'].fillna(0, inplace=True)
    raw['count_products_in_group'].fillna(0, inplace=True)

    # standardized feature for dresses
    raw.loc[raw['henry_category_1'] == 'Dresses', 'henry_category_3'] = raw.henry_category_2
    raw.loc[raw['henry_category_1'] == 'Dresses', 'henry_category_2'] = raw.henry_category_1

    raw['released_month'] = raw['released_month'].str.strip()

    raw['days_in_stock_lt'] = raw['days_in_stock_lt'].replace(0, 1)

    # remove negative net sales
    raw = raw[raw['net_units_sold'] >= 0]
    raw['adjusted_net_units_sold'] = raw['net_units_sold'].apply(np.round)  # did not extrapolate

    # remove na value
    raw = raw[~raw['pv'].isna()]
    
    print('Calculate country distribution')
    #prop of product sold in each warehouse compare to all warehouse
    product_group_sales = raw.groupby(['id_product'])['adjusted_net_units_sold'].sum().reset_index()
    product_group_by_country = raw.groupby(['id_product', 'warehouse'])['adjusted_net_units_sold'].sum().reset_index()
    country_distribution = product_group_sales.merge(product_group_by_country, how='left', on=['id_product'])
    country_distribution['country_dist'] = (
        country_distribution['adjusted_net_units_sold_y'] / country_distribution['adjusted_net_units_sold_x']
    )
    raw_3 = raw.merge(
        country_distribution[['id_product', 'warehouse', 'country_dist']],
        how='left',
        on=['id_product', 'warehouse']
    )

    print('Calculate style in drop')
    # how many products in each collection because if the collection has 5 products, the sales should be different than if the collection has 50 products
    release_per_drop = raw_3.groupby(['release_collection_name', 'warehouse']).id_product.nunique()
    raw_3 = raw_3.merge(release_per_drop, how='left', on=['release_collection_name', 'warehouse'])
    raw_3 = raw_3.rename(columns={'id_product_x': 'id_product', 'id_product_y': 'styles_in_drop'})
    
    print(raw_3.collection_sizes.unique())
    raw_3.loc[(raw_3['collection_sizes'] == 'l'), 'is_big_collection'] = 1
    raw_3['is_big_collection'].fillna(0, inplace = True)
    
    raw_3.loc[(raw_3['is_lookbook_collection'] == 0) & (raw_3['year'] == '2017'), 'small_collection_spend'] = 600
    raw_3.loc[(raw_3['is_lookbook_collection'] == 0) & (raw_3['year'] == '2018'), 'small_collection_spend'] = 688
    raw_3.loc[(raw_3['is_lookbook_collection'] == 0) & (raw_3['year'] == '2019'), 'small_collection_spend'] = 596
    raw_3.loc[(raw_3['is_lookbook_collection'] == 0) & (raw_3['year'] == '2020'), 'small_collection_spend'] = 1153
    raw_3.loc[(raw_3['is_lookbook_collection'] == 0) & (raw_3['year'] == '2021'), 'small_collection_spend'] = 1142
    raw_3.loc[(raw_3['is_lookbook_collection'] == 0) & (raw_3['year'] == '2022'), 'small_collection_spend'] = 1150
    
    print(f'unique values in small_collection_spend: {raw_3.small_collection_spend.unique()}')
    print('Add lookbook collection data')    
    lookbook_pv_dist = lookbook_pv_dist[['warehouse', 'week_id', 'lookbook_pv_dist']]

    raw_3 = raw_3.merge(lookbook_pv_dist, on=['warehouse', 'week_id'], how='left')

    # lookbook is from creative team ( eg. photoshoot)
    # used country_distribution to divided for each country
    lookbook_collection_spend['release_collection_name'] = lookbook_collection_spend.release_collection_name.str.strip()
    raw_3['release_collection_name'] = raw_3.release_collection_name.str.strip()

    raw_3 = raw_3.merge(lookbook_collection_spend, how='left', on='release_collection_name')
    # creative_mkt_spend    / lookbook_spend
    raw_3['lookbook_spend_all'] = np.where(raw_3['lookbook_spend'].isna(),
                                           raw_3['small_collection_spend'] * raw_3['country_dist'] * raw_3[
                                               'lookbook_pv_dist'],
                                           (raw_3['lookbook_spend']) * raw_3['country_dist'] * raw_3[
                                               'lookbook_pv_dist'])

    print('Striping columns...') 
    columns = [
        'released_month',
        'day_of_week',
        'sub_product_line',
        'henry_category_1',
        'henry_category_2',
        'henry_category_3',
        'old_henry_category_1',
        'old_henry_category_2',
        'old_henry_category_3',
        'simple_color',
        'color',
        'hscode_id_fabric_name', 
        'style',
        'sleeve',
        'pattern',
        'sleevestyle',
        'neckline',
        'shape',
        'rise',
        'size'
    ]

    for col in columns:
        raw_3[col] = raw_3[col].str.strip()

    # changing Skirts - Mini/Dresses - Mini to mini in cat3 so when you do dummy varible, there will be less columns
    raw_3.loc[raw_3['old_henry_category_3'] == 'Skirts - Mini', 'old_henry_category_3'] = 'Mini'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Skirts - Midi', 'old_henry_category_3'] = 'Midi'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Skirts - Maxi', 'old_henry_category_3'] = 'Maxi'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Skirts - Knee Length', 'old_henry_category_3'] = 'Knee Length'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Shirts - Short Sleeve', 'old_henry_category_3'] = 'Short Sleeve'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Shirts - Long Sleeve', 'old_henry_category_3'] = 'Long Sleeve'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Shirts - Sleeveless', 'old_henry_category_3'] = 'Sleeveless'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Shirts - Crop Top', 'old_henry_category_3'] = 'Crop Top'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Blouses - Short Sleeve', 'old_henry_category_3'] = 'Short Sleeve'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Blouses - Long Sleeve', 'old_henry_category_3'] = 'Long Sleeve'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Blouses - Sleeveless', 'old_henry_category_3'] = 'Sleeveless'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Blouses - Crop Top', 'old_henry_category_3'] = 'Crop Top'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Tees - Short Sleeve', 'old_henry_category_3'] = 'Short Sleeve'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Tees - Long Sleeve', 'old_henry_category_3'] = 'Long Sleeve'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Tees - Sleeveless', 'old_henry_category_3'] = 'Sleeveless'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Tees - Crop Top', 'old_henry_category_3'] = 'Crop Top'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Tanks - Crop Top', 'old_henry_category_3'] = 'Crop Top'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Dresses - Mini', 'old_henry_category_3'] = 'Mini'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Dresses - Midi', 'old_henry_category_3'] = 'Midi'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Dresses - Maxi', 'old_henry_category_3'] = 'Maxi'
    raw_3.loc[raw_3['old_henry_category_3'] == 'Dresses - Knee Length', 'old_henry_category_3'] = 'Knee Length'

    # replace all the -,/_
    print('Cleaning strings...')
    for col in columns:
        raw_3[col] = raw_3[col].str.lower()
        raw_3[col] = raw_3[col].replace(" ", "_", regex=True)
        raw_3[col] = raw_3[col].replace("-", "", regex=True)
        raw_3[col] = raw_3[col].replace("/", "", regex=True)
        raw_3[col] = raw_3[col].replace("__", "_", regex=True)

    print('Add mkt pageview dist')
    pv_dist_train = pv_dist_train[['warehouse', 'week_id', 'pv_dist']]
    pv_dist_train.rename(columns={'pv_dist': 'mkt_pv_dist'}, inplace=True)
    raw_3 = raw_3.merge(pv_dist_train, on=['warehouse', 'week_id'], how='left')

    # TODO: convert to category dtype
    print('Converting columns to strings')
    categorical_cols = [
        'released_month',
        'day_of_week',
        'week_id',
        'sub_product_line',
        'henry_category_1',
        'henry_category_2',
        'henry_category_3',
        'old_henry_category_1',
        'old_henry_category_2',
        'old_henry_category_3',
        'simple_color',
        'color',
        'hscode_id_fabric_name',
        'size',
        'style',
        'sleeve',
        'pattern',
        'sleevestyle',
        'neckline',
        'shape',
        'rise'
        ]

    raw_3['style'] = raw_3['style'].astype(str)
    raw_3['sleeve'] = raw_3['sleeve'].astype(str)
    raw_3['pattern'] = raw_3['pattern'].astype(str)
    raw_3['sleevestyle'] = raw_3['sleevestyle'].astype(str)
    raw_3['neckline'] = raw_3['neckline'].astype(str)
    raw_3['shape'] = raw_3['shape'].astype(str)
    raw_3['rise'] = raw_3['rise'].astype(str)
    raw_3['fabric_custom_name'] = raw_3['fabric_custom_name'].astype(str)

    continuous_cols = ['is_mega_campaign_order', 'giveaway', 'first_week_of_month', 'last_week_of_month','is_big_collection', 'tie']

    for j in continuous_cols:
        raw_3[j] = (raw_3[j]).astype(int)

    ready_data = raw_3.copy()

    print('Calculating size distribution for each product id')
    
    product_group = ready_data.groupby(['id_product', 'warehouse', 'week_id'])['adjusted_net_units_sold'].sum().reset_index()
    product_group = product_group.rename(
        columns={'adjusted_net_units_sold': 'total_sales_by_product_by_country_by_week'})
    ready_data = ready_data.merge(product_group, how='left', on=['id_product', 'warehouse', 'week_id'])
    ready_data['size_dist'] = np.round(
        ready_data['adjusted_net_units_sold'] / ready_data['total_sales_by_product_by_country_by_week'], 3)

    del ready_data['total_sales_by_product_by_country_by_week']
    ready_data['size_dist'] = ready_data['size_dist'].fillna(0)
    ready_data['size_dist'] = np.nan_to_num(ready_data['size_dist'].astype(np.float32))

    # pv, mkt spend, lookbook spend
    ready_data['dist_pv'] = np.round(ready_data['pv'] * ready_data['size_dist'])
    ready_data['dist_pv'] = np.nan_to_num(ready_data['dist_pv'].astype(np.float32))

    #     ready_data['dist_marketing_collection'] = np.round(ready_data['marketing_spend']*ready_data['size_dist'])
    ready_data['dist_marketing_collection'] = np.round(ready_data['marketing_spend'] * ready_data['mkt_pv_dist'])
    ready_data['dist_marketing_collection'] = np.nan_to_num(ready_data['dist_marketing_collection'].astype(np.float32))

    #     ready_data['dist_lookbook'] = np.round(ready_data['lookbook_spend_all']*ready_data['size_dist'])
    ready_data['dist_lookbook'] = np.round(ready_data['lookbook_spend_all'])
    ready_data['dist_lookbook'] = np.nan_to_num(ready_data['lookbook_spend_all'].astype(np.float32))

    # Clean data types
    ready_data.week_id = ready_data.week_id.map(lambda x: x.lstrip('week')).astype(int)
    ready_data.day_number = ready_data.day_number.astype(int)
    ready_data.year = ready_data.year.astype(int)

    #ready_data = ready_data.fillna(0).reset_index(drop = True)
    print('done')
    
    return ready_data[ready_data['adjusted_net_units_sold'] >= 0]