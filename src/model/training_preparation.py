''' Set of functions used for training only '''

import pandas as pd
import numpy as np

def get_undersampling_index(y: pd.Series) -> list:
    ''' Returns list of indices with undersampled majority class 
    Args:
        y: pd:Series, 1-dim target vector with binary values 
    Returns:
        list of indices to obtain 50/50 ratio of majority/minority classes
    
    '''
    zeros = y[y == 0]
    non_zeros = y[y > 0]
    
    zeros_idx = zeros.sample(non_zeros.shape[0]).index 
    non_zeros_idx = non_zeros.index
    
    return list(zeros_idx) + list(non_zeros_idx)

def resample_to_cluster_level(input_df):
    """ Resamples dataframe to average cluster-id_shop_nam level containing average sales per group """
    grp_cols = [
        'id_product',
        'id_product_attribute',
        'product_cost_usd',
        'original_price_usd',
        'size',
        'product_line',
        'sub_product_line',
        'henry_category_1',
        'henry_category_2',
        'henry_category_3',
        'simple_color',
        'color',
        'fabric_custom_name',
        'hscode_id_fabric_name',
        'giveaway',
        'cluster',
        'pattern',
        'sleevestyle',
        'neckline',
        'shape',
        'rise',
        'sleeve',
        'style', 
        'release_collection_name',
        'week_id',
        'id_shop_name',
        'date_released'
    ]

    input_df = input_df.groupby(grp_cols, dropna = False).agg(
        adjusted_net_units_sold = ('adjusted_net_units_sold','mean'),
        first_available_month = ('first_available_month', min),
        first_available_dow = ('first_available_dow', min),
        first_available_year = ('first_available_year', min),
        first_week_of_month = ('first_week_of_month', min),
        last_week_of_month = ('last_week_of_month',min),
        is_mega_campaign_order = ('is_mega_campaign_order', max)
    ).reset_index()
    
    return input_df
