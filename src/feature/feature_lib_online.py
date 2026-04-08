import numpy as np
import pandas as pd

def get_feature_distribution_online(input_df):
    ''' Outputs simple distribution for key store level features. Needs to be ran before resampling to cluster level.
        This function acts on store level and not average cluster sales
        TODO: 
            Technically this functions "cheats" by looking into future feature distributions while training (mean, min, max, std).
            A better way is to extract the last available data that can be used for each product and calculate statistics on a rolling window from that date.
    '''
    
    feature_dist = (
        input_df
        .groupby(["warehouse",'sub_product_line','henry_category_2', "week_id"])
        .agg(
            feature_item_discount_mean = ('item_discount_percent','mean'),
            feature_item_discount_max = ('item_discount_percent','max'),
            feature_item_discount_std = ('item_discount_percent','std'),
            feature_voucher_discount_mean = ('voucher_discount_percent','mean'),
            feature_voucher_discount_max = ('voucher_discount_percent','max'),
            feature_voucher_discount_std = ('voucher_discount_percent','std'),
            feature_discount_utilization_mean = ('discount_utilization','mean'),
            feature_discount_utilization_max = ('discount_utilization','max'),
            feature_discount_utilization_std = ('discount_utilization','std'),
            feature_dist_lookbook_mean = ('dist_lookbook','mean'),
            feature_dist_lookbook_max = ('dist_lookbook','max'),
            feature_dist_lookbook_std = ('dist_lookbook','std')
        ).reset_index()
    )
    return feature_dist


def get_historic_sales_statistics_online(input_df):
    """ Returns feature files for historic sales statistics calculated on different aggregation levels.
        TODO: 
            Same as function get_feature_distribution, the methodology does not follow machine learning best pracitcies and technically cheats.
            It was tested and found to work well without overfitting but should be replace by a solid methogology that looks back only when possible. 
    """
    
    feature_sales_grp1 = input_df.groupby(['henry_category_2', 'size','week_id', 'warehouse']).agg(
        feature_sales_grp1_mean = ('adjusted_net_units_sold','mean'),
        feature_sales_grp1_max = ('adjusted_net_units_sold','max'),
        feature_sales_grp1_std = ('adjusted_net_units_sold','std')
    ).reset_index()
    
    feature_sales_grp2 = input_df.groupby(['henry_category_2', 'size','week_id', 'warehouse', 'sub_product_line']).agg(
        feature_sales_grp2_mean = ('adjusted_net_units_sold','mean'),
        feature_sales_grp2_max = ('adjusted_net_units_sold','max'),
        feature_sales_grp2_std = ('adjusted_net_units_sold','std')
    ).reset_index()
    
    feature_sales_grp3 = input_df.groupby(['henry_category_2', 'size','week_id', 'warehouse', 'sub_product_line', 'simple_color']).agg(
        feature_sales_grp3_mean = ('adjusted_net_units_sold','mean'),
        feature_sales_grp3_max = ('adjusted_net_units_sold','max'),
        feature_sales_grp3_std = ('adjusted_net_units_sold','std')
    ).reset_index()
    
    return feature_sales_grp1, feature_sales_grp2, feature_sales_grp3



def get_size_distribution_online(input_data, sales_column):

    total_size_dist = input_data.groupby(
        ['id_product','color', 'warehouse', 'henry_category_1', 'sub_product_line']
    )[sales_column].sum().reset_index()
    total_size_dist.rename(columns = {sales_column: 'total_volume'}, inplace = True)

    size_dist_data = input_data.groupby(
        ['id_product','color', 'warehouse', 'size', 'henry_category_1' ]
    )[sales_column].sum().reset_index()
    size_dist_data.rename(columns = {sales_column: 'size_volume'}, inplace = True)

    size_dist_data = pd.merge(
        size_dist_data,
        total_size_dist,
        on = ['id_product','color', 'warehouse', 'henry_category_1'],
        how = 'left'
    )
    size_dist_data['size_dist'] = size_dist_data['size_volume'] / size_dist_data['total_volume']

    size_dist_data = size_dist_data.groupby(
        ['henry_category_1', 'warehouse', 'size']
    )['size_dist'].mean().reset_index()
    size_dist_data['size'] = size_dist_data['size'].astype(str)
    
    return size_dist_data

def get_historic_week_dist_feature_online(input_df: pd.DataFrame) -> pd.DataFrame:
    """ Returns dataframe that carries week distirbution by cat1, cluster, spl and country
    
    Args:
        input_df: DataFrame that the week_distribution should be calculated on
    Returns:
        week distribution dataframe
    """
    
    total_week_sales = input_df.groupby(['henry_category_2', 'sub_product_line', 'warehouse']).adjusted_net_units_sold.sum().reset_index()
    total_week_sales.rename(columns = {'adjusted_net_units_sold': 'total_sales'}, inplace = True)

    week_dist_data = input_df.groupby(['henry_category_2', 'sub_product_line', 'warehouse', 'week_id']).adjusted_net_units_sold.sum().reset_index()
    week_dist_data = pd.merge(
        week_dist_data,
        total_week_sales,
        on = ['henry_category_2', 'sub_product_line', 'warehouse'],
        how = 'left')

    week_dist_data['week_dist'] = week_dist_data.adjusted_net_units_sold / week_dist_data.total_sales
    
    return week_dist_data[['henry_category_2', 'sub_product_line', 'warehouse', 'week_id', 'week_dist']]

