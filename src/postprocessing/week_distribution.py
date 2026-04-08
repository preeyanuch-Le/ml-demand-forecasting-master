import pandas as pd
import numpy as np


def get_historic_week_dist(master_join: pd.DataFrame, year: int) -> pd.DataFrame:
    """ Returns dataframe that carries week distirbution by cat1, cluster, spl and country """
    
    tmp = master_join[master_join.first_available_year == year]
    total_week_sales = tmp.groupby(['henry_category_1', 'sub_product_line', 'cluster', 'id_shop_name']).adjusted_net_units_sold.sum().reset_index()
    total_week_sales.rename(columns = {'adjusted_net_units_sold': 'total_sales'}, inplace = True)

    week_dist_data = tmp.groupby(['henry_category_1', 'sub_product_line', 'cluster', 'id_shop_name', 'week_id']).adjusted_net_units_sold.sum().reset_index()
    week_dist_data = pd.merge(
        week_dist_data,
        total_week_sales,
        on = ['henry_category_1', 'sub_product_line', 'cluster', 'id_shop_name'],
        how = 'left')

    week_dist_data['week_dist'] = week_dist_data.adjusted_net_units_sold / week_dist_data.total_sales    
    return week_dist_data

def adjust_week_distribution(final_data: pd.DataFrame) -> pd.DataFrame:
    """ Adjust forecast following the added week_dist column """
    
    tmp = final_data.copy()
    # Get total forecast volume by style 
    total_week_sales = tmp.groupby(
        ['master_style_id', 'color', 'size', 'id_shop_name','henry_category_1','store_grading', 'sub_product_line']
    ).forecast.sum().reset_index()
    total_week_sales.rename(columns = {'forecast': 'total_forecast'}, inplace = True)
            
    tmp = pd.merge(
        tmp,
        total_week_sales,
        on = ['master_style_id', 'color', 'size', 'id_shop_name','henry_category_1','store_grading', 'sub_product_line'],
        how = 'left'
    )
    
    # Get sum of week dist (in edge cases >1) to not inflate forecasts
    total_week_dist_ = tmp.groupby(
        ['master_style_id', 'color', 'size', 'id_shop_name','henry_category_1','store_grading', 'sub_product_line']
    ).week_dist.sum().reset_index()
    total_week_dist_.rename(columns = {'week_dist' : 'total_week_dist'}, inplace = True)
    tmp = pd.merge(
        tmp,
        total_week_dist_[['master_style_id', 'color', 'size', 'id_shop_name','henry_category_1','store_grading', 'sub_product_line', 'total_week_dist']],
        on = ['master_style_id', 'color', 'size', 'id_shop_name','henry_category_1','store_grading', 'sub_product_line'],
        how = 'left'
    )
    tmp['week_dist'] = tmp['week_dist'] / tmp['total_week_dist']
    
    
    #adjust forecast based on week_dist * total_forecast
    tmp['forecast_new'] = tmp.total_forecast * tmp.week_dist
    tmp['forecast_new'] = np.round(tmp['forecast_new'],2)

    tmp.drop(columns = ['forecast','total_week_dist'], inplace = True)
    tmp.rename(columns = {'forecast_new': 'forecast'}, inplace = True)
    
    return tmp