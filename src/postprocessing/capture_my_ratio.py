import pandas as pd
import numpy as np

def cdc_ratio_adjustment_online(online_file,my_target_ratio,th_target_ratio):
    """ 
    capture MY distribution at 15% then redistribution extra qty while keep size dist, week dist the same
    output : online_file with the same column but with adjusted qty
    """
    print('before adjusting...')
    print(online_file.groupby(['warehouse'])['forecast'].sum()/online_file['forecast'].sum())

    online_file_all_cdc = online_file.groupby(['master_style_id','color','size','week_id'])['forecast'].sum().reset_index()
    online_file_all_cdc.rename(columns={'forecast':'total_for_all_cdc'},inplace=True)

    #adjust online forecast
    online_file2 = pd.merge(online_file,online_file_all_cdc,on=['master_style_id','color','size','week_id'],how='left')
    online_file2['adjust_forecast'] = np.select(
        [online_file2['warehouse']=='MY',online_file2['warehouse']=='TH'],
        [online_file2['total_for_all_cdc']*my_target_ratio,online_file2['total_for_all_cdc']*th_target_ratio ])

    # round to 2 digits
    online_file2['adjust_forecast'] = online_file2['adjust_forecast'].round(2)

    #rename
    del online_file2['forecast']
    online_file2.rename(columns={'adjust_forecast':'forecast'},inplace=True)
    del online_file2['total_for_all_cdc']

    print('\n')
    print('after adjusting...')
    print(online_file2.groupby(['warehouse'])['forecast'].sum()/online_file2['forecast'].sum())
    return online_file2


def cdc_ratio_adjustment_offline(offline_file_weekly, offline_file_agg, my_target_ratio, th_target_ratio):
        
    """ 
    obj : capture MY distribution at 15% then redistribution extra qty while keep size dist, week dist, store cluster dist the same
    
    outputs : offline_file_weekly and offline_file_agg with the same column but with adjusted qty
    
    check these columns in the output in understand better :
    total_for_all_cdc =  39.4

    adjust_forecast_total_cdc:
    MY 4 stores -> sum = 5.91 (15%)
    TH 21 stores -> sum = 33.49 (85%)

    forecast = sum all 7 weeks at store level
    forecast_total = forecast * no_store
    total_for_all_cdc = total qty for all cdc
    adjust_forecast_total_cdc = adjusted forcast for each cdc with capturing MY CDC at 15% 
    store_grading_dist is the distribution of qty within CDC -> MY CDC has 4 stores = 'MY-C' (0.17) , 'SG-A' = 0.3,'SG-B' = 0.28, 'SG-C' = 0.25 -> sum within CDC=1
    adjust_forecast_total_cdc_store_grading = store_grading_dist * adjust_forecast_total_cdc
    adjust_forecast is the new forecast at store level (adjust_forecast_total_cdc_store_grading/no_store)
    """
    
    wh_conditions_weekly = [
        offline_file_weekly['id_shop_name']=='TH',
        offline_file_weekly['id_shop_name']=='ID',
        offline_file_weekly['id_shop_name']=='MY',
        offline_file_weekly['id_shop_name']=='SG'
    ]
    wh_conditions_agg = [
        offline_file_agg['id_shop_name']=='TH',
        offline_file_agg['id_shop_name']=='ID',
        offline_file_agg['id_shop_name']=='MY',
        offline_file_agg['id_shop_name']=='SG'
    ]
    wh_choices =  ['TH','ID','MY','MY']
    
    offline_file_weekly['warehouse'] = np.select(wh_conditions_weekly, wh_choices)
    offline_file_agg['warehouse'] = np.select(wh_conditions_agg, wh_choices)
    
    print('before adjusting...')
    print(offline_file_agg.groupby(['warehouse'])['forecast_total'].sum() / offline_file_agg['forecast_total'].sum())

    temp = offline_file_agg.groupby(['warehouse'])['forecast','forecast_total'].sum().reset_index()
    temp['forecast_cdc_dist'] = np.round(temp['forecast']/offline_file_agg['forecast'].sum(),2)
    temp['forecast_total_cdc_dist'] = np.round(temp['forecast_total']/offline_file_agg['forecast_total'].sum(),2)
    display(temp)
    
    #getting total qty for all cdc for each style at size level
    offline_file_all_cdc = offline_file_agg.groupby(['master_style_id','color','size'])['forecast_total'].sum().reset_index()
    offline_file_all_cdc.rename(columns={'forecast_total':'total_for_all_cdc'},inplace=True)
    
    #adjust offline total forecast
    offline_agg_2 = pd.merge(
        offline_file_agg,
        offline_file_all_cdc,
        on=['master_style_id','color','size'],
        how='left'
    )
    
    offline_agg_2['adjust_forecast_total_cdc'] = np.select(
        [offline_agg_2['warehouse']=='MY', offline_agg_2['warehouse']=='TH'],
        [offline_agg_2['total_for_all_cdc'] * my_target_ratio,offline_agg_2['total_for_all_cdc'] * th_target_ratio]
    )
        
    offline_agg_2["store_grading_dist"] = (
        offline_agg_2["forecast_total"] 
        / offline_agg_2.groupby(["master_style_id", "color","size","warehouse"])["forecast_total"].transform("sum")
    )
        
    offline_agg_2['adjust_forecast_total_cdc_store_grading'] = offline_agg_2["adjust_forecast_total_cdc"] * offline_agg_2["store_grading_dist"]

    #get adjusted forcast at store level
    offline_agg_2['adjust_forecast'] = offline_agg_2['adjust_forecast_total_cdc_store_grading']/offline_agg_2['no_store']
    
    print('after adjusting...')
    print(offline_agg_2.groupby(['warehouse'])['adjust_forecast_total_cdc_store_grading'].sum() / offline_agg_2['adjust_forecast_total_cdc_store_grading'].sum())

    temp2 = offline_agg_2.groupby(['warehouse'])['adjust_forecast','adjust_forecast_total_cdc_store_grading'].sum().reset_index()
    temp2['forecast_cdc_dist'] = np.round(temp2['adjust_forecast'] / offline_agg_2['adjust_forecast'].sum(),2)
    temp2['forecast_total_cdc_dist'] = np.round(temp2['adjust_forecast_total_cdc_store_grading']/offline_agg_2['adjust_forecast_total_cdc_store_grading'].sum(),2)
    display(temp2)
    
    print("onverting new adjusted to weekly level...")
    # get week distribution
    offline_file_weekly["week_dist"] = offline_file_weekly["forecast"] / offline_file_weekly.groupby(
        ["master_style_id", "color","size",'store_grading','id_shop_name']
    )["forecast"].transform("sum")
    
    
    cols1=[
        'master_style_id', 'id_product', 'color', 'size', 'store_grading','sub_product_line', 'id_shop_name',
        'released_collection_name','warehouse','week_id','week_dist'
    ]

    cols2 = [
        'master_style_id', 'color', 'size', 'store_grading', 'sub_product_line', 'id_shop_name',
        'released_collection_name', 'warehouse', 'adjust_forecast'
    ]
    
    # Fill na in week_dist
    fill_week_dist = offline_file_weekly.groupby('week_id').week_dist.mean().reset_index()
    fill_week_dist['week_dist'] = fill_week_dist['week_dist'] / fill_week_dist['week_dist'].sum() # make sum to 1
    for week in [1,2,3,4,5,6,7]:
        offline_file_weekly.loc[
            offline_file_weekly.week_dist.isna() & (offline_file_weekly.week_id == week),
            'week_dist'
        ] = offline_file_weekly[offline_file_weekly.week_id == week].week_dist.values[0]

        
    final_offline_weekly = pd.merge(
        offline_file_weekly[cols1],
        offline_agg_2[cols2],how='left',
        on=['master_style_id','color','size','released_collection_name','store_grading','sub_product_line','id_shop_name','warehouse']
    )
    final_offline_weekly['forecast'] = np.round(final_offline_weekly['adjust_forecast']*final_offline_weekly['week_dist'],2)
    del final_offline_weekly['adjust_forecast'] 
    del final_offline_weekly['week_dist'] 
    
    print('total before adjusting dist:')
    print(offline_agg_2['adjust_forecast'].sum())

    print('total after adjusting weekly dist : ')
    print(np.round(final_offline_weekly['forecast'].sum(),2))
    
    return offline_agg_2,final_offline_weekly



