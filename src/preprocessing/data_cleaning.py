import argparse
import os
import warnings
import pandas as pd
import numpy as np
import boto3
import pickle
from src.utils.decorators import timeit
from sklearn.exceptions import DataConversionWarning
warnings.filterwarnings(action='ignore', category=DataConversionWarning)

@timeit
def clean_data(df: pd.DataFrame):
    """ clean and standardize feature for both online, offline and for model training and deployment

    note: 
    - columns name can be different as online calculate from released date but offline calculate from first order date at the store
        online -> day_of_week,released_month,year
        offline -> first_available_dow  ,first_available_month ,first_available_year

    - columns can be diffrent too as both model has different features
    """
    
    categorical_cols = [
        'released_month',
        'day_of_week',
        'first_available_month',
        'first_available_dow',
        'product_line',
        'sub_product_line',
        'henry_category_1',
        'henry_category_2',
        'henry_category_3',
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
        'rise',
        'fabric_custom_name'
    ]
    
    for i in categorical_cols:
        try:
            df[i] = df[i].astype(str)
            df[i] = df[i].str.strip()
            df[i] = df[i].str.lower()
            df[i] = df[i].replace(" ", "_", regex=True)
            df[i] = df[i].replace("-", "", regex=True)
            df[i] = df[i].replace("/", "", regex=True)
            df[i] = df[i].replace("__", "_", regex=True)  
        except KeyError:
            continue

    continuous_cols = [
        'id_product',
        'master_style_id',
        'is_mega_campaign_order',
        'giveaway',
        'first_week_of_month',
        'last_week_of_month',
        'is_big_collection',
        'tie',
        'day_number',
        'day',
        'month',
        'year'
    ]
    
    for j in continuous_cols:
        try:
            df[j] = df[j].astype(int)
        except KeyError:
            continue
        
    # Clean data types for specific column
    try:
        df['size'] = df['size'].str.upper()
    except KeyError:
        pass
    
    try:
        df['date_released'] = pd.to_datetime(df['date_released'])
    except KeyError:
        pass
        
    try:
        df['style'].replace(['<na>', 'none'], np.nan, inplace = True, regex=True)
    except KeyError:
        pass
    
    try:
        df['neckline'].replace(['none','<na>'],np.nan, inplace = True, regex=True)
    except KeyError:
        pass
        
    try:
        df['pattern'].replace(['none', 'nopattern', '<na>'], np.nan, inplace = True, regex=True)
    except KeyError:
        pass
        
    try:
        df['rise'].replace(['none','<na>'], np.nan, inplace = True, regex=True)
    except KeyError:
        pass
        
    try:
        df['shape'].replace(['<na>', 'none'], np.nan, inplace = True, regex=True)
    except KeyError:
        pass
    
    try:
        df['sleeve'].replace('none', np.nan, inplace = True, regex=True)
    except KeyError:
        pass
        
    try:
        df['sleevestyle'].replace('none', np.nan, inplace = True, regex=True)
    except KeyError:
        pass
    
    try:
        df['giveaway'] = np.where(df.giveaway > 0, 1, 0)
    except KeyError:
        pass

    try:
        df['hscode_id_fabric_name'].replace(pd.NA, 'No Fabric', inplace = True)
    except:
        pass
   
    try:
        df['fabric_custom_name'].replace('<na>', np.nan, inplace = True)
    except KeyError:
        pass
    
    try:
        df['henry_category_3'].replace(['<na>',''], np.nan, inplace = True, regex=True)
    except KeyError:
        pass
    
    try:
        df['henry_category_2'].replace(['<na>',''], np.nan, inplace = True, regex=True)
    except KeyError:
        pass
        
    return df


def fill_new_categories(raw_merged: pd.DataFrame, cat: str) -> pd.DataFrame:
    ''' Backfills NA in henry_category_1 where an unique mapping exists form old categories 1,2 and 3
    Args:
        raw_merged: dataframe after query and merging from database
        cat: string number for new category to be filled, '1', '2' or '3'
    '''
    # dummy col
    category_to_fill = 'henry_category_'+ cat
    
    raw_merged['old-cat-mapping'] = (raw_merged['old_henry_category_1'] + '-' + 
                                     raw_merged['old_henry_category_2' ]+ '-' + 
                                     raw_merged['old_henry_category_3']
                                    )
    cat_mapping_list = raw_merged[~(raw_merged[category_to_fill].isna())].groupby(['old-cat-mapping'])[category_to_fill].nunique().reset_index()
    unique_mapping = cat_mapping_list[cat_mapping_list[category_to_fill] == 1] # mapping where only one value (excl. NA) in cat 1 exists for
    cat_1_map = dict((
        raw_merged[
            (raw_merged['old-cat-mapping'].isin(unique_mapping['old-cat-mapping'].unique())) 
            & ~(raw_merged[category_to_fill].isna())
        ].groupby('old-cat-mapping')[category_to_fill].agg('first')
    ))
    print(f'Unique categories before mapping: {raw_merged[category_to_fill].unique()}')
    print(f'Number of NA rows before mapping missing categories: {raw_merged[raw_merged[category_to_fill].isna()].shape}')
    
    raw_merged.loc[raw_merged[category_to_fill].isna(),'new_cat'] = (
        raw_merged[raw_merged[category_to_fill].isna()]['old-cat-mapping'].map(cat_1_map).values
    )
    raw_merged.loc[~raw_merged['new_cat'].isna(), category_to_fill] = raw_merged.new_cat
    
    print(f'Unique categories after mapping: {raw_merged[category_to_fill].unique()}')
    print(f'Number of NA rows after mapping missing categories: {raw_merged[raw_merged[category_to_fill].isna()].shape}')
    
    raw_merged.drop(columns = ['old-cat-mapping', 'new_cat' ], axis = 1, inplace = True)
    return raw_merged
