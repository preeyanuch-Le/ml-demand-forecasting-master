''' Contains several function that are needed to clean collection files for online deplyment '''

import io
import os

import pickle
from datetime import datetime

import numpy as np
import pandas as pd

from os import listdir
from os.path import isfile, join



def clean_collection_input(df: pd.DataFrame) -> pd.DataFrame:
    ''' Cleanes the input file from excel templates. Adds one row per size, warehouse 
    and adds id_product_attribute column.
    
    Args:
        df: output from prep_collection_input script
    returns:
        cleaned dataframe
    '''
    # clean columns
    df["id_product"] = df["id_product"].astype(float).astype(int)
    df["master_style_id"] = df["master_style_id"].apply(
        lambda x: int(x) if x == x else ""
    )

    df["date_released"] = pd.to_datetime(df["date_released"])
    df["day_number"] = df["date_released"].dt.day
    df["year"] = df["date_released"].dt.year
    df["week"] = df["date_released"].dt.isocalendar().week
    
    df["day_of_week"] = df["date_released"].dt.day_name()
    df["released_month"] = df["date_released"].dt.month_name()
    df["first_week_of_month"] = np.where(df.day_number < 8, 1, 0)
    df["last_week_of_month"] = np.where(df.day_number > 21, 1, 0)
    df["giveaway"] = np.where(df.giveaways_yes_or_no == "yes", 1, 0)
    df["id_product"] = df["id_product"].astype(float).astype(int)
    df["released_month"] = df["released_month"].str.strip()

    
    # add sizes
    df['size_range'] = df['size_range'].str.upper()
    df['size'] = df['size_range'].str.split(',')
    trial_df = df.explode('size')
    trial_df['size'] = trial_df['size'].str.replace('/','') # slash sizes
    trial_df['size'] = trial_df['size'].str.strip()
    
    # add warehouse
    trial_df['warehouse'] = "TH,MY,ID"
    trial_df['warehouse'] = trial_df['warehouse'].str.split(',')
    trial_df = trial_df.explode('warehouse')
    
    # add id_product_attribute
    trial_df['cnt'] = np.arange(len(trial_df))
    trial_df['rank'] = trial_df.groupby(['id_product', 'color', 'warehouse', 'size'])['cnt'].rank("dense", ascending=False)
    trial_df['id_product_attribute'] = (trial_df['id_product']*10 + trial_df['rank']).astype(int)
    trial_df.drop(columns = ['cnt', 'rank'], inplace = True)
    
    return trial_df


def transform_collection_file(input_file_name, collection_folder_name):
    
    df = pd.read_excel(
        collection_folder_name + "/" + input_file_name,
        sheet_name=0,
        header=2,
        engine="openpyxl"
    )
    
    df = df.dropna(subset=["master_style_id"])

    # clean all row that doesn’t have product information
    df["master_style_id"] = df["master_style_id"].astype(int)
    
    df.rename(columns={
        "new_category_1": "category_1",
        "new_category_2": "category_2",
        "new_category_3": "category_3",
    }, inplace=True)

    
    non_clothing_list = [
                "Bags",
                "Accessories",
                "Bath&Body",
                "Beverage Container",
                "Cosmetics",
                "Hair Care",
                "Miscellaneous",
                "Shoes",
                "Skincare",
                "Stationery"
    ]
    # drop non-clothing product
    if (df["category_1"].isin(non_clothing_list).any()):
        print(f"there are non-clothing products in the {input_file_name} file....")
        print("dropping those non-clothing product as this is Clothing DFM")

    df = df.loc[~df.category_1.isin(non_clothing_list)]

    # rename col Henry Id to id_product
    df.rename(columns={"Henry Id": "id_product"}, inplace=True)

    df["id_product"] = df["id_product"].astype(str)
    # clean all comment in the henry id
    if df["id_product"].str.contains("[a-zA-Z]", regex=True).any():
        print("cleaning comment in the henry id")
        df["id_product"] = np.where(
            df["id_product"].str.contains("[a-zA-Z]", regex=True),
            np.nan,
            df["id_product"],
        )

    # create temp 'id' column
    df["id"] = df.groupby("master_style_id")["master_style_id"].transform(
        lambda x: pd.Series(range(1, len(x) + 1), index=x.index)
    )

    # create id_product column
    df["id_product"] = np.where(
        df["id_product"].isnull(),
        df["master_style_id"].astype(str) + df["id"].astype(str),
        df["id_product"],
    )

    # Delete id column
    del df["id"]

    # clean date release column
    cols = ["day", "month", "year"]
    df.loc[:, cols] = df.loc[:, cols].ffill(axis=0)
    df.rename(columns={"date_released_TH": "date_released"}, inplace=True)
    df["date_released"] = pd.to_datetime(df[["year", "month", "day"]])

    # Rename col ‘total_number_of_styles_in_drop’ to ‘number_of_styles_in_drop’
    df.rename(
        columns={"total_number_of_styles_in_drop": "number_of_styles_in_drop"},
        inplace=True,
    )

    # Check if total style in drop is correct
    # if not then assign to number of row
    # need to make sure that, in the file , there aren't any accessory ( only clothing)
    if df["number_of_styles_in_drop"].iloc[0] == len(df.index):
        pass
    else:
        print(f"number_of_styles_in_drop in {input_file_name} is not correct!!")
        print("currently {}".format(df["number_of_styles_in_drop"].iloc[0]))
        print(f"changing to {len(df.index)}")
        df["number_of_styles_in_drop"] = len(df.index)

    # creating ‘size_range_type’ column baze on size_range column
    # if size_range = xs-xl then 'General'
    # if size_range = 36,38,... then 'Bottom'
    df["size_range_type"] = np.where(
        df.size_range.str.contains(r"\d", regex=True), "bottoms", "general"
    )

    # rename column ‘primary_fabric_composition’ to ‘fabric’
    df.rename(columns={"primary_fabric_composition": "fabric"}, inplace=True)

    # Clean col color and simple_color
    df.color = df.color.str.title()
    df.simple_color = df.simple_color.replace(np.nan, "", regex=True).str.title()
    print(f"{input_file_name} is finished")

    # rearrange column order
    df = df[
        [
            "master_style_id",
            "id_product",
            "Image",
            "color",
            "day",
            "month",
            "year",
            "date_released",
            "buyer's key piece",
            "TH",
            "MY",
            "ID",
            "RP_THB",
            "original_price_usd",
            "product_cost_usd",
            "size_range",
            "size_range_type",
            "number_of_styles_in_drop",
            "released_collection_name",
            "product_line",
            "sub_product_line",
            "category_1",
            "category_2",
            "category_3",
            "simple_color",
            "fabric_type",
            "fabric",
            "pattern",
            "style",
            "shape",
            "rise",
            "neckline",
            "sleeve",
            "sleeve_style",
            "product_lifecycle",
            "studio",
            "giveaways_yes_or_no",
            "Pomelo_total_giveaways",
            "total_giveaways_TH"
        ]
    ]
    return df


# prep collection file and upload to s3 #####
def prep_collection_file(collection_folder_name, collection_file_name):
    ''' 
    Args:
    collection_folder_name: path to save collection files locally
    collection_file_name: name to output file
    
    '''
    
    frames = []

    if not os.path.exists(collection_folder_name):
        os.makedirs(collection_folder_name)
    
    collection_files = [f for f in listdir(collection_folder_name) 
                        if isfile(join(collection_folder_name, f))
                    ]
    
    for f in collection_files:
        #print(collection_folder_name + f)
        full_path = collection_folder_name + "/" + f
        print(full_path)
        df = pd.read_excel(
            full_path,
            engine="openpyxl",
            skiprows=2
        )
        print(df.info())
        df = transform_collection_file(f, collection_folder_name)
        frames.append(df)

    result = pd.concat(frames)
    result.reset_index(drop=True, inplace=True)
    result.to_csv(
        f"s3://hal-bi-bucket/data_science/dfm/excel_files/{collection_file_name}.csv",
        index=False,
    )
    return result