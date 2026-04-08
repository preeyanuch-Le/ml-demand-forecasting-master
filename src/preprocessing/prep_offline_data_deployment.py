from datetime import date, datetime, timedelta
import re

import pandas as pd
import numpy as np

from src.feature.predict_du_deploy_offline import predict_du_deploy
from src.io.loaders import load_external_data
from src.utils.decorators import timeit



def clean(description):
    return re.sub(r"[^\w\s]", "", description).lower()    


@timeit
def prepare_input_file_offline(input_file):
    """ Cleans collection data. Creates rows for sizes, store_clusters, id_shop_names and id_product_attribute columns """
    df = input_file.copy()

    df["size_range"] = df["size_range"].str.strip()
    df["id_product"] = df["id_product"].astype(int)
    df["master_style_id"] = df["master_style_id"].astype(int)
    df["day"] = df["day"].astype(int)
    df["month"] = df["month"].astype(int)
    df["year"] = df["year"].astype(int)
    df["date_released"] = pd.to_datetime(df["date_released"])
    df["day_number"] = df["date_released"].dt.day
    df["year"] = df["date_released"].dt.year
    df["week"] = df["date_released"].dt.isocalendar().week
    df["day_of_week"] = df["date_released"].dt.day_name()
    df["released_month"] = df["date_released"].dt.month_name()
    df["first_week_of_month"] = np.where(df.day_number < 8, 1, 0)
    df["last_week_of_month"] = np.where(df.day_number > 21, 1, 0)
    df["giveaway"] = np.where(df.giveaways_yes_or_no == "yes", 1, 0)
    df["covid_lockdown"] = 0
    
    # add sizes
    df['size_range'] = df['size_range'].str.upper()
    df['size'] = df['size_range'].str.split(',')
    trial_df = df.explode('size')
    trial_df['size'] = trial_df['size'].str.replace('/','') # slash sizes
    trial_df['size'] = trial_df['size'].str.strip()
    
    # add id_shop_name
    trial_df['id_shop_name'] = "TH,MY,ID,SG"
    trial_df['id_shop_name'] = trial_df['id_shop_name'].str.split(',')
    trial_df = trial_df.explode('id_shop_name')
    
    # add store_cluster
    trial_df["store_cluster"] = "A,B,C,D"
    trial_df["store_cluster"] = trial_df["store_cluster"].str.split(',')
    trial_df = trial_df.explode("store_cluster")
    
    # add id_product_attribute
    trial_df['cnt'] = np.arange(len(trial_df))
    trial_df['rank'] = trial_df.groupby(
        ['id_product', 'color', 'id_shop_name', 'store_cluster', 'size']
    )['cnt'].rank("dense", ascending=False)
    trial_df['id_product_attribute'] = (trial_df['id_product']*10 + trial_df['rank']).astype(int)
    trial_df.drop(columns = ['cnt', 'rank'], inplace = True)

    # find weekly start date and week_id
    temp1 = trial_df[["id_product_attribute"]]
    temp1.loc[:, "week1"] = 0
    temp1.loc[:, "week2"] = 7
    temp1.loc[:, "week3"] = 14
    temp1.loc[:, "week4"] = 21
    temp1.loc[:, "week5"] = 28
    temp1.loc[:, "week6"] = 35
    temp1.loc[:, "week7"] = 42

    df_unpivoted = temp1.melt(
        id_vars=["id_product_attribute"], var_name="week_id", value_name="day_no"
    )

    df_merge = pd.merge(trial_df, df_unpivoted, on="id_product_attribute", how="left")
    df_merge = df_merge.drop_duplicates()


    trial_df = df_merge.copy()
    trial_df["start_date_weekly"] = pd.DatetimeIndex(trial_df["date_released"]) + trial_df[
        "day_no"
    ].apply(pd.offsets.Day)
    trial_df = trial_df.reset_index(drop=True)

    trial_df["week_id"] = trial_df["week_id"].astype(str)
    return trial_df


    
@timeit    
def add_discount(trial_df):
    df_final2 = predict_du_deploy(trial_df)
    df_final3 = df_final2[
        [
            "id_product_attribute",
            "id_shop_name",
            "store_cluster",
            "week_id",
            "voucher_du",
            "item_du",
            "total_du",
        ]
    ]
    df_final3.rename(
        columns={
            "voucher_du": "voucher_discount_percent",
            "item_du": "item_discount_percent",
            "total_du": "discount_utilization",
        },
        inplace=True,
    )
    df_final3["week_id"] = "week" + df_final3["week_id"].astype(str)


    df_final3 = df_final2[
        [
            "id_product_attribute",
            "id_shop_name",
            "store_cluster",
            "week_id",
            "voucher_du",
            "item_du",
            "total_du",
        ]
    ]
    df_final3.rename(
        columns={
            "voucher_du": "voucher_discount_percent",
            "item_du": "item_discount_percent",
            "total_du": "discount_utilization",
        },
        inplace=True,
    )
    df_final3["week_id"] = "week" + df_final3["week_id"].astype(str)


    # if discount == 0 then use avg
    temp = df_final3.groupby(["week_id"])["voucher_discount_percent"].mean().reset_index()

    temp["voucher_discount_percent"] = np.where(
        temp["voucher_discount_percent"] > 0.1,
        temp["voucher_discount_percent"] * 0.05,
        temp["voucher_discount_percent"],
    )

    df_final3["voucher_discount_percent"] = np.where(
        (df_final3["voucher_discount_percent"] == 0)
        & (df_final3["id_shop_name"] == "ID")
        & (df_final3["week_id"] == "week5"),
        temp[temp["week_id"] == "week5"]["voucher_discount_percent"].item(),
        df_final3["voucher_discount_percent"],
    )

    df_final3["voucher_discount_percent"] = np.where(
        (df_final3["voucher_discount_percent"] == 0)
        & (df_final3["id_shop_name"] == "ID")
        & (df_final3["week_id"] == "week6"),
        temp[temp["week_id"] == "week6"]["voucher_discount_percent"].item(),
        df_final3["voucher_discount_percent"],
    )

    df_final3["voucher_discount_percent"] = np.where(
        (df_final3["voucher_discount_percent"] == 0)
        & (df_final3["id_shop_name"] == "ID")
        & (df_final3["week_id"] == "week7"),
        temp[temp["week_id"] == "week7"]["voucher_discount_percent"].item(),
        df_final3["voucher_discount_percent"],
    )

    df_final3["discount_utilization"] = np.where(
        (df_final3["voucher_discount_percent"] == 0)
        & (df_final3["id_shop_name"] == "ID")
        & (df_final3["week_id"] == "week5"),
        temp[temp["week_id"] == "week5"]["voucher_discount_percent"].item(),
        df_final3["voucher_discount_percent"],
    )

    df_final3["discount_utilization"] = np.where(
        (df_final3["voucher_discount_percent"] == 0)
        & (df_final3["id_shop_name"] == "ID")
        & (df_final3["week_id"] == "week6"),
        temp[temp["week_id"] == "week6"]["voucher_discount_percent"].item(),
        df_final3["voucher_discount_percent"],
    )

    df_final3["discount_utilization"] = np.where(
        (df_final3["voucher_discount_percent"] == 0)
        & (df_final3["id_shop_name"] == "ID")
        & (df_final3["week_id"] == "week7"),
        temp[temp["week_id"] == "week7"]["voucher_discount_percent"].item(),
        df_final3["voucher_discount_percent"],
    )


    # df_final3['voucher_discount_percent'] = np.where((df_final3['voucher_discount_percent']>0.1)&(df_final3['id_shop_name']=='MY'),
    #                      df_final3['voucher_discount_percent']*0.05,
    #                      df_final3['voucher_discount_percent'])

    # df_final3['discount_utilization'] = np.where((df_final3['discount_utilization']>0.1)&(df_final3['id_shop_name']=='MY'),
    #                      df_final3['discount_utilization']*0.05,
    #                      df_final3['discount_utilization'])

    # merge with discount (DUS)
    trial_df = trial_df.merge(
        df_final3,
        how="left",
        on=["id_product_attribute", "id_shop_name", "store_cluster", "week_id"],
    )
    print("discount na :")
    print(len(trial_df[trial_df["discount_utilization"].isna()]))
    print(len(trial_df[trial_df["item_discount_percent"].isna()]))
    print(len(trial_df[trial_df["voucher_discount_percent"].isna()]))

    # first_available_dow, first_available_month, first_available_year
    trial_df.rename(
        columns={
            "day_of_week": "first_available_dow",
            "released_month": "first_available_month",
            "year": "first_available_year",
        },
        inplace=True,
    )
    trial_df["start_date_weekly"] = pd.to_datetime(trial_df["start_date_weekly"])
    trial_df["month"] = trial_df["start_date_weekly"].dt.month_name()
    trial_df["year"] = trial_df["start_date_weekly"].dt.year
    
    return trial_df


""" TO-DO : once decided to use this fn : -> need to check store cluster"""
@timeit
def clean_and_merge_traffic_data(traffic_df, trial_df):
    del traffic_df["month_year"]
    del traffic_df["year"]
    traffic_df = traffic_df.dropna(subset=["store_cluster"])
    traffic_df["fullyear"] = traffic_df["fullyear"].astype(int)
    traffic_df.rename(columns={"fullyear": "year"}, inplace=True)
    traffic_df.rename(columns={"country": "id_shop_name"}, inplace=True)
    traffic_df.rename(columns={"month": "first_available_month"}, inplace=True)
    traffic_df = (
        traffic_df.groupby(["id_shop_name", "store_cluster", "year", "first_available_month"])["traffic"]
        .sum()
        .reset_index()
    )
    trial_df = trial_df.merge(
        traffic_df, how="left", on=["year", "first_available_month", "store_cluster", "id_shop_name"]
    )

    trial_df = trial_df[~trial_df["traffic"].isna()]  # drop imposstible store
    return trial_df
    
""" TO-DO : once decided to use this fn : -> need to check store cluster"""    
@timeit    
def add_traffic_distribution(trial_df, traffic_dist_df ,week_in_month, STORE_DIST_FILE_PATH):
    week_in_month["full_date"] = pd.to_datetime(week_in_month["full_date"])
    trial_df["start_date_weekly"] = pd.to_datetime(trial_df["start_date_weekly"])
    week_in_month.rename(columns={"full_date": "start_date_weekly"}, inplace=True)
    trial_df = trial_df.merge(
        week_in_month[["start_date_weekly", "week_in_month"]],
        how="left",
        on="start_date_weekly",
    )
    traffic_dist_df.rename(
        columns={
            "Month": "first_available_month",
            "Week In Month": "week_in_month",
            "Store Cluster": "store_cluster",
            "% of Total Total Traffic In": "traffic_dist_wim",
        },
        inplace=True,
    )
    trial_df = trial_df.merge(
        traffic_dist_df, how="left", on=["first_available_month", "week_in_month", "store_cluster"]
    )
    trial_df["traffic_dist_2"] = trial_df["traffic"] * trial_df["traffic_dist_wim"]

    # sales prop
    # new product only one week
    sales_prop = pd.read_csv(
       STORE_DIST_FILE_PATH
    )
    sales_prop = (
        sales_prop.groupby(["oldcluster", "country"])[
            "avg_prop_new_pd", "avg_prop_existing_pd"
        ]
        .mean()
        .reset_index()
    )
    sales_prop.rename(
        columns={"oldcluster": "store_cluster", "country": "id_shop_name"}, inplace=True
    )
    trial_df = trial_df.merge(sales_prop, how="left", on=["store_cluster", "id_shop_name"])

    # traffic dist
    trial_df["traffic_dist"] = np.where(
        trial_df["week_id"] == "week1",
        trial_df["traffic_dist_2"] * trial_df["avg_prop_new_pd"],
        trial_df["traffic_dist_2"] * trial_df["avg_prop_existing_pd"],
    )

    return trial_df

""" TO-DO : once decided to use this fn : -> need to check store cluster"""
@timeit
def clean_and_merge_retail_data(retail_mkt_df, trial_df):
    retail_mkt_df = retail_mkt_df.reset_index(drop=True)
    retail_mkt_df = retail_mkt_df.dropna()
    retail_mkt_df["month"] = pd.to_datetime(retail_mkt_df["month"])
    retail_mkt_df["month_name"] = retail_mkt_df["month"].dt.month_name()
    del retail_mkt_df["month"]
    retail_mkt_df.rename(columns={"month_name": "first_available_month"}, inplace=True)
    retail_mkt_df.rename(columns={"VM budget (USD)": "retail_mkt_spend"}, inplace=True)
    retail_mkt_df.rename(columns={"country": "id_shop_name"}, inplace=True)
    trial_df = trial_df.merge(
        retail_mkt_df[["first_available_month", "id_shop_name", "retail_mkt_spend"]],
        how="left",
        on=["first_available_month", "id_shop_name"],
    )
    trial_df[
        trial_df["retail_mkt_spend"].isna()
    ]  # drop imposstible store like MY and sc1 -> doesn't exits
    trial_df["retail_mkt_spend"] = trial_df["retail_mkt_spend"] / 4
    trial_df["retail_mkt_spend_dist"] = np.where(
        trial_df["week_id"] == "week1",
        trial_df["retail_mkt_spend"] * trial_df["avg_prop_new_pd"],
        trial_df["retail_mkt_spend"] * trial_df["avg_prop_existing_pd"],
    )
    # use avg inout
    del trial_df["traffic_dist"]
    del trial_df["retail_mkt_spend_dist"]
    
    return trial_df
    
    
    
@timeit   
def prep_model_format(mid_list,FEATURES_OFFLINE):

    full_list = mid_list.drop(["master_style_id", "released_collection_name"], axis=1)

    full_list = full_list.replace(np.nan, "", regex=True)
    ready_data = full_list.copy()
    ready_data = ready_data.reset_index(drop=1)
    ready_data["covid"] = np.where(ready_data["covid_cases"] > 0, 1, 0)

    new_columns = ready_data.columns
    old_columns = FEATURES_OFFLINE

    model_format = pd.DataFrame(columns=old_columns)

    for i in range(0, len(old_columns)):
        for j in range(0, len(new_columns)):
            if new_columns[j] == old_columns[i]:
                model_format[old_columns[i]] = ready_data[new_columns[j]]
                
                
    #temp before fizing DU stategy
    #use avg discount to fill na(O)
    model_format['item_discount_percent'] = np.where(
    (model_format['week_id'].isin(['week5','week6','week7'])
     & (model_format['item_discount_percent']==0))
        ,0.15,model_format['item_discount_percent'])
    
    model_format['voucher_discount_percent'] = np.where(
    (model_format['week_id'].isin(['week5','week6','week7'])
     & (model_format['voucher_discount_percent']==0))
        ,0.17,model_format['voucher_discount_percent'])
    model_format['discount_utilization'] = model_format['item_discount_percent']+model_format['voucher_discount_percent']
    
    display(model_format.groupby('id_shop_name')['item_discount_percent','voucher_discount_percent',
                                     'discount_utilization'].mean())
    
    model_format['traffic_dist'] = model_format['traffic_dist'].replace('',np.nan)
    model_format['traffic_dist'] = model_format['traffic_dist'].astype(float)
    
    model_format['retail_mkt_spend_dist'] = model_format['retail_mkt_spend_dist'].replace('',np.nan)
    model_format['retail_mkt_spend_dist'] = model_format['retail_mkt_spend_dist'].astype(float)

    return model_format, mid_list

