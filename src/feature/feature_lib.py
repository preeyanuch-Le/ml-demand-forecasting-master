import sys
import pandas as pd
import numpy as np
from datetime import datetime

# set path to find local modules
sys.path.append("/home/ec2-user/SageMaker/ml-demand-forecasting")
from src.feature.predict_du import predict_du_deploy


def add_collection_features(df: pd.DataFrame, ms_lookbook: list):
    """ Feature function to add is_lookbook_collection and is_big_collection. 
    Args:
        df: dataframe from excel sheet after cleaning (src.preprocessing.depoyment_online.prep_collection_input)
        ms_lookbook_list: List of master style ids that are large lookbook collections
    
    """
    
    col_size = 'l' # collection size if master styles are lookbook collections

    df["is_lookbook_collection"] = np.where(df["master_style_id"].isin(ms_lookbook), 1, 0)
    df["collection_sizes"] = np.where(df["master_style_id"].isin(ms_lookbook), col_size, "normal_drop")
    df["is_big_collection"] = np.select(
        [
            df["collection_sizes"] == "s",
            df["collection_sizes"] == "m",
            df["collection_sizes"] == "l",
            df["collection_sizes"] == "normal_drop",
        ],
        [0, 0, 1, 0],
    )
    
    lookbook_conditions = [
        df["collection_sizes"] == "s",
        df["collection_sizes"] == "m",
        df["collection_sizes"] == "l",
        df["collection_sizes"] == "normal_drop"
    ]
    lookbook_choices = [5000, 10000, 25000, 1142]
    df["lookbook"] = np.select(lookbook_conditions,lookbook_choices)
    
    return df


def add_week_id(trial_df: pd.DataFrame) -> pd.DataFrame:
    """ Adds columns week_id, start_date_weekly, year_weekly, month_weekly """
    
    # prepare data to join with marketing spend on start date of the week (year and month)
    temp1 = trial_df[["id_product_attribute"]]
    temp1.loc[:, "week1"] = 0
    temp1.loc[:, "week2"] = 7
    temp1.loc[:, "week3"] = 14
    temp1.loc[:, "week4"] = 21
    temp1.loc[:, "week5"] = 28
    temp1.loc[:, "week6"] = 35
    temp1.loc[:, "week7"] = 42
    temp1.loc[:, "week8"] = 49
    temp1.loc[:, "week9"] = 56
    temp1.loc[:, "week10"] = 63
    temp1.loc[:, "week11"] = 70
    temp1.loc[:, "week12"] = 77
    temp1.loc[:, "week13"] = 84
    temp1.loc[:, "week14"] = 91
    temp1.loc[:, "week15"] = 98
    temp1.loc[:, "week16"] = 105
    temp1.loc[:, "week17"] = 112
    # id_product_attribute|week1|week2|....|week17
    #        xxx          | 0   | 7   |....| 112

    df_unpivoted = temp1.melt(
        id_vars=["id_product_attribute"], var_name="week_id", value_name="day_no"
    )
    df_merge = pd.merge(trial_df, df_unpivoted, on="id_product_attribute", how="left")
    df_merge = df_merge.drop_duplicates()  # add week_id and day_no columns    
    return df_merge



def add_start_date_weekly(trial_df):
    ''' Helper columns for future functions '''
    
    trial_df["start_date_weekly"] = pd.DatetimeIndex(trial_df["date_released"]) + trial_df["day_no"].apply(pd.offsets.Day)

    trial_df["start_date_weekly"] = pd.to_datetime(trial_df["start_date_weekly"])
    trial_df["year_weekly"] = trial_df["start_date_weekly"].dt.year
    trial_df["month_weekly"] = trial_df["start_date_weekly"].dt.month_name()
    
    return trial_df


def add_external_data_features(
    trial_df: pd.DataFrame,
    lookbook_pv_dist_file: str,
    country_dist_file: str,
    size_dist_file: str,
    size_dist_median_file: str
) -> pd.DataFrame():
    """ Add features for lookbook distribution (dist_lookbook) and size distribution (size_dist) """
    
    # add info data and transform
    # new lookbook pv dist
    lookbook_pv_dist = pd.read_csv(lookbook_pv_dist_file)
    lookbook_pv_dist = lookbook_pv_dist[["warehouse", "week_id", "lookbook_pv_dist"]]
    trial_df = trial_df.merge(lookbook_pv_dist, on=["warehouse", "week_id"], how="left")

    # country dist
    country_dist = pd.read_csv(country_dist_file)
    trial_df = trial_df.merge(
        country_dist[["warehouse", "country_dist"]], on=["warehouse"], how="left"
    )

    # size dist
    size_dist = pd.read_csv(size_dist_file)
    size_dist_med = pd.read_csv(size_dist_median_file)

    size_dist_med.rename(columns={"size_dist": "size_dist_med"}, inplace=True)
    trial_df = trial_df.merge(
        size_dist, on=["size", "warehouse", "week_id"], how="left"
    )
    trial_df = trial_df.merge(
        size_dist_med, on=["size", "warehouse", "week_id"], how="left"
    )

    # adjust the percent of mean and median adcording to % of 0 in training data
    trial_df["size_dist_adjusted"] = np.select(
        [
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "S"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "M"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "XS"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "L"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "XL"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "XXL"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "28"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "26"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "25"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "30"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "32"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "27"),
            (trial_df["warehouse"] == "TH") & (trial_df["size"] == "34"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "S"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "M"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "XS"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "L"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "XL"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "XXL"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "28"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "26"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "25"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "30"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "32"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "27"),
            (trial_df["warehouse"] == "MY") & (trial_df["size"] == "34"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "S"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "M"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "XS"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "L"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "XL"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "XXL"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "28"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "26"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "25"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "30"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "32"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "27"),
            (trial_df["warehouse"] == "ID") & (trial_df["size"] == "34"),
        ],
        [
            (trial_df["size_dist"] * 0.35 + trial_df["size_dist_med"] * 0.65),  # S
            (trial_df["size_dist"] * 0.35 + trial_df["size_dist_med"] * 0.65),  # M
            (trial_df["size_dist"] * 0.5 + trial_df["size_dist_med"] * 0.45),  # XS
            (trial_df["size_dist"] * 0.75 + trial_df["size_dist_med"] * 0.35),  # L
            (trial_df["size_dist"] * 1.0 + trial_df["size_dist_med"] * 0.1),  # XL
            (trial_df["size_dist"] * 0.95 + trial_df["size_dist_med"] * 0.1),  # XXL
            (trial_df["size_dist"] * 0.35 + trial_df["size_dist_med"] * 0.6),  # 28
            (trial_df["size_dist"] * 0.2 + trial_df["size_dist_med"] * 0.8),  # 26
            (trial_df["size_dist"] * 0.4 + trial_df["size_dist_med"] * 0.55),  # 25
            (trial_df["size_dist"] * 1 + trial_df["size_dist_med"] * 0.4),  # 30
            (trial_df["size_dist"] * 1.1 + trial_df["size_dist_med"] * 0.25),  # 32
            (trial_df["size_dist"] * 0.6 + trial_df["size_dist_med"] * 0.5),  # 27
            (trial_df["size_dist"] * 1.35 + trial_df["size_dist_med"] * 0.25),  # 34
            (trial_df["size_dist"] * 0.45 + trial_df["size_dist_med"] * 0.55),
            (trial_df["size_dist"] * 0.5 + trial_df["size_dist_med"] * 0.5),
            (trial_df["size_dist"] * 0.55 + trial_df["size_dist_med"] * 0.45),
            (trial_df["size_dist"] * 0.85 + trial_df["size_dist_med"] * 0.15),
            (trial_df["size_dist"] * 0.9 + trial_df["size_dist_med"] * 0.1),  # XL
            (trial_df["size_dist"] * 0.9 + trial_df["size_dist_med"] * 0.1),  # XXL
            (trial_df["size_dist"] * 0.6 + trial_df["size_dist_med"] * 0.5),  # 28
            (trial_df["size_dist"] * 0.6 + trial_df["size_dist_med"] * 0.5),  # 26
            (trial_df["size_dist"] * 0.55 + trial_df["size_dist_med"] * 0.5),  # 25
            (trial_df["size_dist"] * 0.8 + trial_df["size_dist_med"] * 0.55),  # 30
            (trial_df["size_dist"] * 1 + trial_df["size_dist_med"] * 0.4),  # 32
            (trial_df["size_dist"] * 0.7 + trial_df["size_dist_med"] * 0.4),  # 27
            (trial_df["size_dist"] * 0.8 + trial_df["size_dist_med"] * 0.55),  # 34
            (trial_df["size_dist"] * 0.4 + trial_df["size_dist_med"] * 0.5),
            (trial_df["size_dist"] * 0.4 + trial_df["size_dist_med"] * 0.6),
            (trial_df["size_dist"] * 0.4 + trial_df["size_dist_med"] * 0.6),
            (trial_df["size_dist"] * 0.6 + trial_df["size_dist_med"] * 0.3),  # L
            (trial_df["size_dist"] * 0.75 + trial_df["size_dist_med"] * 0.15),
            (trial_df["size_dist"] * 0.7 + trial_df["size_dist_med"] * 0.2),
            (trial_df["size_dist"] * 0.3 + trial_df["size_dist_med"] * 0.7),  # 28
            (trial_df["size_dist"] * 0.4 + trial_df["size_dist_med"] * 0.6),  # 26
            (trial_df["size_dist"] * 0.3 + trial_df["size_dist_med"] * 0.65),  # 25
            (trial_df["size_dist"] * 0.5 + trial_df["size_dist_med"] * 0.6),  # 30
            (trial_df["size_dist"] * 0.65 + trial_df["size_dist_med"] * 0.5),  # 32
            (trial_df["size_dist"] * 0.45 + trial_df["size_dist_med"] * 0.5),  # 27
            (trial_df["size_dist"] * 0.75 + trial_df["size_dist_med"] * 0.5),  # 34
        ],
        default=trial_df["size_dist"],
    )

    # dist lookbook spend
    trial_df["dist_lookbook"] = np.round(
        trial_df["lookbook"] * trial_df["country_dist"] * trial_df["lookbook_pv_dist"]
    )
    trial_df["dist_lookbook"].fillna(0, inplace=True)
    
    trial_df["size_dist"] = trial_df["size_dist"].replace("",0)
    trial_df["size_dist"] = trial_df["size_dist"].astype("float64")
    
    return trial_df



def add_mega_campaign(trial_df: pd.DataFrame):
    
    # discount
    trial_df["start_date_weekly"] = trial_df["start_date_weekly"].dt.strftime(
        "%Y-%m-%d"
    )
    
    megacampaign_name1 = "0404"
    current_year = "2022"
    start1 = datetime.strptime("2022-04-04", "%Y-%m-%d")
    end1 = datetime.strptime("2022-04-04", "%Y-%m-%d")
    start1_str = datetime.date(start1).strftime("%Y-%m-%d")
    end1_str = datetime.date(end1).strftime("%Y-%m-%d")

    megacampaign_name2 = "0505"
    current_year = "2022"
    start2 = datetime.strptime("2022-05-04", "%Y-%m-%d")
    end2 = datetime.strptime("2022-05-07", "%Y-%m-%d")
    start2_str = datetime.date(start2).strftime("%Y-%m-%d")
    end2_str = datetime.date(end2).strftime("%Y-%m-%d")

    megacampaign_name3 = "0606"
    current_year = "2022"
    start3 = datetime.strptime("2022-06-04", "%Y-%m-%d")
    end3 = datetime.strptime("2022-06-07", "%Y-%m-%d")
    start3_str = datetime.date(start3).strftime("%Y-%m-%d")
    end3_str = datetime.date(end3).strftime("%Y-%m-%d")

    megacampaign_name4 = "0707"
    current_year = "2022"
    start4 = datetime.strptime("2022-07-05", "%Y-%m-%d")
    end4 = datetime.strptime("2022-07-07", "%Y-%m-%d")
    start4_str = datetime.date(start4).strftime("%Y-%m-%d")
    end4_str = datetime.date(end4).strftime("%Y-%m-%d")

    megacampaign_name5 = "0808"
    current_year = "2022"
    start5 = datetime.strptime("2022-08-08", "%Y-%m-%d")
    end5 = datetime.strptime("2022-08-11", "%Y-%m-%d")
    start5_str = datetime.date(start5).strftime("%Y-%m-%d")
    end5_str = datetime.date(end5).strftime("%Y-%m-%d")

    megacampaign_name6 = "pomeloBD"
    current_year = "2022"
    start6 = datetime.strptime("2022-03-25", "%Y-%m-%d")
    end6 = datetime.strptime("2022-03-30", "%Y-%m-%d")
    start6_str = datetime.date(start6).strftime("%Y-%m-%d")
    end6_str = datetime.date(end6).strftime("%Y-%m-%d")
    
    megacampaign_name7 = "0909"
    current_year = "2022"
    start7 = datetime.strptime("2022-09-08", "%Y-%m-%d")
    end7 = datetime.strptime("2022-09-13", "%Y-%m-%d")
    start7_str = datetime.date(start7).strftime("%Y-%m-%d")
    end7_str = datetime.date(end7).strftime("%Y-%m-%d")
    
    megacampaign_name8 = "1010"
    current_year = "2022"
    start8 = datetime.strptime("2022-10-10", "%Y-%m-%d")
    end8 = datetime.strptime("2022-10-10", "%Y-%m-%d")
    start8_str = datetime.date(start8).strftime("%Y-%m-%d")
    end8_str = datetime.date(end8).strftime("%Y-%m-%d")
    
    megacampaign_name9 = "1111"
    current_year = "2022"
    start9 = datetime.strptime("2022-11-10", "%Y-%m-%d")
    end9 = datetime.strptime("2022-11-15", "%Y-%m-%d")
    start9_str = datetime.date(start9).strftime("%Y-%m-%d")
    end9_str = datetime.date(end9).strftime("%Y-%m-%d")
    
    megacampaign_name10 = "1212"
    current_year = "2022"
    start10 = datetime.strptime("2022-12-08", "%Y-%m-%d")
    end10 = datetime.strptime("2022-12-14", "%Y-%m-%d")
    start10_str = datetime.date(start10).strftime("%Y-%m-%d")
    end10_str = datetime.date(end10).strftime("%Y-%m-%d")
    

    megacampaign_name11 = "0404"
    current_year = "2023"
    start12 = datetime.strptime("2023-04-04", "%Y-%m-%d")
    end12 = datetime.strptime("2023-04-04", "%Y-%m-%d")
    start12_str = datetime.date(start12).strftime("%Y-%m-%d")
    end12_str = datetime.date(end12).strftime("%Y-%m-%d")

    megacampaign_name12 = "0505"
    current_year = "2023"
    start13 = datetime.strptime("2023-05-04", "%Y-%m-%d")
    end13 = datetime.strptime("2023-05-07", "%Y-%m-%d")
    start13_str = datetime.date(start13).strftime("%Y-%m-%d")
    end13_str = datetime.date(end13).strftime("%Y-%m-%d")

    megacampaign_name13 = "0606"
    current_year = "2023"
    start14 = datetime.strptime("2023-06-04", "%Y-%m-%d")
    end14 = datetime.strptime("2023-06-07", "%Y-%m-%d")
    start14_str = datetime.date(start14).strftime("%Y-%m-%d")
    end14_str = datetime.date(end14).strftime("%Y-%m-%d")

    megacampaign_name14 = "0707"
    current_year = "2023"
    start15 = datetime.strptime("2023-07-05", "%Y-%m-%d")
    end15 = datetime.strptime("2023-07-07", "%Y-%m-%d")
    start15_str = datetime.date(start15).strftime("%Y-%m-%d")
    end15_str = datetime.date(end15).strftime("%Y-%m-%d")

    megacampaign_name15 = "0808"
    current_year = "2023"
    start16 = datetime.strptime("2023-08-08", "%Y-%m-%d")
    end16 = datetime.strptime("2023-08-11", "%Y-%m-%d")
    start16_str = datetime.date(start16).strftime("%Y-%m-%d")
    end16_str = datetime.date(end16).strftime("%Y-%m-%d")

    megacampaign_name16 = "pomeloBD"
    current_year = "2023"
    start17 = datetime.strptime("2023-03-25", "%Y-%m-%d")
    end17 = datetime.strptime("2023-03-30", "%Y-%m-%d")
    start17_str = datetime.date(start17).strftime("%Y-%m-%d")
    end17_str = datetime.date(end17).strftime("%Y-%m-%d")
    
    megacampaign_name17 = "0909"
    current_year = "2023"
    start18 = datetime.strptime("2023-09-08", "%Y-%m-%d")
    end18 = datetime.strptime("2023-09-13", "%Y-%m-%d")
    start18_str = datetime.date(start18).strftime("%Y-%m-%d")
    end18_str = datetime.date(end18).strftime("%Y-%m-%d")
    
    megacampaign_name18 = "1010"
    current_year = "2023"
    start19 = datetime.strptime("2023-10-10", "%Y-%m-%d")
    end19 = datetime.strptime("2023-10-10", "%Y-%m-%d")
    start19_str = datetime.date(start19).strftime("%Y-%m-%d")
    end19_str = datetime.date(end19).strftime("%Y-%m-%d")
    
    megacampaign_name19 = "1111"
    current_year = "2023"
    start20 = datetime.strptime("2023-11-10", "%Y-%m-%d")
    end20 = datetime.strptime("2023-11-15", "%Y-%m-%d")
    start20_str = datetime.date(start20).strftime("%Y-%m-%d")
    end20_str = datetime.date(end20).strftime("%Y-%m-%d")
    
    megacampaign_name20 = "1212"
    current_year = "2023"
    start21 = datetime.strptime("2023-12-08", "%Y-%m-%d")
    end21 = datetime.strptime("2023-12-14", "%Y-%m-%d")
    start21_str = datetime.date(start21).strftime("%Y-%m-%d")
    end21_str = datetime.date(end21).strftime("%Y-%m-%d")

    rowIndex = trial_df[
        (trial_df["start_date_weekly"] <= end1_str) & (trial_df["start_date_weekly"] >= start1_str)
        | (trial_df["start_date_weekly"] <= end2_str) & (trial_df["start_date_weekly"] >= start2_str)
        | (trial_df["start_date_weekly"] <= end3_str) & (trial_df["start_date_weekly"] >= start3_str)
        | (trial_df["start_date_weekly"] <= end4_str) & (trial_df["start_date_weekly"] >= start4_str)
        | (trial_df["start_date_weekly"] <= end5_str) & (trial_df["start_date_weekly"] >= start5_str)
        | (trial_df["start_date_weekly"] <= end6_str) & (trial_df["start_date_weekly"] >= start6_str)
        | (trial_df["start_date_weekly"] <= end7_str) & (trial_df["start_date_weekly"] >= start7_str)
        | (trial_df["start_date_weekly"] <= end8_str) & (trial_df["start_date_weekly"] >= start8_str)
        | (trial_df["start_date_weekly"] <= end9_str) & (trial_df["start_date_weekly"] >= start9_str)
        | (trial_df["start_date_weekly"] <= end10_str) & (trial_df["start_date_weekly"] >= start10_str)
        | (trial_df["start_date_weekly"] <= end12_str) & (trial_df["start_date_weekly"] >= start12_str)
        | (trial_df["start_date_weekly"] <= end13_str) & (trial_df["start_date_weekly"] >= start13_str)
        | (trial_df["start_date_weekly"] <= end14_str) & (trial_df["start_date_weekly"] >= start14_str)
        | (trial_df["start_date_weekly"] <= end15_str) & (trial_df["start_date_weekly"] >= start15_str)
        | (trial_df["start_date_weekly"] <= end16_str) & (trial_df["start_date_weekly"] >= start16_str)
        | (trial_df["start_date_weekly"] <= end17_str) & (trial_df["start_date_weekly"] >= start17_str)
        | (trial_df["start_date_weekly"] <= end18_str) & (trial_df["start_date_weekly"] >= start18_str)
        | (trial_df["start_date_weekly"] <= end19_str) & (trial_df["start_date_weekly"] >= start19_str)
        | (trial_df["start_date_weekly"] <= end20_str) & (trial_df["start_date_weekly"] >= start20_str)
    ].index
    
    trial_df.loc[rowIndex, "is_mega_campaign_order"] = 1
    trial_df["is_mega_campaign_order"] = trial_df["is_mega_campaign_order"].fillna(0)

    return trial_df


def add_du_features(df: pd.DataFrame) -> pd.DataFrame:
    """ Adds DU features. Function has dependency on  """
    
    trial_df2 = predict_du_deploy(df)
    trial_df2.rename(columns={"total_du": "discount_utilization"}, inplace=True)
    trial_df2.rename(columns={"item_du": "item_discount_percent"}, inplace=True)
    trial_df2.rename(columns={"voucher_du": "voucher_discount_percent"}, inplace=True)
    
    trial_df2.discount_utilization = trial_df2.discount_utilization.replace("", 0.1)
    trial_df2.item_discount_percent = trial_df2.item_discount_percent.replace("", 0)

    return trial_df2


def get_feature_distribution(input_df):
    ''' Outputs simple distribution for key store level features. Needs to be ran before resampling to cluster level.
        This function acts on store level and not average cluster sales
        TODO: 
            Technically this functions "cheats" by looking into future feature distributions while training (mean, min, max, std).
            A better way is to extract the last available data that can be used for each product and calculate statistics on a rolling window from that date.
    '''
    
    feature_dist = (
        input_df
        .groupby(["id_shop_name",'sub_product_line','henry_category_2', "cluster", "week_id"])
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
            feature_traffic_dist_mean = ('traffic_dist','mean'),
            feature_traffic_dist_max = ('traffic_dist','max'),
            feature_traffic_dist_std = ('traffic_dist','std'),
            feature_retail_mkt_spend_dist_mean = ('retail_mkt_spend_dist','mean'),
            feature_retail_mkt_spend_dist_max = ('retail_mkt_spend_dist','max'),
            feature_retail_mkt_spend_dist_std = ('retail_mkt_spend_dist','std')
        ).reset_index()
    )
    
    return feature_dist

def add_price_features(input_df):
    ''' Adds the features regarding the product price
        1. average price per category
        2. price / (average price per category) -> for global cannibalization
        3. price / (average price per category in same release collection) -> collection cannibalization
    '''
    input_df['avg_price_per_category'] = input_df.henry_category_2.map(dict(input_df.groupby('henry_category_2').original_price_usd.mean()))
    input_df['price_ratio_category'] = input_df.original_price_usd / input_df.avg_price_per_category
    
    # price cannibalization
    price_by_collection = input_df.groupby(['release_collection_name','henry_category_2']).original_price_usd.mean().reset_index()
    price_by_collection.rename(columns = {'original_price_usd': 'collection_price_mean'}, inplace = True)

    input_df = pd.merge(
        input_df,
        price_by_collection,
        on = ['release_collection_name', 'henry_category_2']
    )
    
    input_df['feature_price_cannibalization'] = input_df['original_price_usd']  / input_df['collection_price_mean']
    input_df.drop(columns = ['collection_price_mean'], axis = 1, inplace = True )
    return input_df


def get_historic_sales_statistics(input_df):
    """ Returns feature files for historic sales statistics calculated on different aggregation levels.
        TODO: 
            Same as function get_feature_distribution, the methodology does not follow machine learning best pracitcies and technically cheats.
            It was tested and found to work well without overfitting but should be replace by a solid methogology that looks back only when possible. 
    """
    
    feature_sales_category = input_df.groupby(['henry_category_2', 'size', 'color','week_id']).agg(
        feature_sales_cat_mean = ('adjusted_net_units_sold','mean'),
        feature_sales_cat_max = ('adjusted_net_units_sold','max'),
        feature_sales_cat_std = ('adjusted_net_units_sold','std')
    ).reset_index()
    
    feature_sales_category2 = input_df.groupby(['henry_category_2', 'size', 'color','week_id', 'cluster' ]).agg(
        feature_sales_cat2_mean = ('adjusted_net_units_sold','mean'),
        feature_sales_cat2_max = ('adjusted_net_units_sold','max'),
        feature_sales_cat2_std = ('adjusted_net_units_sold','std')
    ).reset_index()
    
    feature_sales_cluster = input_df.groupby(['henry_category_2', 'sub_product_line', 'cluster', 'size','week_id']).agg(
        feature_sales_cluster_mean = ('adjusted_net_units_sold','mean'),
        feature_sales_cluster_max = ('adjusted_net_units_sold','max'),
        feature_sales_cluster_std = ('adjusted_net_units_sold','std')
    ).reset_index()
    
    return feature_sales_category, feature_sales_category2, feature_sales_cluster


def get_size_distribution(input_data, sales_column):
    #input_data = mid_list.copy()
    #sales_column = 'pred_round'

    size_vals = ['XXS', 'XS', 'S', 'M', 'L', 'XL', 'XXL']
    input_data = input_data[input_data['size'].isin(size_vals)]

    total_size_dist = input_data.groupby(
        ['id_product','color', 'id_shop_name','sub_product_line', 'cluster']
    )[sales_column].sum().reset_index()
    total_size_dist.rename(columns = {sales_column: 'total_volume'}, inplace = True)

    size_dist_data = input_data.groupby(
        ['id_product','color', 'id_shop_name', 'cluster', 'sub_product_line','size']
    )[sales_column].sum().reset_index()
    size_dist_data.rename(columns = {sales_column: 'size_volume'}, inplace = True)

    size_dist_data = pd.merge(
        size_dist_data,
        total_size_dist,
        on = ['id_product','color', 'id_shop_name','sub_product_line', 'cluster'],
        how = 'left'
    )
    size_dist_data['size_dist'] = size_dist_data['size_volume'] / size_dist_data['total_volume']

    size_dist_data = size_dist_data.groupby(['id_shop_name', 'cluster', 'sub_product_line','size'])['size_dist'].mean().reset_index()

    size_dist_data['size'] = pd.Categorical(
        size_dist_data['size'], 
        categories=size_vals,
        ordered=True)

    size_dist_data.sort_values(by = 'size', inplace = True)
    size_dist_data['size'] = size_dist_data['size'].astype(str)
    
    return size_dist_data


def get_historic_week_dist_feature(input_df: pd.DataFrame) -> pd.DataFrame:
    """ Returns dataframe that carries week distirbution by cat1, cluster, spl and country
    
    Args:
        input_df: DataFrame that the week_distribution should be calculated on
    Returns:
        week distribution dataframe
    """
    
    total_week_sales = input_df.groupby(['henry_category_2', 'sub_product_line', 'cluster', 'id_shop_name']).adjusted_net_units_sold.sum().reset_index()
    total_week_sales.rename(columns = {'adjusted_net_units_sold': 'total_sales'}, inplace = True)

    week_dist_data = input_df.groupby(['henry_category_2', 'sub_product_line', 'cluster', 'id_shop_name', 'week_id']).adjusted_net_units_sold.sum().reset_index()
    week_dist_data = pd.merge(
        week_dist_data,
        total_week_sales,
        on = ['henry_category_2', 'sub_product_line', 'cluster', 'id_shop_name'],
        how = 'left')

    week_dist_data['week_dist'] = week_dist_data.adjusted_net_units_sold / week_dist_data.total_sales
    
    return week_dist_data

