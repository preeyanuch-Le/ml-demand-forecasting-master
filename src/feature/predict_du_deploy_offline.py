from datetime import datetime, timedelta

import gspread
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from src.queries.query_du_strategy_dfm_offline import query_du_strategy_dfm_offline
from src.queries.offline_week_in_month import week_in_month
from config.project_config import GSHEET_SECRET

# https://docs.google.com/spreadsheets/d/1aUOYlUtyCzA9bYB0d3cttAhru9gnv39q0c4W2a0snw8/edit#gid=0
gc = gspread.service_account(GSHEET_SECRET)
sh = gc.open_by_key("1aUOYlUtyCzA9bYB0d3cttAhru9gnv39q0c4W2a0snw8")
tab = sh.worksheet("offline_DU_target_country_channel")
df_DU_target = pd.DataFrame.from_dict(tab.get_all_records())

# fill nan
df_DU_target = df_DU_target.replace("None", 0)


def predict_du_deploy(actual_data):
    dfm = actual_data.copy()
    df_week = week_in_month()

    # replace 'week'
    dfm["week_id"] = dfm["week_id"].str.replace("week", "").astype("int")

    # add is_new_arrival
    na_indexes = dfm[dfm["week_id"] < 4].index
    dfm.loc[na_indexes, "is_new_arrivals"] = 1
    dfm["is_new_arrivals"] = dfm["is_new_arrivals"].fillna(0)

    # change to datetime
    dfm["start_date_weekly"] = pd.to_datetime(dfm["start_date_weekly"])
    df_week["full_date"] = pd.to_datetime(df_week["full_date"])

    df_dfm = dfm.merge(
        df_week[["full_date", "week_in_month"]],
        how="left",
        right_on=["full_date"],
        left_on=["start_date_weekly"],
    )
    df_dfm["year_month"] = df_dfm["start_date_weekly"].dt.strftime("%Y-%m")

    # - ->ADD PRODUCT_TYPE TO RAW DATA
    # After 28 Days = non_clearance
    df_dfm["product_type"] = [
        "new_arrivals" if x <= 4 else "non_clearance" for x in df_dfm["week_id"]
    ]

    # --> OFFLINE NO HERO PRODUCT
    df_dfm["sub_product_line"] = df_dfm["sub_product_line"].str.lower()
    df_dfm["category_1"] = df_dfm["category_1"].str.lower()

    # DU Strategy

    # Query History DataFrame (Insert Start Date and End Date)
    # start_date = 3 months prior to today
    start_date = datetime.now() + relativedelta(months=-3)
    start_date = start_date.replace(day=1).strftime("%Y-%m-%d")
    # end_date = yesterday
    end_date = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d")
    print(f"Get historical date: from {start_date} to {end_date}")

    # Get Historical Data
    df = query_du_strategy_dfm_offline(start_date, end_date)
    display(df.groupby("product_type").mean()[["voucher_du", "item_du"]])

    # --> Calculate Diff Ratio

    # Historic DU Group by "SHOP" -> Get old_voucher_du
    df_top = (
        df.groupby(["id_shop"]).agg(old_voucher_du=("voucher_du", "mean")).reset_index()
    )

    def get_shop_id(x):
        if x == "1":
            shop = "TH"
        if x == "2":
            shop = "SG"
        if x == "5":
            shop = "ID"
        if x == "11":
            shop = "MY"
        return shop

    df_top["country"] = df_top["id_shop"].apply(get_shop_id)

    display(df_top)

    # Reference DU (Reference Month) -> Get ref_du
    # Merge Historic DU with Reference DU
    ref_du = pd.merge(
        df_top,
        df_DU_target[["country", "date", "voucher_du_target"]],
        on="country",
        how="left",
    )
    
    
    ref_du = ref_du.rename({"voucher_du_target": "ref_du"}, axis=1)

    # Diff Ratio = Reference DU / Historical DU
    ref_du["diff_ratio"] = ref_du["ref_du"] / ref_du["old_voucher_du"]
    ref_du = ref_du[["id_shop", "country", "date", "diff_ratio"]]

    # FILL NA AND INF WITH 0
    ref_du = ref_du.replace([np.inf, -np.inf], np.nan)
    ref_du.fillna(0, inplace=True)

    display(ref_du)
    df.rename(columns={"henry_category_1": "category_1"}, inplace=True)

    # Transform New DU

    # Aggregate DU on most bottom level (henry_cat1 level)
    df_bottom = (
        df.groupby(
            [
                "id_shop_name",
                "week_in_month",
                "is_mega_campaign_order",
                "product_type",
                "sub_product_line",
                "category_1",
                #                         'flag_broken_size',
            ]
        )
        .agg(old_voucher_du=("voucher_du", "mean"))
        .reset_index()
    )

    # Join with Actual Data to get old_voucher_du for each row
    df_final = pd.merge(
        df_dfm,
        df_bottom,
        on=[
            "id_shop_name",
            "week_in_month",
            "is_mega_campaign_order",
            "product_type",
            "sub_product_line",
            "category_1",
        ],
        how="left",
    )

    # Aggregate back 1-3 level
    df_spare = (
        df.groupby(
            [
                "id_shop_name",
                "week_in_month",
                "is_mega_campaign_order",
                "product_type",
                "sub_product_line",
                #                         'category_1', #aggregate back 1 level
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(spare_voucher_du=("voucher_du", "mean"))
        .reset_index()
    )

    df_spare2 = (
        df.groupby(
            [
                "id_shop_name",
                "week_in_month",
                "is_mega_campaign_order",
                "product_type",
                #                         'sub_product_line', #aggregate back 2 level
                #                         'category_1', #aggregate back 1 level
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(spare_voucher_du=("voucher_du", "mean"))
        .reset_index()
    )

    df_spare3 = (
        df.groupby(
            [
                "id_shop_name",
                "week_in_month",
                #                     'is_mega_campaign_order',
                "product_type",
                "sub_product_line",  # aggregate back 2 level
                "category_1",  # aggregate back 1 level
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(spare_voucher_du=("voucher_du", "mean"))
        .reset_index()
    )

    df_spare_week_1 = (
        df.groupby(
            [
                "id_shop_name",
                #                         'week_in_month',
                "is_mega_campaign_order",
                "product_type",  # aggregate back 3 level
                "sub_product_line",  # aggregate back 2 level
                "category_1",  # aggregate back 1 level
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(spare_voucher_du=("voucher_du", "mean"))
        .reset_index()
    )

    df_spare_week_2 = (
        df.groupby(
            [
                "id_shop_name",
                #                         'week_in_month',
                "is_mega_campaign_order",
                "product_type",  # aggregate back 3 level
                "sub_product_line",  # aggregate back 2 level
                #                         'category_1', #aggregate back 1 level
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(spare_voucher_du=("voucher_du", "mean"))
        .reset_index()
    )

    # Join with missing rows
    df_final["old_voucher_du"] = df_final["old_voucher_du"].fillna(
        df_final.merge(
            df_spare,
            on=[
                "id_shop_name",
                "week_in_month",
                "is_mega_campaign_order",
                "product_type",
                "sub_product_line",
            ],
            how="left",
        )["spare_voucher_du"]
    )

    df_final["old_voucher_du"] = df_final["old_voucher_du"].fillna(
        df_final.merge(
            df_spare2,
            on=[
                "id_shop_name",
                "week_in_month",
                "is_mega_campaign_order",
                "product_type",
            ],
            how="left",
        )["spare_voucher_du"]
    )

    df_final["old_voucher_du"] = df_final["old_voucher_du"].fillna(
        df_final.merge(
            df_spare_week_1,
            on=[
                "id_shop_name",
                "is_mega_campaign_order",
                "product_type",
                "sub_product_line",
                "category_1",
            ],
            how="left",
        )["spare_voucher_du"]
    )

    df_final["old_voucher_du"] = df_final["old_voucher_du"].fillna(
        df_final.merge(
            df_spare_week_2,
            on=[
                "id_shop_name",
                "is_mega_campaign_order",
                "product_type",
                "sub_product_line",
            ],
            how="left",
        )["spare_voucher_du"]
    )

    df_final["old_voucher_du"] = df_final["old_voucher_du"].fillna(
        df_final.merge(
            df_spare3,
            on=[
                "id_shop_name",
                "week_in_month",
                "product_type",
                "sub_product_line",
                "category_1",
            ],
            how="left",
        )["spare_voucher_du"]
    )

    # Join with ref_du table to calculate new DU
    df_final2 = pd.merge(
        df_final,
        ref_du,
        left_on=["id_shop_name", "year_month"],
        right_on=["country", "date"],
        how="left",
    )

    # new_voucher_du = old_voucher_du * diff_ratio
    # New Arrivals Inhouse are given 0% Item DU
    df_final2["voucher_du"] = np.where(
        df_final2["is_new_arrivals"] == "new_arrivals",
        0,
        df_final2["old_voucher_du"] * df_final2["diff_ratio"],
    )
    # Voucher DU are given 10%
    df_final2["item_du"] = 0.0

    # Total DU = Item DU + Voucher DU
    df_final2["total_du"] = df_final2["voucher_du"] + df_final2["item_du"]
    df_final2.drop(columns=["old_voucher_du", "diff_ratio"], inplace=True)

    display(df_final2[["voucher_du", "item_du"]].mean())

    # Check number of Null Value
    num_null = len(df_final2[df_final2["voucher_du"].isna()])

    # Print Information
    print("\\\\Historical Data//")
    print("start date: ", start_date)
    print("end date: ", end_date)
    print("\n\\\\DU Strategy//")
    print("\nNum rows raw data: ", len(dfm))
    print("Num rows DU Strategy: ", len(df_final2))
    print("\nNo. of Null Rows: ", num_null)
    print(f"Percentage: {100 * num_null / len(df):.2f} %")
    print("\nSample Output")
    display(df_final2.head(10))

    return df_final2
