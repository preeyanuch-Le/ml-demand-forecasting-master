from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from src.queries.query_du_strategy_dfm import query_du_strategy_dfm


def predict_du_deploy(actual_data):

    # Retrieve week csv from s3
    df_week = pd.read_csv(
        "s3://hal-bi-bucket/data_science/du_forecasting/data/week.csv"
    )
    df_week.rename(columns={"year_id": "year"}, inplace=True)
    df_week["full_date"] = pd.to_datetime(df_week["full_date"])
    df_week = df_week[["full_date", "week_in_month"]]

    # Retrieve warehouse budget target from s3
    
    #"s3://hal-bi-bucket/data_science/du_forecasting/data/warehouse_budget_target.csv"
    warehouse_budget_target = pd.read_csv(
         "s3://hal-bi-bucket/data_science/du_forecasting/data/warehouse_budget_target_temp.csv"
    )

    actual_data["start_date_weekly"] = pd.to_datetime(actual_data["start_date_weekly"])

    dfm = actual_data.copy()
    # Clean actual_data

    # Calculate week_in_month
    # Get week_id
    # dfm["week_id"] = dfm["week_id"].str.replace("week", "").astype("int")

    dfm["date_released"] = pd.to_datetime(dfm["date_released"])

    # datediff = week_id * 7
    dfm["datediff"] = 7 * dfm["week_id"]
    dfm["datediff"] = pd.to_timedelta(dfm["datediff"], unit="day")

    # date of interest = date_released + datediff
    dfm["actual_date"] = dfm["date_released"] + dfm["datediff"]

    # join week_in_month
    df_dfm = pd.merge(dfm, df_week, left_on=["actual_date"], right_on=["full_date"])
    df_dfm.drop(columns=["full_date", "datediff"], inplace=True)
    df_dfm["year_month"] = df_dfm["actual_date"].dt.strftime("%Y-%m")

    df_dfm["year_month"] = df_dfm["start_date_weekly"].dt.strftime("%Y-%m")

    # Product Type
    # After 28 Days = non_clearance
    df_dfm["product_type"] = [
        "new_arrivals" if x <= 4 else "non_clearance" for x in df_dfm["week_id"]
    ]

    # clean henry cat1
    df_dfm.rename(columns={"category_1": "henry_category_1"}, inplace=True)
    df_dfm["sub_product_line"] = df_dfm["sub_product_line"].str.lower()
    df_dfm["henry_category_1"] = df_dfm["henry_category_1"].str.lower()

    # DU Strategy

    # Query History DataFrame (Insert Start Date and End Date)

    # start_date = 3 months prior to today
    start_date = datetime.now() + relativedelta(months=-3)
    start_date = start_date.replace(day=1).strftime("%Y-%m-%d")

    # end_date = yesterday
    end_date = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d")

    # Get DataFrame
    df = query_du_strategy_dfm(start_date, end_date)

    # Calculate Diff Ratio

    # Historic DU Group by warehouse -> Get old_item_du
    df_top = (
        df.groupby(["warehouse"]).agg(old_item_du=("item_du", "mean")).reset_index()
    )

    # Reference DU (Reference Month) -> Get ref_du
    # Merge Historic DU with Reference DU
    ref_du = pd.merge(df_top, warehouse_budget_target, on="warehouse", how="left")

    # Diff Ratio = Reference DU / Historical DU
    ref_du["diff_ratio"] = ref_du["ref_du"] / ref_du["old_item_du"]
    ref_du = ref_du[["warehouse", "year_month", "diff_ratio"]]
    display(ref_du)

    # Transform New DU

    # Aggregate DU on most bottom level (henry_cat1 level)
    df_bottom = (
        df.groupby(
            [
                "warehouse",  # done
                "week_in_month",  # done
                "is_mega_campaign_order",  # done
                "product_type",  # done
                "sub_product_line",  # done
                "henry_category_1",  # done
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(old_item_du=("item_du", "mean"))
        .reset_index()
    )

    #     del df_dfm['week_in_month_x']
    df_dfm.rename(columns={"week_in_month_y": "week_in_month"}, inplace=True)

    # Join with Actual Data to get old_item_du for each row
    df_final = pd.merge(
        df_dfm,
        df_bottom,
        on=[
            "warehouse",
            "week_in_month",
            "is_mega_campaign_order",
            "product_type",
            "sub_product_line",
            "henry_category_1",
        ],
        how="left",
    )

    # Aggregate back 1-3 level
    df_spare = (
        df.groupby(
            [
                "warehouse",  # done
                "week_in_month",  # done
                "is_mega_campaign_order",  # done
                "product_type",  # done
                "sub_product_line",  # done
                #                         'henry_category_1', #aggregate back 1 level
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(spare_item_du=("item_du", "mean"))
        .reset_index()
    )

    df_spare2 = (
        df.groupby(
            [
                "warehouse",  # done
                "week_in_month",  # done
                "is_mega_campaign_order",  # done
                "product_type",  # done
                #                         'sub_product_line', #aggregate back 2 level
                #                         'henry_category_1', #aggregate back 1 level
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(spare_item_du=("item_du", "mean"))
        .reset_index()
    )

    df_spare_week_1 = (
        df.groupby(
            [
                "warehouse",  # done
                #                         'week_in_month', #done
                "is_mega_campaign_order",  # done
                "product_type",  # aggregate back 3 level
                "sub_product_line",  # aggregate back 2 level
                "henry_category_1",  # aggregate back 1 level
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(spare_item_du=("item_du", "mean"))
        .reset_index()
    )

    df_spare_week_2 = (
        df.groupby(
            [
                "warehouse",  # done
                #                         'week_in_month', #done
                "is_mega_campaign_order",  # done
                "product_type",  # aggregate back 3 level
                "sub_product_line",  # aggregate back 2 level
                #                         'henry_category_1', #aggregate back 1 level
                #                         'flag_broken_size', #remove
            ]
        )
        .agg(spare_item_du=("item_du", "mean"))
        .reset_index()
    )

    # Join with missing rows
    df_final["old_item_du"] = df_final["old_item_du"].fillna(
        df_final.merge(
            df_spare,
            on=[
                "warehouse",
                "week_in_month",
                "is_mega_campaign_order",
                "product_type",
                "sub_product_line",
            ],
            how="left",
        )["spare_item_du"]
    )

    df_final["old_item_du"] = df_final["old_item_du"].fillna(
        df_final.merge(
            df_spare2,
            on=["warehouse", "week_in_month", "is_mega_campaign_order", "product_type"],
            how="left",
        )["spare_item_du"]
    )

    df_final["old_item_du"] = df_final["old_item_du"].fillna(
        df_final.merge(
            df_spare_week_1,
            on=[
                "warehouse",
                "is_mega_campaign_order",
                "product_type",
                "sub_product_line",
                "henry_category_1",
            ],
            how="left",
        )["spare_item_du"]
    )

    df_final["old_item_du"] = df_final["old_item_du"].fillna(
        df_final.merge(
            df_spare_week_2,
            on=[
                "warehouse",
                "is_mega_campaign_order",
                "product_type",
                "sub_product_line",
            ],
            how="left",
        )["spare_item_du"]
    )

    # Join with ref_du table to calculate new DU
    df_final2 = pd.merge(df_final, ref_du, on=["warehouse", "year_month"])

    # new_item_du = old_item_du * diff_ratio
    # New Arrivals Inhouse are given 0% Item DU
    df_final2["item_du"] = np.where(
        df_final2["product_type"] == "new_arrivals",
        0,
        df_final2["old_item_du"] * df_final2["diff_ratio"],
    )
    # Voucher DU are given 10%
    df_final2["voucher_du"] = 0.10

    # Total DU = Item DU + Voucher DU
    df_final2["total_du"] = df_final2["item_du"] + df_final2["voucher_du"]
    df_final2.drop(columns=["old_item_du", "diff_ratio"], inplace=True)

    # Check number of Null Value
    num_null = len(df_final2[df_final2["item_du"].isna()])

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