import gspread
import pandas as pd

from config.project_config import GSHEET_SECRET
from src.utils.pomelo_utils import Hal



def retail_marketing_spend(google_service=None):
    # Reatil marketing spend both hitsorical and budget data
    # Fetch data from Google sheet
    # NOTE: Change SERVICE ACCOUNT
    gc = gspread.service_account(
        GSHEET_SECRET
    )

    # historical retail marketing spend
    # https://docs.google.com/spreadsheets/d/1HpegKJjxUFCBDwvpR29DUgcPZEAwgaUFmVBlIHt1844/edit#gid=0
    sh = gc.open_by_key("1HpegKJjxUFCBDwvpR29DUgcPZEAwgaUFmVBlIHt1844")
    wk = sh.worksheet("summary")
    retail_marketing_hist = pd.DataFrame.from_dict(wk.get_all_records())

    # retail marketing spend  budget
    # https://docs.google.com/spreadsheets/d/1ygDhDd9rWv7R4aU8W7NkRMVTJ-N_bmi9JFg6ttqBkug/edit?ts=6070315b#gid=1436820854
    sh = gc.open_by_key("1ygDhDd9rWv7R4aU8W7NkRMVTJ-N_bmi9JFg6ttqBkug")
    wk = sh.worksheet("Data science")
    retail_marketing_budget = pd.DataFrame.from_dict(wk.get_all_records())

    # store mapping
    # https://docs.google.com/spreadsheets/d/1TTM285yQXTxK_I0Kzb3MYq0XftBI6Wjf7WaBNq9traw/edit#gid=0
    sh = gc.open_by_key("1TTM285yQXTxK_I0Kzb3MYq0XftBI6Wjf7WaBNq9traw")
    wk = sh.worksheet("Sheet1")
    store_mapping = pd.DataFrame.from_dict(wk.get_all_records())

    # monthid
    sql = """ select distinct month_id
                            ,month_name
                from dwh.dim_calendar
                order by month_id"""
    hal = Hal()
    monthid = hal.get_pandas_df(sql)
    monthid["month_name"] = monthid["month_name"].str.strip()
    monthid["month_name"] = monthid["month_name"].replace("Septembe", "September")

    retail_marketing_hist = retail_marketing_hist.merge(
        monthid, how="left", on="month_id"
    ).drop("month", axis=1)
    retail_marketing_hist.rename({"store_id": "erply_store_id"}, axis=1, inplace=True)
    retail_marketing_hist = retail_marketing_hist.merge(
        store_mapping[["id_shop", "erply_store_id"]].drop_duplicates(),
        how="left",
        on="erply_store_id",
    )
    retail_marketing_hist.rename({"erply_store_id": "store_id"}, axis=1, inplace=True)
    retail_marketing_hist = retail_marketing_hist.dropna()
    convert = {"id_shop": int}
    retail_marketing_hist = retail_marketing_hist.astype(convert)

    retail_budget_unpivot = retail_marketing_budget.melt(
        id_vars=["Branch", "Abbreviation"],
        var_name="month_name",
        value_name="retail_mkt_spend",
    )
    retail_budget_unpivot = retail_budget_unpivot.dropna()
    retail_budget_unpivot.rename({"Branch": "store_name"}, axis=1, inplace=True)
    retail_budget_unpivot.rename(
        {"Abbreviation": "store_name_from_0421"}, axis=1, inplace=True
    )

    retail_budget_unpivot_1 = retail_budget_unpivot.merge(
        store_mapping[["id_shop", "erply_store_id", "store_name_from_0421"]],
        how="left",
        on="store_name_from_0421",
    )
    retail_budget_unpivot_1 = retail_budget_unpivot_1.dropna()
    convert = {"id_shop": int, "erply_store_id": int}
    retail_budget_unpivot_1 = retail_budget_unpivot_1.astype(convert)

    retail_budget_unpivot_1["year"] = 2021
    retail_budget_unpivot_1 = retail_budget_unpivot_1.merge(
        monthid, how="left", on="month_name"
    )
    convert = {"month_id": int}
    retail_budget_unpivot_1 = retail_budget_unpivot_1.astype(convert)
    retail_budget_unpivot_1.rename({"erply_store_id": "store_id"}, axis=1, inplace=True)

    def year_month_id(df):
        if df["month_id"] < 10:
            return str(df["year"]) + "0" + str(df["month_id"])
        elif df["month_id"] >= 10:
            return str(df["year"]) + str(df["month_id"])

    retail_budget_unpivot_1["year_month_id"] = retail_budget_unpivot_1.apply(
        year_month_id, axis=1
    )
    retail_budget_unpivot_1 = retail_budget_unpivot_1.merge(
        retail_marketing_hist[["id_shop", "country"]].drop_duplicates(),
        how="left",
        on="id_shop",
    )
    retail_budget_unpivot_1.rename({"store_name": "store"}, axis=1, inplace=True)

    convert = {"year_month_id": int, "store_id": int, "id_shop": int}
    retail_marketing_hist = retail_marketing_hist.astype(convert)
    retail_budget_unpivot_1 = retail_budget_unpivot_1.astype(convert)
    retail_marketing_spend = pd.concat(
        [
            retail_marketing_hist[
                [
                    "year",
                    "month_id",
                    "year_month_id",
                    "store",
                    "store_id",
                    "country",
                    "retail_mkt_spend",
                    "month_name",
                    "id_shop",
                ]
            ],
            retail_budget_unpivot_1[
                [
                    "year",
                    "month_id",
                    "year_month_id",
                    "store",
                    "store_id",
                    "country",
                    "retail_mkt_spend",
                    "month_name",
                    "id_shop",
                ]
            ],
        ]
    )
    retail_marketing_spend = retail_marketing_spend[
        retail_marketing_spend["retail_mkt_spend"] > 0
    ]

    print("Marketing data is loaded...")

    #     retail_marketing_spend.to_csv('s3://hal-bi-bucket/data_science/pricing/pricing_offline_v3/data/retail_mkt_spend.csv', index=False)
    retail_marketing_spend.to_csv(
        "s3://hal-bi-bucket/data_science/pricing/pricing_offline_v3/trainset/data/retail_mkt_spend.csv",
        index=False,
    )
    return retail_marketing_spend
