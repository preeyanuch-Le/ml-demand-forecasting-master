import argparse
import os
import warnings
import pandas as pd
import numpy as np
import boto3
import pickle
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import awswrangler as wr
from src.utils.decorators import timeit
from config.project_config import *
from sklearn.exceptions import DataConversionWarning
warnings.filterwarnings(action='ignore', category=DataConversionWarning)

from src.queries.offline_year_week_id import year_week_id
from src.queries.calendar import get_calendar 
from src.queries.offline_get_store_traffic import get_tot_store_traffic 
from src.queries.offline_get_total_product_styles import get_tot_product_style 
from src.queries.offline_get_product_sold import get_product_sold
from src.queries.offline_get_existing_styles import get_existing_styles
from src.queries.offline_get_product_lauched import get_product_lauched
from src.feature.add_covid_offline import add_covid
from config.project_config import GSHEET_SECRET


import gspread
import boto3

gc = gspread.service_account(filename=GSHEET_SECRET)

s3 = boto3.resource("s3")

def _calculate_day_no(row):
    if row["week_id"] == "week1":
        return 0
    if row["week_id"] == "week2":
        return 7
    if row["week_id"] == "week3":
        return 14
    if row["week_id"] == "week4":
        return 21
    if row["week_id"] == "week5":
        return 28
    if row["week_id"] == "week6":
        return 35
    if row["week_id"] == "week7":
        return 42
    
    
class prep_offline_data :
    """ 
    - clean and transform raw data
    - add features : start_date_weekly, covid, store traffic, sale proportion, retail mkt spend
    - fill missing values for features
    
    output : cleaned raw data for offline_processing_transform function
    """
        
    def __init__(self, unpivot_raw):
        self.unpivot_raw = unpivot_raw.copy()
        
    @timeit
    def run(self, unpivot_raw):
        unpivot_raw = unpivot_raw.copy()
        
        # CALCULATE start_date_weekly ON first_available_date AND CLEAN DATE COLUMNS 
        unpivot_raw = self.transform_date(unpivot_raw)
        # ADD COVID
        raw_w_covid = self.add_covid(unpivot_raw)
        # GET STORE TRAFFIC
        store_traffic = self.get_store_traffic() 
        # ADD STORE TRAFFIC
        raw3 = self.add_store_traffic(raw_w_covid,store_traffic)
        # FILL NA STORE TRAFFIC
        raw4 = self.fill_na_store_traffic(raw3)
        # GET SALE PROP
        sale_prop = self.get_sale_prop()
        # ADD SALE PROP
        raw5 = self.add_sale_prop(raw4,sale_prop)
        # ADD RETAIL MKT SPEND
        raw6 = self.add_retail_mkt_spend(raw5)
        # FILL NA RETAIL MKT SPEND
        raw6 = self.fill_retail_mkt_spend(raw6)
        return raw6
    
    @timeit   
    def transform_date(self ,unpivot_raw: pd.DataFrame):
        unpivot_raw["date_released"] = pd.to_datetime(unpivot_raw["date_released"])
        unpivot_raw["max_transaction_date"] = pd.to_datetime(
            unpivot_raw["max_transaction_date"]
        )
        unpivot_raw["first_available_date"] = pd.to_datetime(
            unpivot_raw["first_available_date"]
        )
        unpivot_raw["min_transaction_date"] = pd.to_datetime(
            unpivot_raw["min_transaction_date"]
        )

        # if min_transaction_date before first available date, then that will be the new first availble date
        unpivot_raw["first_available_date"] = np.where(
        (unpivot_raw["first_available_date"] < unpivot_raw["min_transaction_date"])
        & (~unpivot_raw["min_transaction_date"].isna()),
        unpivot_raw["first_available_date"],
        unpivot_raw["min_transaction_date"],
    )

        # add col 'day_no' for calculate start_date_weekly on first_available_date not released date
        unpivot_raw.loc[:, "day_no"] = unpivot_raw.apply(
            lambda row: _calculate_day_no(row), axis=1
        )

        # add col 'start_date_weekly'
        unpivot_raw["start_date_weekly"] = pd.DatetimeIndex(
            unpivot_raw["first_available_date"]
        ) + unpivot_raw["day_no"].apply(pd.offsets.Day)

        del unpivot_raw["day_no"]  # we don't need it anymore
        return unpivot_raw
    
    @timeit 
    def add_covid(self,unpivot_raw):
        df = pd.read_csv(COVID_FILE_URL, error_bad_lines=False)

        filtered_data = df[
            ["date", "Thailand", "Malaysia", "Singapore", "Indonesia", "Philippines"]
        ]
        filtered_data.fillna(0, inplace=True)
        filtered_data["date"] = pd.to_datetime(filtered_data["date"])

        # daily cavid case for each CDC
        cdc_data = pd.DataFrame(columns=["date", "TH", "MY", "ID"])
        cdc_data["date"] = filtered_data["date"]
        cdc_data["TH"] = filtered_data["Thailand"]
        cdc_data["ID"] = filtered_data["Indonesia"]
        cdc_data["MY"] = (
            0.45 * filtered_data["Malaysia"]
            + 0.45 * filtered_data["Singapore"]
            + 0.1 * filtered_data["Philippines"]
        )

        # unique date_released for each warehouse
        raw_covid = pd.DataFrame()
        
        for i in ["TH", "MY", "ID"]:
            raw_empty = pd.DataFrame(columns=["start_date_weekly", "warehouse", "covid_cases"])
            raw_country = unpivot_raw[unpivot_raw["warehouse"] == i]
            raw_empty["start_date_weekly"] = raw_country["start_date_weekly"].unique()
            raw_empty["warehouse"] = i
            raw_covid = raw_covid.append(raw_empty)
        
        raw_covid = raw_covid.sort_values(by="start_date_weekly").reset_index(drop=True)
        raw_covid["start_date_weekly"] = pd.to_datetime(raw_covid["start_date_weekly"])

        # adding no.of covid cases for each week (the first 7days since start_date_weekly)
        for i in range(0, len(raw_covid)):
            for j in range(0, len(cdc_data)):
                for k in ["TH", "MY", "ID"]:
                    if (raw_covid["start_date_weekly"][i] == cdc_data["date"][j]) & (
                        raw_covid["warehouse"][i] == k
                    ):
                        raw_covid["covid_cases"][i] = cdc_data[
                            (cdc_data["date"] >= raw_covid["start_date_weekly"][i])
                            & (
                                cdc_data["date"]
                                < (
                                    raw_covid["start_date_weekly"][i] + timedelta(days=7)
                                ).strftime("%Y-%m-%d")
                            )
                        ][k].sum()

        raw_covid.fillna(0, inplace=True)
        unpivot_raw["start_date_weekly"] = unpivot_raw["start_date_weekly"].astype(str)
        raw_covid["start_date_weekly"] = raw_covid["start_date_weekly"].astype(str)
        raw_w_covid = unpivot_raw.merge(
            raw_covid, how="left", on=["start_date_weekly", "warehouse"]
        )

        return raw_w_covid
    
    @timeit
    def get_store_traffic(self):
        """
        1. join tot_store_traffic with calendar
        2. join tot_product_style with calendar 
        3. join 1 and 2 then calculate traffic_per_styles = tot_store_traffic/tot_product_styles
        """
        calendar_df = get_calendar()
        tot_store_traffic = get_tot_store_traffic()
        tot_product_style = get_tot_product_style()
        
        store_traffic = pd.merge(tot_store_traffic,calendar_df,how='left',on='full_date')
        store_traffic = store_traffic.groupby(['year_week_id','id_store','store_name','id_shop'])['tot_store_traffic'].sum().reset_index()
        
        product_style = pd.merge(tot_product_style,calendar_df,how='left',on='full_date')
        product_style = product_style.groupby(['year_week_id','id_store','id_shop'])['id_product'].count().reset_index()
        product_style.rename(columns={'id_product':'tot_product_styles'},inplace=True)
        product_style['id_shop'] = product_style['id_shop'].astype(int)
    
        store_traffic_final = pd.merge(store_traffic,product_style,how='left',on=['year_week_id','id_store','id_shop'])
        store_traffic_final['traffic_per_styles'] = store_traffic_final['tot_store_traffic']/store_traffic_final['tot_product_styles']
        store_traffic_final['traffic_per_styles'] = store_traffic_final['traffic_per_styles'].fillna(0)
        return store_traffic_final

    
    
    @timeit
    def add_store_traffic(self,raw_w_covid: pd.DataFrame,store_traffic):
        calendar = year_week_id()
        calendar.rename(columns={"full_date": "start_date_weekly"}, inplace=True)
        calendar["start_date_weekly"] = pd.to_datetime(
        calendar["start_date_weekly"], format="%Y-%m-%d")

        raw_w_covid["start_date_weekly"] = pd.to_datetime(
        raw_w_covid["start_date_weekly"], format="%Y-%m-%d")

        raw2 = raw_w_covid.merge(calendar, how="left", on="start_date_weekly")
        raw2 = raw2[~raw2["year_week_id"].isna()]

        store_traffic2 = store_traffic[["year_week_id", "id_store", "store_name", "id_shop", "tot_store_traffic"]]
        # id store cleaning
        raw2["id_store"] = np.where(raw2["id_store"] == "5bb2e6ac1e5abf2252df039c", "251", raw2["id_store"])
        raw2["id_store"] = np.where(raw2["id_store"] == "5b55a681868e636a8ab1eaa2", "261", raw2["id_store"])
        raw2["id_store"] = np.where(raw2["id_store"] == "5b9890f4d49bab0743f5689e", "241", raw2["id_store"])

        store_traffic["id_shop"] = store_traffic["id_shop"].astype(str)
        raw3 = raw2.merge(
        store_traffic2[["year_week_id", "id_store", "tot_store_traffic"]].drop_duplicates(),
        how="left",
        on=["year_week_id", "id_store"],)

        raw3[raw3["tot_store_traffic"].isna()]
        return raw3

    @timeit
    def fill_na_store_traffic(self,raw3) :
        """
        To fill missing value (store traffic by month)
        there are some store in some week that doesn't have store trafic- > we use the month avg from the finance team to fill the na

    for week before Jan 19 -> use MoM & YoY avg to fill

    like Dec 18 of Central world we use % different of Dec 19 and Dec 20 of Central world to back fill 

    eg. if for Central world

    Dec20 -> strore traffic ==100

    Dec19 -> strore traffic ==120

    Dec18 = 100/(1+((120-100)/100)) = 100/(1+0.2) = 83.3

    for paragon used avg of store cluster

        """
        raw3["month"] = raw3["start_date_weekly"].dt.month_name()
        raw3["year"] = raw3["start_date_weekly"].dt.year

        end_date = date.today().strftime("%Y-%m-%d")
        start_date = (date.today() - relativedelta(weeks=+7)).strftime("%Y-%m-%d")
        # filter out product that hasn't been in store for more than 7 weeks
        raw4 = raw3[raw3["first_available_date"] < start_date]
        raw4["year"] = raw4["year"].astype(str)

        store_na_filled = pd.read_csv(
        "s3://hal-bi-bucket/data_science/dfm/excel_files/store_traffic_na_filled.csv")
        store_na_filled.replace(",", "", regex=True, inplace=True)
        store_na_filled["month"] = store_na_filled["month"].str.strip()
        store_na_filled["tot_traffic"] = store_na_filled["tot_traffic"].astype(np.int64)
        store_na_filled["tot_store_traffic"] = store_na_filled["tot_traffic"] / 4
        del store_na_filled["tot_traffic"]
        del store_na_filled["store_name"]

        store_na_filled["year"] = store_na_filled["year"].astype(str)
        store_na_filled["id_store"] = store_na_filled["id_store"].astype(str)
        raw4.replace({pd.NA: np.nan},inplace=True)

        raw4.loc[raw4["tot_store_traffic"].isna(), "tot_store_traffic"] = (
        raw4[raw4["tot_store_traffic"].isna()].merge(store_na_filled, how="left", on=["month", "year", "id_store"])["tot_store_traffic_y"].values)
        # fill with 0 as either store was closed or it's not open yet
        raw4["tot_store_traffic"] = raw4["tot_store_traffic"].fillna(0)  
        return raw4
    
    @timeit
    def get_sale_prop(self):
        pd_sold = get_product_sold()
        pd_launched = get_product_lauched()
        exist_styles = get_existing_styles()
        # join pd_sold with exist_styles 
        sale_prop = pd.merge(pd_sold,exist_styles, how='left', left_on = ['year_week_sold','id_store','id_shop'],
         right_on = ['year_week_id','id_store','id_shop'])
        
        pd_launched['id_store'] =pd_launched['id_store'].astype(str)
        # join with product_lauched
        sale_prop = pd.merge(sale_prop,pd_launched, how='left', left_on = ['year_week_id','id_store'],
         right_on = ['year_week_available','id_store'])
        
        # Get data by week sold, month id, shop, warehouse, store, product launched  level----
        sale_prop = sale_prop.groupby(['year_week_sold','year_month_id','id_shop', 'id_warehouse',])[
        'product_launched','active_product','new_pd_sold','old_pd_sold','net_units_sold'].sum().reset_index()
        sale_prop.rename(columns = {'product_launched':'tot_product_launched',
                           'active_product':'tot_product_existing'},inplace=True)
        sale_prop['total_products'] = sale_prop['tot_product_launched']+sale_prop['tot_product_existing']
        
        sale_prop['prop_new_pd'] = np.where((sale_prop['net_units_sold']==0)|(sale_prop['new_pd_sold']<=0)
    ,0,np.round(sale_prop['new_pd_sold']/sale_prop['net_units_sold'],2))

        sale_prop['prop_existing_pd'] = np.where((sale_prop['net_units_sold']==0)|(sale_prop['old_pd_sold']<=0)
    ,0,np.round(sale_prop['old_pd_sold']/sale_prop['net_units_sold'],2))
        
        # fill na
        sale_prop = sale_prop.fillna(0)
        
        # re arrange column
        sale_prop = sale_prop[['year_week_sold', 'year_month_id', 'id_shop', 'id_warehouse',
       'tot_product_existing', 'tot_product_launched', 'total_products',
       'new_pd_sold', 'old_pd_sold', 'prop_new_pd', 'prop_existing_pd']]

        return sale_prop

    @timeit
    def add_sale_prop(self,raw4: pd.DataFrame,sale_prop):

        sale_prop["tot_product_launched"] = sale_prop["tot_product_launched"].astype(float)
        sale_prop["tot_product_existing"] = sale_prop["tot_product_existing"].astype(float)
        sale_prop["total_products"] = sale_prop["total_products"].astype(float)

        sale_prop["prop_new_styles"] = np.round(sale_prop["tot_product_launched"] / sale_prop["total_products"], 2)

        sale_prop["prop_existing_styles"] = np.round(sale_prop["tot_product_existing"] / sale_prop["total_products"], 2)

        sale_prop.rename(
        columns={
            "total_products": "tot_products",
            "prop_new_pd": "prop_new_sold",
            "prop_existing_pd": "prop_existing_sold",
            "year_week_sold": "year_week_id",
        },
        inplace=True,)

        sale_prop = sale_prop[[
                "year_week_id",
                "id_shop",
                "prop_new_sold",
                "prop_existing_sold",
                "tot_product_launched",
                "tot_product_existing"]].drop_duplicates()

        sale_prop["id_shop"] = sale_prop["id_shop"].astype(str)
        # need to groupby first to make it at id_shop /week level
        sale_prop2 = (sale_prop.groupby(["year_week_id", "id_shop"]).mean().reset_index()) 

        sale_prop2["id_shop"] = sale_prop2["id_shop"].astype(int)  
        sale_prop2["year_week_id"] = sale_prop2["year_week_id"].astype(int)
        raw4['id_shop'] = raw4["id_shop"].astype(int)  
        raw4["year_week_id"] = raw4["year_week_id"].astype(int)
        
        raw5 = raw4.merge(sale_prop2, on=["year_week_id", "id_shop"], how="left")

        raw5["traffic_dist"] = np.where(raw5["week_id"] == "week1",
        raw5["tot_store_traffic"] * raw5["prop_new_sold"] / raw5["tot_product_launched"],
        raw5["tot_store_traffic"]* raw5["prop_existing_sold"]/ raw5["tot_product_existing"])
        del raw5["tot_store_traffic"]
        return raw5

    @timeit
    def add_retail_mkt_spend(self,raw5: pd.DataFrame):
        ##Retail marketing spend both historical and budget data
        sh = gc.open_by_key(RETAIL_MKT_SPEND_HIST_FILE_ID)
        wk = sh.worksheet(RETAIL_MKT_SPEND_HIST_SHEEET_NAME)
        retail_marketing_hist = pd.DataFrame.from_dict(wk.get_all_records())

        # retail marketing spend  budget
        sh = gc.open_by_key(RETAIL_MKT_SPEND_BUDGET_FILE_ID)
        wk = sh.worksheet(RETAIL_MKT_SPEND_BUDGET_SHEEET_NAME)
        retail_marketing_budget = pd.DataFrame.from_dict(wk.get_all_records())
        
        display(retail_marketing_budget)
        
        retail_marketing_budget = retail_marketing_budget.drop(columns=["May", "June", "July", "August"])

        # store mapping
        sh = gc.open_by_key(STORE_MAPPING_FILE_ID)
        wk = sh.worksheet(STORE_MAPPING_SHEET_NAME)
        store_mapping = pd.DataFrame.from_dict(wk.get_all_records())
        display(store_mapping)

        # monthid
        sql = """ select distinct month_id
                                ,month_name
                    from dwh.dim_calendar
                    order by month_id"""

        monthid = wr.athena.read_sql_query(sql, database="dwh")

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
            value_name="retail_mkt_spend")

        retail_budget_unpivot = retail_budget_unpivot.dropna()
        retail_budget_unpivot.rename({"Branch": "store_name"}, axis=1, inplace=True)
        retail_budget_unpivot.rename({"Abbreviation": "store_name_from_0421"}, axis=1, inplace=True)

        retail_budget_unpivot_1 = retail_budget_unpivot.merge(
            store_mapping[["id_shop", "erply_store_id", "store_name_from_0421"]],
            how="left",
            on="store_name_from_0421")

        retail_budget_unpivot_1 = retail_budget_unpivot_1.dropna()

        convert = {"id_shop": int, "erply_store_id": int}
        retail_budget_unpivot_1 = retail_budget_unpivot_1.astype(convert)

        retail_budget_unpivot_1["year"] = 2021
        retail_budget_unpivot_1 = retail_budget_unpivot_1.merge(monthid, how="left", on="month_name")

        convert = {"month_id": int}
        retail_budget_unpivot_1 = retail_budget_unpivot_1.astype(convert)

        retail_budget_unpivot_1.rename({"erply_store_id": "store_id"}, axis=1, inplace=True)
        retail_budget_unpivot_1["year_month_id"] = np.nan

        for index, row in retail_budget_unpivot_1.iterrows():
            retail_budget_unpivot_1.iloc[index, -1] = np.where(
                retail_budget_unpivot_1.iloc[index]["month_id"] < 10 ,
                str(retail_budget_unpivot_1.iloc[index]["year"])+ "0" + str(retail_budget_unpivot_1.iloc[index]["month_id"]),
                str(retail_budget_unpivot_1.iloc[index]["year"]) + str(retail_budget_unpivot_1.iloc[index]["month_id"]))

        retail_budget_unpivot_1 = retail_budget_unpivot_1.merge(
            retail_marketing_hist[["id_shop", "country"]].drop_duplicates(),
            how="left",
            on="id_shop")

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

        retail_marketing_spend = retail_marketing_spend[retail_marketing_spend["retail_mkt_spend"] > 0]

        print("Marketing data is loaded...")
        retail_marketing_spend2 = retail_marketing_spend[["year", "month_name", "store_id", "retail_mkt_spend"]]

        retail_marketing_spend2.rename(
            columns={"month_name": "month", "store_id": "id_store"}, inplace=True)

        retail_marketing_spend2["id_store"] = retail_marketing_spend2["id_store"].astype(str)
        retail_marketing_spend2["year"] = retail_marketing_spend2["year"].astype(int)
        retail_marketing_spend2["month"] = retail_marketing_spend2["month"].astype(str)
        raw5["month"] = raw5["month"].str.strip()
        raw5["id_store"] = raw5["id_store"].astype(str)
        raw5["year"] = raw5["year"].astype(int)

        raw6 = raw5.merge(retail_marketing_spend2, how="left", on=["year", "month", "id_store"])
        return raw6

    # fill 2018 data
    @timeit
    def fill_retail_mkt_spend(self,raw6: pd.DataFrame):
        # historical retail marketing spend
        sh = gc.open_by_key(RETAIL_MKT_SPEND_2018_FILE_ID)
        wk = sh.worksheet("2018")
        retail_marketing_2018 = pd.DataFrame.from_dict(wk.get_all_records())
        retail_marketing_2018["year"] = retail_marketing_2018["year"].astype(str)

        raw6.loc[(raw6["retail_mkt_spend"].isna()) & (raw6["year"] == "2018"), "retail_mkt_spend"] = (
        raw6[(raw6["retail_mkt_spend"].isna()) & (raw6["year"] == "2018")].merge(retail_marketing_2018, how="left",       on=["month", "year", "store_name"])[ "retail_mkt_spend_y"].values)

        raw6["retail_mkt_spend"] = raw6["retail_mkt_spend"].fillna(0)
        raw6["days_in_month"] = np.select(
            [
                (raw6["month"] == "January")
                | (raw6["month"] == "March")
                | (raw6["month"] == "May")
                | (raw6["month"] == "July")
                | (raw6["month"] == "August")
                | (raw6["month"] == "October")
                | (raw6["month"] == "December"),
                (raw6["month"] == "April")
                | (raw6["month"] == "June")
                | (raw6["month"] == "September")
                | (raw6["month"] == "November"),
                (raw6["month"] == "February") & (raw6["year"] != "2020"),
                (raw6["month"] == "January") & (raw6["year"] == "2020"),
            ],
            [31, 28, 30, 29],
        )

        raw6["retail_mkt_spend_per_week"] = raw6["retail_mkt_spend"] * 7 / raw6["days_in_month"]

        raw6["retail_mkt_spend_dist"] = np.where(raw6["week_id"] == "week1",
            raw6["retail_mkt_spend_per_week"]
            * raw6["prop_new_sold"]
            / raw6["tot_product_launched"],
            raw6["retail_mkt_spend_per_week"]
            * raw6["prop_existing_sold"]
            / raw6["tot_product_existing"])

        del raw6["prop_new_sold"]
        del raw6["prop_existing_sold"]
        del raw6["tot_product_launched"]
        del raw6["tot_product_existing"]
        del raw6["retail_mkt_spend_per_week"]
        del raw6["days_in_month"]
        del raw6["retail_mkt_spend"]
        return raw6
    
    


