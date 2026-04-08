from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import numpy as np
import pandas as pd

# https://covid19.healthdata.org/thailand?view=infections-testing&tab=trend&test=infections

# inf_mean = Estimated infections are the number of people we estimate are infected with COVID-19 each day, including those not tested


def deploy_covid():
    covid_zip = (
        "https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip"
    )
    resp = urlopen(covid_zip)
    zipfile = ZipFile(BytesIO(resp.read()))
    file_name = [
        x for x in zipfile.namelist() if "data_download_file_best_masks_2021" in x
    ]
    covid = pd.read_csv(zipfile.open(file_name[0]))
    covid = covid[["date", "location_name", "inf_mean"]]
    covid = covid[
        covid["location_name"].isin(
            ["Thailand", "Malaysia", "Singapore", "Indonesia", "Philippines"]
        )
    ]
    covid["date"] = pd.to_datetime(covid["date"])

    covid["warehouse"] = np.select(
        [
            covid["location_name"] == "Thailand",
            covid["location_name"] == "Singapore",
            covid["location_name"] == "Malaysia",
            covid["location_name"] == "Philippines",
            covid["location_name"] == "Indonesia",
        ],
        ["TH", "MY", "MY", "TH", "ID"],
    )

    covid["inf_mean"] = np.select(
        [
            covid["location_name"] == "Thailand",
            covid["location_name"] == "Singapore",
            covid["location_name"] == "Malaysia",
            covid["location_name"] == "Philippines",
            covid["location_name"] == "Indonesia",
        ],
        [
            covid["inf_mean"] * 0.9,
            covid["inf_mean"] * 0.5,
            covid["inf_mean"] * 0.5,
            covid["inf_mean"] * 0.1,
            covid["inf_mean"],
        ],
    )

    df_group = covid.groupby(["date", "warehouse"])["inf_mean"].sum().reset_index()
    df_group.rename(columns={"date": "start_date_weekly"}, inplace=True)
    df_group["start_date_weekly"] = pd.to_datetime(df_group["start_date_weekly"])
    df_group.to_csv(
        "s3://hal-bi-bucket/data_science/dfm/online_clothing_v2/data/covid_projections_online.csv",
        index=False,
    )
