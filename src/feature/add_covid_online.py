from datetime import timedelta

import pandas as pd


def add_covid(raw):
    url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/jhu/new_cases.csv"
    df = pd.read_csv(url, error_bad_lines=False)

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
        raw_empty = pd.DataFrame(
            columns=["start_date_weekly", "warehouse", "covid_cases"]
        )
        raw_country = raw[raw["warehouse"] == i]
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
    raw["start_date_weekly"] = pd.to_datetime(raw["start_date_weekly"])
    raw_w_covid = raw.merge(
        raw_covid, how="left", on=["start_date_weekly", "warehouse"]
    )

    return raw_w_covid
