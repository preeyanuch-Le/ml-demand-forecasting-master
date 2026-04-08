from datetime import timedelta

import pandas as pd


def add_covid(raw):
    url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/jhu/new_cases.csv"
    df = pd.read_csv(url, error_bad_lines=False)

    filtered_data = df[["date", "Thailand", "Malaysia", "Singapore", "Indonesia"]]
    filtered_data.fillna(0, inplace=True)
    filtered_data["date"] = pd.to_datetime(filtered_data["date"])

    # daily cavid case for each country
    country_data = pd.DataFrame(columns=["date", "TH", "MY", "SG", "ID"])
    country_data["date"] = filtered_data["date"]
    country_data["TH"] = filtered_data["Thailand"]
    country_data["ID"] = filtered_data["Indonesia"]
    country_data["MY"] = filtered_data["Malaysia"]
    country_data["SG"] = filtered_data["Singapore"]

    # unique date_released for each country
    raw_covid = pd.DataFrame()
    for i in ["TH", "MY", "SG", "ID"]:
        raw_empty = pd.DataFrame(
            columns=["start_date_weekly", "id_shop_name", "covid_cases"]
        )
        raw_country = raw[raw["id_shop_name"] == i]
        raw_empty["start_date_weekly"] = raw_country["start_date_weekly"].unique()
        raw_empty["id_shop_name"] = i
        raw_covid = raw_covid.append(raw_empty)

    raw_covid = raw_covid.sort_values(by="start_date_weekly").reset_index(drop=True)
    raw_covid["start_date_weekly"] = pd.to_datetime(raw_covid["start_date_weekly"])

    # adding no.of covid cases for each week (the first 7days since start_date_weekly)
    for i in range(0, len(raw_covid)):
        for j in range(0, len(country_data)):
            for k in ["TH", "MY", "SG", "ID"]:
                if (raw_covid["start_date_weekly"][i] == country_data["date"][j]) & (
                    raw_covid["id_shop_name"][i] == k
                ):
                    raw_covid["covid_cases"][i] = country_data[
                        (country_data["date"] >= raw_covid["start_date_weekly"][i])
                        & (
                            country_data["date"]
                            < (
                                raw_covid["start_date_weekly"][i] + timedelta(days=7)
                            ).strftime("%Y-%m-%d")
                        )
                    ][k].sum()

    raw_covid.fillna(0, inplace=True)
    raw_covid = raw_covid[raw_covid["start_date_weekly"] != 0]
    raw["start_date_weekly"] = pd.to_datetime(raw["start_date_weekly"])
    raw_covid["start_date_weekly"] = pd.to_datetime(raw_covid["start_date_weekly"])

    print(raw_covid["start_date_weekly"].dtype)
    print(raw["start_date_weekly"].dtype)
    raw_w_covid = raw.merge(
        raw_covid, how="left", on=["start_date_weekly", "id_shop_name"]
    )

    return raw_w_covid
