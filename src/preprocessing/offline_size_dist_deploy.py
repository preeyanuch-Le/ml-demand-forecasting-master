import io

import numpy as np
import pandas as pd

# authenticate your identity using the JSON credentials and provide access to your Google Drive #####
from apiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from httplib2 import Http
from oauth2client import client, file, tools

"""
obj = lambda: None
lmao = {
    "auth_host_name": "localhost",
    "noauth_local_webserver": "store_true",
    "auth_host_port": [8080, 8090],
    "logging_level": "ERROR",
}
for k, v in lmao.items():
    setattr(obj, k, v)

# authorization boilerplate code
SCOPES = "https://www.googleapis.com/auth/drive.readonly"
store = file.Storage("token.json")
creds = store.get()
# creds = None
# The following will give you a link if token.json does not exist, the link allows the user to give this app permission
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets(
        "/home/ec2-user/SageMaker/ml-demand-forecasting/secrets/quick-cogency-314404-6a4762e0bc8d.json",
        SCOPES,
    )
    creds = tools.run_flow(flow, store, obj)
 """


def resize_dist(final_data):

    DRIVE = discovery.build("drive", "v3", http=creds.authorize(Http()))
    # if you get the shareable link, the link contains this id, replace the file_id below
    # https://docs.google.com/spreadsheets/d/1i4OWgnNqZ70uFTkDCqgVSfIH270VNaNZ/edit#gid=1848370073
    file_id = "1i4OWgnNqZ70uFTkDCqgVSfIH270VNaNZ"
    file_name = "size_dist_offline.xlsx"
    request = DRIVE.files().get_media(fileId=file_id)
    fh = io.FileIO(
        f"/home/ec2-user/SageMaker/business-intelligence-notebooks/dfm_clothing/offline_dfm_v2/{file_name}",
        mode="w",
    )
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))

    size_dist_offline = pd.read_excel(
        f"/home/ec2-user/SageMaker/business-intelligence-notebooks/dfm_clothing/offline_dfm_v2/{file_name}",
        engine="openpyxl",
        sheet_name="Sheet1",
    )

    size_dist_offline = pd.read_csv(
        "s3://hal-bi-bucket/data_science/dfm/offline_clothing_v2/data/size_dist_offline_cat1.csv"
    )

    size_dist_offline.rename(
        columns={
            "Product Sub Product Line": "sub_product_line",
            "Product Henry Category 1": "category_1",
            "Product Size": "size",
            "Product Metrics ID Shop": "id_shop",
            "Product Metrics Offline Net Sales F45d": "net_sales_f45",
        },
        inplace=True,
    )

    size_dist_offline["id_shop_name"] = np.select(
        [
            size_dist_offline["id_shop"] == 1,
            size_dist_offline["id_shop"] == 2,
            size_dist_offline["id_shop"] == 5,
            size_dist_offline["id_shop"] == 11,
        ],
        ["TH", "SG", "ID", "MY"],
    )

    size_dist_offline = size_dist_offline[
        ["sub_product_line", "category_1", "size", "id_shop_name", "net_sales_f45"]
    ]
    size_dist_offline["net_sales_f45"] = size_dist_offline["net_sales_f45"].str.replace(
        ",", ""
    )
    size_dist_offline["net_sales_f45"] = size_dist_offline["net_sales_f45"].astype(int)
    size_dist_offline2 = size_dist_offline.groupby(
        ["sub_product_line", "category_1", "id_shop_name"]
    )["net_sales_f45"].sum()

    size_dist_offline2 = size_dist_offline2.reset_index()
    size_dist_offline2.rename(
        columns={"net_sales_f45": "total_sale_by_size"}, inplace=True
    )

    size_dist_offline3 = size_dist_offline.merge(
        size_dist_offline2,
        how="left",
        on=["sub_product_line", "category_1", "id_shop_name"],
    )

    size_dist_offline3["size_dist"] = (
        size_dist_offline3["net_sales_f45"] / size_dist_offline3["total_sale_by_size"]
    )
    size_dist_offline3.dropna(inplace=True)
    size_dist_offline3["size"] = size_dist_offline3["size"].astype(str)

    final_data["size"] = final_data["size"].astype(str)
    final_data = final_data.merge(
        size_dist_offline,
        how="left",
        on=["sub_product_line", "category_1", "size", "id_shop_name"],
    )

    final_data_group = (
        final_data.groupby(
            [
                "id_product",
                "id_shop_name",
                "store_cluster",
                "sub_product_line",
                "category_1",
                "week_id",
            ]
        )["pred"]
        .sum()
        .reset_index()
    )
    size_temp = final_data[
        ["id_product", "id_product_attribute", "size"]
    ].drop_duplicates()
    final_data_group2 = final_data_group.merge(size_temp, how="left", on=["id_product"])
    size_dist_offline3 = size_dist_offline3[
        ["sub_product_line", "category_1", "size", "id_shop_name", "size_dist"]
    ]
    final_data_group3 = final_data_group2.merge(
        size_dist_offline3,
        how="left",
        on=["sub_product_line", "category_1", "id_shop_name", "size"],
    )

    final_data_group4 = final_data_group3.merge(
        size_dist_offline3, how="left", on=["sub_product_line", "id_shop_name", "size"]
    )

    final_data_group3["size_dist"] = final_data_group3["size_dist"].fillna(
        final_data_group3.merge(
            final_data_group4,
            on=["sub_product_line", "id_shop_name", "size"],
            how="left",
        )["size_dist_y"]
    )
    final_data_group3["adjusted_pred"] = (
        final_data_group3["pred"] * final_data_group3["size_dist"]
    )

    final_list_products = final_data[
        [
            "master_style_id",
            "id_product",
            "id_product_attribute",
            "warehouse",
            "id_shop_name",
            "store_cluster",
            "week_id",
            "released_collection_name",
            "product_cost_usd",
            "size",
            "sub_product_line",
            "category_1",
            "category_2",
            "category_3",
            "color",
            "simple_color",
            "original_price_usd",
            "fabric",
            "giveaway",
            "style",
            "sleeve",
            "pattern",
            "sleeve_style",
            "neckline",
            "shape",
            "rise",
        ]
    ].drop_duplicates()

    final_list = final_list_products.merge(
        final_data_group3,
        how="left",
        on=[
            "id_product_attribute",
            "id_product",
            "size",
            "id_shop_name",
            "store_cluster",
            "week_id",
        ],
    )

    return final_list
