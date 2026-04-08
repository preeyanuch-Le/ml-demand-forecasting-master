from src.utils.function_lib import get_project_root

ROOT = get_project_root()

GDRIVE_SECRET = f'{ROOT}/secrets/gdrive_secret.json'
GSHEET_SECRET = f'{ROOT}/secrets/gsheets_secret.json'

S3_BUCKET = 'hal-bi-bucket'


# external data
# Store traffic sheet: store_traffic_newsc
# Retail marketing budget sheet: VM_budget
# https://docs.google.com/spreadsheets/d/10oYU4hw65uK-tNN2g2OhYkrMqCe9aqyf/edit#gid=1099449561
RETAIL_MKT_SPEND_FILE_ID = "10oYU4hw65uK-tNN2g2OhYkrMqCe9aqyf"
RETAIL_MKT_SPEND_FILE_NAME = "20211227 Traffic and VM expenses (to Data science).xlsx"

# Traffic distribution
# https://docs.google.com/spreadsheets/d/1Li_Qw14i76ObqYhA08vd9h3Zv8ggAi3v/edit#gid=1220357515
TRAFFIC_DIST_FILE_ID = "1Li_Qw14i76ObqYhA08vd9h3Zv8ggAi3v"
TRAFFIC_DIST_FILE_NAME =  "week_in_month_store_traffic.xlsx"
STORE_DIST_FILE_PATH = "s3://hal-bi-bucket/data_science/dfm/offline_clothing_v2/data/deploy_store_dist_offline.csv"

# Size distribution offline
# https://docs.google.com/spreadsheets/d/1i4OWgnNqZ70uFTkDCqgVSfIH270VNaNZ/edit#gid=1848370073
SIZE_DIST_OFFLINE_FILE_ID = "1i4OWgnNqZ70uFTkDCqgVSfIH270VNaNZ"
SIZE_DIST_OFFLINE_FILE_NAME = "size_dist_offline.xlsx"
SIZE_DIST_OFFLINE_CAT1_FILE = "s3://hal-bi-bucket/data_science/dfm/offline_clothing_v2/data/size_dist_offline_cat1.csv"

# store cluster
# # https://docs.google.com/spreadsheets/d/1qD0RLI049rjINi83waf96ixNe-6tsSU7/edit#gid=1848370073
# STORE_CLUSTER_FILE_ID = "1qD0RLI049rjINi83waf96ixNe-6tsSU7"
# STORE_CLUSTER_FILE_NAME = "store_clusters.xlsx"
STORE_CLUSTER_FILE = "s3://hal-bi-bucket/data_science/dfm/excel_files/Store Cluster_Final.xlsx"
STORE_CLUSTER_SHEET_NAME = "Lookup"
STORE_CLUSTER_FILE_DEPLOY = 's3://hal-bi-bucket/data_science/dfm/excel_files/Store_Cluster_new_unpivot.csv'

# covid
COVID_FILE_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/jhu/new_cases.csv"

##Retail marketing spend both historical and budget data
# historical retail marketing spend
# https://docs.google.com/spreadsheets/d/1HpegKJjxUFCBDwvpR29DUgcPZEAwgaUFmVBlIHt1844/edit#gid=0
RETAIL_MKT_SPEND_HIST_FILE_ID = "1HpegKJjxUFCBDwvpR29DUgcPZEAwgaUFmVBlIHt1844"
RETAIL_MKT_SPEND_HIST_SHEEET_NAME = "summary"


# retail marketing spend  budget
# https://docs.google.com/spreadsheets/d/1ygDhDd9rWv7R4aU8W7NkRMVTJ-N_bmi9JFg6ttqBkug/edit?ts=6070315b#gid=1436820854
RETAIL_MKT_SPEND_BUDGET_FILE_ID = "1ygDhDd9rWv7R4aU8W7NkRMVTJ-N_bmi9JFg6ttqBkug"
RETAIL_MKT_SPEND_BUDGET_SHEEET_NAME = "Data science"

# store mapping
# https://docs.google.com/spreadsheets/d/1TTM285yQXTxK_I0Kzb3MYq0XftBI6Wjf7WaBNq9traw/edit#gid=0
STORE_MAPPING_FILE_ID = "1TTM285yQXTxK_I0Kzb3MYq0XftBI6Wjf7WaBNq9traw"
STORE_MAPPING_SHEET_NAME = "Sheet1"

# historical retail marketing spend
# https://docs.google.com/spreadsheets/d/1HpegKJjxUFCBDwvpR29DUgcPZEAwgaUFmVBlIHt1844/edit#gid=0
RETAIL_MKT_SPEND_2018_FILE_ID = "1HpegKJjxUFCBDwvpR29DUgcPZEAwgaUFmVBlIHt1844"

# External Files for Online Deployment
LOOKBOOK_PV_DIST_FILE = "s3://hal-bi-bucket/data_science/dfm/online_clothing_v2/data/deploy_loobook_pv_dist.csv"
COUNTRY_DIST_FILE = "s3://hal-bi-bucket/data_science/dfm/online_clothing_v2/data/country_dist.csv"
SIZE_DIST_FILE = "s3://hal-bi-bucket/data_science/dfm/online_clothing_v2/data/deploy_size_dist.csv"
SIZE_DIST_MEDIAN_FILE = "s3://hal-bi-bucket/data_science/dfm/online_clothing_v2/data/deploy_size_dist_median.csv"


