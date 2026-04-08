from datetime import date

from dateutil.relativedelta import relativedelta
from src.utils.pomelo_utils import Hal


def deply_country_dist():
    end_date = date.today().strftime("%Y-%m-%d")
    six_months = date.today() - relativedelta(months=+6)
    start_date = six_months.strftime("%Y-%m-%d")

    hal = Hal()

    sql = f"""
     SELECT
                CASE WHEN pm.id_shop = 5 THEN 'ID'
                  WHEN pm.id_shop = 2 THEN 'MY'
                  WHEN pm.id_shop = 11 THEN 'MY'
                  ELSE 'TH' END as warehouse
        ,SUM(on_net_sales_f60d) online_units_f60d
    FROM dwh.fact_product_metrics pm
    LEFT JOIN dwh.dim_product dp
    ON pm.id_product_attribute  = dp.id_product_attribute
    WHERE dp.date_released BETWEEN '{start_date}' AND '{end_date}'
    AND dp.brand in ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio','Pomelo Man','5th Avenue Eyewear')
    AND dp.release_collection_name IS NOT NULL
    AND dp."size" IN ('25', '26', '27', '28', '30', '32', '34', 'L', 'M', 'S', 'XL', 'XS', 'XXL')
    GROUP BY 1
    """

    country_dist = hal.get_pandas_df(sql)

    country_dist["country_dist"] = (
        country_dist["online_units_f60d"] / country_dist["online_units_f60d"].sum()
    )

    country_dist.to_csv(
        "s3://hal-bi-bucket/data_science/dfm/online_clothing_v2/data/country_dist.csv",
        index=False,
    )
