from pomelo.utils import Hal


def query_data_th():
    hal = Hal()
    sql = """
SELECT
-- product attributes
    dp.id_product_attribute
    ,dp.id_product
    ,dp.date_released
    ,dp.size
-- warehouse
    ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
-- week_id
    ,pv2.week_id

-- transaction data
    ,fsn2.net_units_sold

FROM dwh.dim_product dp


-- fact sales fs
LEFT JOIN dwh.fact_sales fs
    ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product

-- pageview
LEFT JOIN
(
select distinct dp.id_product,pv.warehouse,pv.week_id, dp.date_released,
case when pv.week_id = 'week1' then dp.date_released
          when pv.week_id = 'week2' then DATEADD(day, 7, dp.date_released)
          when pv.week_id = 'week3' then DATEADD(day, 14, dp.date_released)
          when pv.week_id = 'week4' then DATEADD(day, 21, dp.date_released)
          when pv.week_id = 'week5' then DATEADD(day, 28, dp.date_released)
          when pv.week_id = 'week6' then DATEADD(day, 35, dp.date_released)
          when pv.week_id = 'week7' then DATEADD(day, 42, dp.date_released)
          when pv.week_id = 'week8' then DATEADD(day, 49, dp.date_released)
          when pv.week_id = 'week9' then DATEADD(day, 56, dp.date_released)
    end AS start_date_weekly

,pv.pv_metric
from
(
SELECT fpp2.id_product, fpp2.warehouse, 'week1' as week_id, pv_f1w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATEDIFF(days,dpp.date_released ,fpp.date_view) >= 0 AND DATEDIFF(days,dpp.date_released ,fpp.date_view) <= 6 THEN fpp.counter ELSE 0 END) as pv_f1w
        FROM dwh.fact_product_pageview fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week2' as week_id, pv_f2w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATEDIFF(days,dpp.date_released ,fpp.date_view) >= 7 AND DATEDIFF(days,dpp.date_released ,fpp.date_view) <= 13 THEN fpp.counter ELSE 0 END) as pv_f2w
        FROM dwh.fact_product_pageview fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week3' as week_id, pv_f3w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATEDIFF(days,dpp.date_released ,fpp.date_view) >= 14 AND DATEDIFF(days,dpp.date_released ,fpp.date_view) <= 20 THEN fpp.counter ELSE 0 END) as pv_f3w
        FROM dwh.fact_product_pageview fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week4' as week_id, pv_f4w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATEDIFF(days,dpp.date_released ,fpp.date_view) >= 21 AND DATEDIFF(days,dpp.date_released ,fpp.date_view) <= 27 THEN fpp.counter ELSE 0 END) as pv_f4w
        FROM dwh.fact_product_pageview fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
 union all
SELECT fpp2.id_product, fpp2.warehouse, 'week5' as week_id, pv_f5w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATEDIFF(days,dpp.date_released ,fpp.date_view) >= 28 AND DATEDIFF(days,dpp.date_released ,fpp.date_view) <= 34 THEN fpp.counter ELSE 0 END) as pv_f5w
        FROM dwh.fact_product_pageview fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week6' as week_id, pv_f6w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATEDIFF(days,dpp.date_released ,fpp.date_view) >= 35 AND DATEDIFF(days,dpp.date_released ,fpp.date_view) <= 41 THEN fpp.counter ELSE 0 END) as pv_f6w
        FROM dwh.fact_product_pageview fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week7' as week_id, pv_f7w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATEDIFF(days,dpp.date_released ,fpp.date_view) >= 42 AND DATEDIFF(days,dpp.date_released ,fpp.date_view) <= 48 THEN fpp.counter ELSE 0 END) as pv_f7w
        FROM dwh.fact_product_pageview fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week8' as week_id, pv_f8w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATEDIFF(days,dpp.date_released ,fpp.date_view) >= 49 AND DATEDIFF(days,dpp.date_released ,fpp.date_view) <= 55 THEN fpp.counter ELSE 0 END) as pv_f8w
        FROM dwh.fact_product_pageview fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week9' as week_id, pv_f9w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATEDIFF(days,dpp.date_released ,fpp.date_view) >= 56 AND DATEDIFF(days,dpp.date_released ,fpp.date_view) <= 62 THEN fpp.counter ELSE 0 END) as pv_f9w
        FROM dwh.fact_product_pageview fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
        ) pv

        LEFT JOIN dwh.dim_product dp
        on dp.id_product = pv.id_product
) pv2
        on pv2.id_product = dp.id_product
        AND pv2.warehouse = (CASE WHEN fs.id_shop = 5 THEN 'ID'
                        WHEN fs.id_shop = 2 THEN 'MY'
                        WHEN fs.id_shop = 11 THEN 'MY'
                        ELSE 'TH' END)

-- transaction data
LEFT JOIN
(
SELECT fsn.id_product_attribute,  fsn.id_product, fsn.warehouse, 'week1' as week_id, net_units_sold_f1w as net_units_sold, gross_units_sold_f1w as gross_units_sold, gross_revenue_usd_f1w as gross_revenue_usd
      ,revenue_usd_f1w as revenue_usd, item_discount_usd_f1w as item_discount_usd, voucher_discount_usd_f1w as voucher_discount_usd
FROM
(SELECT  dp.id_product_attribute
        ,dp.id_product
        ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=0 AND DATEDIFF(day,dp.date_released,fs.order_date)<=6 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_f1w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=0 AND DATEDIFF(day,dp.date_released,fs.order_date)<=6 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_f1w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=0 AND DATEDIFF(day,dp.date_released,fs.order_date)<=6 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_f1w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=0 AND DATEDIFF(day,dp.date_released,fs.order_date)<=6 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_f1w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=0 AND DATEDIFF(day,dp.date_released,fs.order_date)<=6 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_f1w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=0 AND DATEDIFF(day,dp.date_released,fs.order_date)<=6
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_f1w
FROM dwh.fact_sales fs
LEFT JOIN dwh.dim_product dp
ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product
Group by 1,2,3) fsn
union all
SELECT fsn.id_product_attribute,  fsn.id_product, fsn.warehouse, 'week2' as week_id, net_units_sold_f2w as net_units_sold, gross_units_sold_f2w as gross_units_sold, gross_revenue_usd_f2w as gross_revenue_usd
      ,revenue_usd_f2w as revenue_usd, item_discount_usd_f2w as item_discount_usd, voucher_discount_usd_f2w as voucher_discount_usd
FROM
(SELECT  dp.id_product_attribute
        ,dp.id_product
        ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=7 AND DATEDIFF(day,dp.date_released,fs.order_date)<=13 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_f2w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=7 AND DATEDIFF(day,dp.date_released,fs.order_date)<=13 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_f2w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=7 AND DATEDIFF(day,dp.date_released,fs.order_date)<=13 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_f2w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=7 AND DATEDIFF(day,dp.date_released,fs.order_date)<=13 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_f2w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=7 AND DATEDIFF(day,dp.date_released,fs.order_date)<=13 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_f2w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=7 AND DATEDIFF(day,dp.date_released,fs.order_date)<=13
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_f2w
FROM dwh.fact_sales fs
LEFT JOIN dwh.dim_product dp
ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product
Group by 1,2,3) fsn
union all
SELECT fsn.id_product_attribute,  fsn.id_product, fsn.warehouse, 'week3' as week_id, net_units_sold_f3w as net_units_sold, gross_units_sold_f3w as gross_units_sold, gross_revenue_usd_f3w as gross_revenue_usd
      ,revenue_usd_f3w as revenue_usd, item_discount_usd_f3w as item_discount_usd, voucher_discount_usd_f3w as voucher_discount_usd
FROM
(SELECT  dp.id_product_attribute
        ,dp.id_product
        ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=14 AND DATEDIFF(day,dp.date_released,fs.order_date)<=20 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_f3w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=14 AND DATEDIFF(day,dp.date_released,fs.order_date)<=20 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_f3w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=14 AND DATEDIFF(day,dp.date_released,fs.order_date)<=20 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_f3w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=14 AND DATEDIFF(day,dp.date_released,fs.order_date)<=20 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_f3w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=14 AND DATEDIFF(day,dp.date_released,fs.order_date)<=20 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_f3w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=14 AND DATEDIFF(day,dp.date_released,fs.order_date)<=20
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_f3w
FROM dwh.fact_sales fs
LEFT JOIN dwh.dim_product dp
ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product
Group by 1,2,3) fsn
union all
SELECT fsn.id_product_attribute,  fsn.id_product, fsn.warehouse, 'week4' as week_id, net_units_sold_f4w as net_units_sold, gross_units_sold_f4w as gross_units_sold, gross_revenue_usd_f4w as gross_revenue_usd
      ,revenue_usd_f4w as revenue_usd, item_discount_usd_f4w as item_discount_usd, voucher_discount_usd_f4w as voucher_discount_usd
FROM
(SELECT  dp.id_product_attribute
        ,dp.id_product
        ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=21 AND DATEDIFF(day,dp.date_released,fs.order_date)<=27 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_f4w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=21 AND DATEDIFF(day,dp.date_released,fs.order_date)<=27 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_f4w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=21 AND DATEDIFF(day,dp.date_released,fs.order_date)<=27 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_f4w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=21 AND DATEDIFF(day,dp.date_released,fs.order_date)<=27 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_f4w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=21 AND DATEDIFF(day,dp.date_released,fs.order_date)<=27 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_f4w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=21 AND DATEDIFF(day,dp.date_released,fs.order_date)<=27
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_f4w
FROM dwh.fact_sales fs
LEFT JOIN dwh.dim_product dp
ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product
Group by 1,2,3) fsn
union all
SELECT fsn.id_product_attribute,  fsn.id_product, fsn.warehouse, 'week5' as week_id, net_units_sold_f5w as net_units_sold, gross_units_sold_f5w as gross_units_sold, gross_revenue_usd_f5w as gross_revenue_usd
      ,revenue_usd_f5w as revenue_usd, item_discount_usd_f5w as item_discount_usd, voucher_discount_usd_f5w as voucher_discount_usd
FROM
(SELECT  dp.id_product_attribute
        ,dp.id_product
        ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=28 AND DATEDIFF(day,dp.date_released,fs.order_date)<=34 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_f5w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=28 AND DATEDIFF(day,dp.date_released,fs.order_date)<=34 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_f5w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=28 AND DATEDIFF(day,dp.date_released,fs.order_date)<=34 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_f5w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=28 AND DATEDIFF(day,dp.date_released,fs.order_date)<=34 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_f5w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=28 AND DATEDIFF(day,dp.date_released,fs.order_date)<=34 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_f5w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=28 AND DATEDIFF(day,dp.date_released,fs.order_date)<=34
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_f5w
FROM dwh.fact_sales fs
LEFT JOIN dwh.dim_product dp
ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product
Group by 1,2,3) fsn
union all
SELECT fsn.id_product_attribute,  fsn.id_product, fsn.warehouse, 'week6' as week_id, net_units_sold_f6w as net_units_sold, gross_units_sold_f6w as gross_units_sold, gross_revenue_usd_f6w as gross_revenue_usd
      ,revenue_usd_f6w as revenue_usd, item_discount_usd_f6w as item_discount_usd, voucher_discount_usd_f6w as voucher_discount_usd
FROM
(SELECT  dp.id_product_attribute
        ,dp.id_product
        ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=35 AND DATEDIFF(day,dp.date_released,fs.order_date)<=41 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_f6w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=35 AND DATEDIFF(day,dp.date_released,fs.order_date)<=41 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_f6w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=35 AND DATEDIFF(day,dp.date_released,fs.order_date)<=41 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_f6w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=35 AND DATEDIFF(day,dp.date_released,fs.order_date)<=41 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_f6w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=35 AND DATEDIFF(day,dp.date_released,fs.order_date)<=41 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_f6w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=35 AND DATEDIFF(day,dp.date_released,fs.order_date)<=41
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_f6w
FROM dwh.fact_sales fs
LEFT JOIN dwh.dim_product dp
ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product
Group by 1,2,3) fsn
union all
SELECT fsn.id_product_attribute,  fsn.id_product, fsn.warehouse, 'week7' as week_id, net_units_sold_f7w as net_units_sold, gross_units_sold_f7w as gross_units_sold, gross_revenue_usd_f7w as gross_revenue_usd
      ,revenue_usd_f7w as revenue_usd, item_discount_usd_f7w as item_discount_usd, voucher_discount_usd_f7w as voucher_discount_usd
FROM
(SELECT  dp.id_product_attribute
        ,dp.id_product
        ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=42 AND DATEDIFF(day,dp.date_released,fs.order_date)<=48 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_f7w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=42 AND DATEDIFF(day,dp.date_released,fs.order_date)<=48 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_f7w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=42 AND DATEDIFF(day,dp.date_released,fs.order_date)<=48 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_f7w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=42 AND DATEDIFF(day,dp.date_released,fs.order_date)<=48 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_f7w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=42 AND DATEDIFF(day,dp.date_released,fs.order_date)<=48 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_f7w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=42 AND DATEDIFF(day,dp.date_released,fs.order_date)<=48
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_f7w
FROM dwh.fact_sales fs
LEFT JOIN dwh.dim_product dp
ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product
Group by 1,2,3) fsn
union all
SELECT fsn.id_product_attribute,  fsn.id_product, fsn.warehouse, 'week8' as week_id, net_units_sold_f8w as net_units_sold, gross_units_sold_f8w as gross_units_sold, gross_revenue_usd_f8w as gross_revenue_usd
      ,revenue_usd_f8w as revenue_usd, item_discount_usd_f8w as item_discount_usd, voucher_discount_usd_f8w as voucher_discount_usd
FROM
(SELECT  dp.id_product_attribute
        ,dp.id_product
        ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=49 AND DATEDIFF(day,dp.date_released,fs.order_date)<=55 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_f8w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=49 AND DATEDIFF(day,dp.date_released,fs.order_date)<=55 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_f8w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=49 AND DATEDIFF(day,dp.date_released,fs.order_date)<=55 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_f8w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=49 AND DATEDIFF(day,dp.date_released,fs.order_date)<=55 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_f8w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=49 AND DATEDIFF(day,dp.date_released,fs.order_date)<=55 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_f8w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=49 AND DATEDIFF(day,dp.date_released,fs.order_date)<=55
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_f8w
FROM dwh.fact_sales fs
LEFT JOIN dwh.dim_product dp
ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product
Group by 1,2,3) fsn
union all
SELECT fsn.id_product_attribute,  fsn.id_product, fsn.warehouse, 'week9' as week_id, net_units_sold_f9w as net_units_sold, gross_units_sold_f9w as gross_units_sold, gross_revenue_usd_f9w as gross_revenue_usd
      ,revenue_usd_f9w as revenue_usd, item_discount_usd_f9w as item_discount_usd, voucher_discount_usd_f9w as voucher_discount_usd
FROM
(SELECT  dp.id_product_attribute
        ,dp.id_product
        ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=56 AND DATEDIFF(day,dp.date_released,fs.order_date)<=62 THEN fs.net_units_sold ELSE 0 END) as net_units_sold_f9w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=56 AND DATEDIFF(day,dp.date_released,fs.order_date)<=62 THEN fs.gross_units_sold ELSE 0 END) as gross_units_sold_f9w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=56 AND DATEDIFF(day,dp.date_released,fs.order_date)<=62 THEN fs.gross_revenue_usd ELSE 0 END) as gross_revenue_usd_f9w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=56 AND DATEDIFF(day,dp.date_released,fs.order_date)<=62 THEN fs.revenue_usd ELSE 0 END) as revenue_usd_f9w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=56 AND DATEDIFF(day,dp.date_released,fs.order_date)<=62 THEN fs.item_discount_usd ELSE 0 END) as item_discount_usd_f9w
    ,SUM(CASE WHEN DATEDIFF(day,dp.date_released,fs.order_date)>=56 AND DATEDIFF(day,dp.date_released,fs.order_date)<=62
    AND voucher_discount_pomelo_share_usd <> 0 THEN fs.voucher_discount_pomelo_share_usd ELSE fs.voucher_discount_pomelo_usd END) as voucher_discount_usd_f9w
FROM dwh.fact_sales fs
LEFT JOIN dwh.dim_product dp
ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product
Group by 1,2,3) fsn
) fsn2

ON fsn2.id_product_attribute = dp.id_product_attribute
AND fsn2.week_id = pv2.week_id
AND fsn2.warehouse = (CASE WHEN fs.id_shop = 5 THEN 'ID'
                         WHEN fs.id_shop = 2 THEN 'MY'
                         WHEN fs.id_shop = 11 THEN 'MY'
                         ELSE 'TH' END)

WHERE fs.order_date BETWEEN date(getdate()-180) AND date(getdate())
AND fs.id_shop NOT IN(2,11,5)
AND dp.parent_product_line NOT IN ('Free Gift')
AND dp.product_line NOT IN('Bags','3P Apparel','Shoes')
AND dp.henry_category_1 NOT IN ('Accessories','Bags','Bath&Body','Beverage Container','Cosmetics','Hair','Miscellaneous','Shoes','Skin Care','Stationery')
AND dp.product_cost_usd_first_order_date > 0
AND dp.brand IN ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio','Blackdog BKK')
AND dp.release_collection_name IS NOT NULL
AND fs.order_type IN ('Marketplace','Web','Android','iOS','Partner','Site to Store')
GROUP BY 1,2,3,4,5,6,7
-- ORDER BY week_id,warehouse
ORDER BY 1

        """
    raw = hal.get_pandas_df(sql)
    return raw
