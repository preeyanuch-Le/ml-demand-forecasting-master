import awswrangler as wr


# mkt and pv data
def query_info_data_my():
    sql = f"""
SELECT
-- product attributes
    dp.id_product

-- warehouse
    ,CASE WHEN fs.id_shop = 5 THEN 'ID'
          WHEN fs.id_shop = 2 THEN 'MY'
          WHEN fs.id_shop = 11 THEN 'MY'
          ELSE 'TH' END as warehouse
-- page view
    ,pv2.week_id
    ,pv2.pv_metric as pv
-- start date of week
    ,pv2.start_date_weekly

 -- marketing spend
    ,ms.marketing_spend


FROM dwh.dim_product dp


-- fact sales fs
LEFT JOIN dwh.fact_sales fs
    ON fs.id_product_attribute  = dp.id_product_attribute AND fs.id_product = dp.id_product


-- pageview
LEFT JOIN
(
select distinct dp.id_product,pv.warehouse,pv.week_id, dp.date_released_my,
case when pv.week_id = 'week1' then dp.date_released_my
          when pv.week_id = 'week2' then DATE_ADD('day', 7, dp.date_released_my)
          when pv.week_id = 'week3' then DATE_ADD('day', 14, dp.date_released_my)
          when pv.week_id = 'week4' then DATE_ADD('day', 21, dp.date_released_my)
          when pv.week_id = 'week5' then DATE_ADD('day', 28, dp.date_released_my)
          when pv.week_id = 'week6' then DATE_ADD('day', 35, dp.date_released_my)
          when pv.week_id = 'week7' then DATE_ADD('day', 42, dp.date_released_my)
          when pv.week_id = 'week8' then DATE_ADD('day', 49, dp.date_released_my)
          when pv.week_id = 'week9' then DATE_ADD('day', 56, dp.date_released_my)
          when pv.week_id = 'week10' then DATE_ADD('day', 63, dp.date_released_my)
          when pv.week_id = 'week11' then DATE_ADD('day', 70, dp.date_released_my)
          when pv.week_id = 'week12' then DATE_ADD('day', 77, dp.date_released_my)
          when pv.week_id = 'week13' then DATE_ADD('day', 84, dp.date_released_my)
          when pv.week_id = 'week14' then DATE_ADD('day', 91, dp.date_released_my)
          when pv.week_id = 'week15' then DATE_ADD('day', 98, dp.date_released_my)
          when pv.week_id = 'week16' then DATE_ADD('day', 105, dp.date_released_my)
          when pv.week_id = 'week17' then DATE_ADD('day', 112, dp.date_released_my)
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
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 0 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 6 THEN fpp.counter ELSE 0 END) as pv_f1w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
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
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 7 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 13 THEN fpp.counter ELSE 0 END) as pv_f2w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
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
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 14 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 20 THEN fpp.counter ELSE 0 END) as pv_f3w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
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
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 21 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 27 THEN fpp.counter ELSE 0 END) as pv_f4w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
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
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 28 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 34 THEN fpp.counter ELSE 0 END) as pv_f5w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
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
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 35 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 41 THEN fpp.counter ELSE 0 END) as pv_f6w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
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
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 42 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 48 THEN fpp.counter ELSE 0 END) as pv_f7w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
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
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 49 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 55 THEN fpp.counter ELSE 0 END) as pv_f8w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
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
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 56 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 62 THEN fpp.counter ELSE 0 END) as pv_f9w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week10' as week_id, pv_f10w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 63 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 69 THEN fpp.counter ELSE 0 END) as pv_f10w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week11' as week_id, pv_f11w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 70 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 76 THEN fpp.counter ELSE 0 END) as pv_f11w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week12' as week_id, pv_f12w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 77 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 83 THEN fpp.counter ELSE 0 END) as pv_f12w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week13' as week_id, pv_f13w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 84 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 90 THEN fpp.counter ELSE 0 END) as pv_f13w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week14' as week_id, pv_f14w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 91 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 97 THEN fpp.counter ELSE 0 END) as pv_f14w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week15' as week_id, pv_f15w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 98 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 104 THEN fpp.counter ELSE 0 END) as pv_f15w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week16' as week_id, pv_f16w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 105 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 111 THEN fpp.counter ELSE 0 END) as pv_f16w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
        LEFT JOIN dwh.dim_product dpp
        ON dpp.id_product = fpp.id_product
        GROUP BY 1,2) fpp2
union all
SELECT fpp2.id_product, fpp2.warehouse, 'week17' as week_id, pv_f17w as pv_metric
FROM
(SELECT
        fpp.id_product,
            CASE WHEN fpp.id_shop = 5 THEN 'ID'
              WHEN fpp.id_shop = 2 THEN 'MY'
              WHEN fpp.id_shop = 11 THEN 'MY'
              ELSE 'TH' END as warehouse,
              SUM(CASE WHEN DATE_DIFF('day',dpp.date_released ,fpp.date_view) >= 112 AND DATE_DIFF('day',dpp.date_released ,fpp.date_view) <= 118 THEN fpp.counter ELSE 0 END) as pv_f17w
        FROM (select id_product , id_shop, date_view, counter
        FROM dwh_snapshot.fact_product_pageview fpp ,
        (select max(date(snapshot_created_at)) max_snap_date from dwh_snapshot.fact_product_pageview)
        where date(snapshot_created_at) = max_snap_date) fpp
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

-- mkt spend for each warehouse ms
LEFT JOIN
    (SELECT
        date_format(fms.date, '%M' ) AS "release_month"
        ,EXTRACT(year FROM fms.date) as year
        ,CASE WHEN campaign_country = 'TH' THEN 'TH'
              WHEN campaign_country = 'SG' THEN 'MY'
              WHEN campaign_country = 'GL' THEN 'TH'
              WHEN campaign_country = 'PH' THEN 'MY'
              WHEN campaign_country = 'AU' THEN 'TH'
              WHEN campaign_country = 'HK' THEN 'TH'
              WHEN campaign_country = 'ID' THEN 'ID'
              WHEN campaign_country = 'MY' THEN 'MY' END as warehouse
        ,SUM(cost)/4 as marketing_spend from dwh.fact_marketing_spend fms
    GROUP BY 1,2,3
    ORDER BY 1) ms
ON ms.release_month  = date_format(pv2.start_date_weekly, '%M')
AND ms.warehouse = (CASE WHEN fs.id_shop = 5 THEN 'ID'
                                WHEN fs.id_shop = 2 THEN 'MY'
                                WHEN fs.id_shop = 11 THEN 'MY'
                                ELSE 'TH' END)
AND cast(ms.year as varchar) = date_format(pv2.start_date_weekly, '%Y')


WHERE dp.date_released_my BETWEEN cast('2017-01-01' as date) AND DATE_TRUNC('day', NOW() - INTERVAL '119' DAY)
AND fs.id_shop IN (2,11)
AND dp.parent_product_line NOT IN ('Free Gift')
AND dp.product_line NOT IN('Bags','3P Apparel','Shoes')
AND dp.henry_category_1 NOT IN ('Accessories','Bags','Bath&Body','Beverage Container','Cosmetics','Hair','Miscellaneous','Shoes','Skin Care','Stationery')
AND dp.product_cost_usd_first_order_date > 0
AND dp.brand IN ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio','Blackdog BKK')
AND dp.release_collection_name IS NOT NULL
AND fs.order_type IN ('Marketplace','Web','Android','iOS','Partner','Site to Store')
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3

        """
    info_data_my = wr.athena.read_sql_query(sql, database="dwh_snapshot")
    return info_data_my
