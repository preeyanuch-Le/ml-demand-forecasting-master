import awswrangler as wr


def query_du_strategy_dfm_offline(start_date, end_date):
    sql = """
            WITH
                FACT_SALES_OFF  AS  (
                    SELECT
                        ID_PRODUCT
                    ,   ID_SHOP
                    ,   ID_PROMOTION
                    , CASE
                          WHEN ID_STORE = '5a7c011b6878b592402d76bd' THEN '371'
                          WHEN ID_STORE = '5b9890f4d49bab0743f5689e' THEN '241'
                          WHEN ID_STORE = '5b989110a9afac06f24b80ce' THEN '371'
                          WHEN ID_STORE = '5b55a681868e636a8ab1eaa2' THEN '261'
                          WHEN ID_STORE = '5bbedd0bc3cef17af5b1a3ab' THEN '11'
                          WHEN ID_STORE = 'temp01' THEN '211'
                          WHEN ID_STORE = 'temp02' THEN '221'
                          WHEN ID_STORE = '5bb2e6ac1e5abf2252df039c' THEN '251'
                          ELSE ID_STORE
                      END AS ID_STORE ,
                      CASE
                          WHEN ID_SHOP = '5' THEN 'ID'
                          WHEN ID_SHOP = '2' THEN 'MY'
                          WHEN ID_SHOP = '11' THEN 'MY'
                          ELSE 'TH'
                      END AS WAREHOUSE
                    ,   PROMOTION_NAME
                    ,   GROSS_REVENUE_USD
                    ,   REVENUE_USD
                    ,   DATE(TRANSACTION_TIME)      TRANSACTION_TIME
                    , CASE
                          WHEN LOWER(PROMOTION_NAME) LIKE '%5.5%'
                               OR LOWER(PROMOTION_NAME) LIKE '%6.6%'
                               OR LOWER(PROMOTION_NAME) LIKE '%7.7%'
                               OR LOWER(PROMOTION_NAME) LIKE '%8.8%'
                               OR LOWER(PROMOTION_NAME) LIKE '%9.9%'
                               OR LOWER(PROMOTION_NAME) LIKE '%10.10%'
                               OR LOWER(PROMOTION_NAME) LIKE '%11.11%'
                               OR LOWER(PROMOTION_NAME) LIKE '%12.12%'
                               OR LOWER(PROMOTION_NAME) LIKE '%halloween%'
                               OR LOWER(PROMOTION_NAME) LIKE '%july%'
                               OR LOWER(PROMOTION_NAME) LIKE '%birthday%'
                               OR LOWER(PROMOTION_NAME) LIKE '%birdthday%'
                               OR LOWER(PROMOTION_NAME) LIKE '%black friday%' THEN 1
                          ELSE 0
                      END AS IS_MEGA_CAMPAIGN_ORDER
                    , CASE
                          WHEN LOWER(PROMOTION_NAME) LIKE '%5.5%'
                               OR LOWER(PROMOTION_NAME) LIKE '%6.6%'
                               OR LOWER(PROMOTION_NAME) LIKE '%7.7%'
                               OR LOWER(PROMOTION_NAME) LIKE '%8.8%'
                               OR LOWER(PROMOTION_NAME) LIKE '%9.9%'
                               OR LOWER(PROMOTION_NAME) LIKE '%10.10%'
                               OR LOWER(PROMOTION_NAME) LIKE '%11.11%'
                               OR LOWER(PROMOTION_NAME) LIKE '%12.12%' THEN 'mega'
                          WHEN LOWER(PROMOTION_NAME) LIKE '%birthday%'
                               OR LOWER(PROMOTION_NAME) LIKE '%birdthday%' THEN 'pml birthday'
                          WHEN LOWER(PROMOTION_NAME) LIKE '%black friday%' THEN 'black friday'
                          WHEN LOWER(PROMOTION_NAME) LIKE '%halloween%' THEN 'halloween'
                          ELSE 'normal_drop'
                      END AS CAMPAIGN_NAME
                    FROM
                        DWH_SNAPSHOT.FACT_SALES_OFFLINE FOS
                    ,   (
                            SELECT
                                MAX(DATE(SNAPSHOT_CREATED_AT))  MAX_SNAP_DATE
                            FROM
                                DWH_SNAPSHOT.FACT_SALES_OFFLINE
                        )
                    WHERE
                        DATE(SNAPSHOT_CREATED_AT)   =   MAX_SNAP_DATE
                    AND PRODUCT_QUANTITY            >   0
                    AND TRANSACTION_TYPE            =   'Sale'
                )
            ,   DIM_PRODUCT     AS  (
                    SELECT
                        DISTINCT
                        ID_PRODUCT
                    ,  CASE
                 WHEN LOWER(PRODUCT_LINE) = '-' THEN 'none'
                 ELSE LOWER(PRODUCT_LINE)
             END AS SUB_PRODUCT_LINE ,
             CASE
                 WHEN LOWER(HENRY_CATEGORY_1) = '-' THEN 'none'
                 ELSE LOWER(HENRY_CATEGORY_1)
             END AS HENRY_CATEGORY_1
                    ,   DATE_RELEASED
                    ,   DATE_RELEASED_ID
                    ,   DATE_RELEASED_MY
                    ,   DATE_RELEASED_PH
                    ,   DATE_RELEASED_SG
                    FROM
                        DWH_SNAPSHOT.DIM_PRODUCT
                    ,   (
                            SELECT
                                MAX(DATE(SNAPSHOT_CREATED_AT))  MAX_SNAP_DATE
                            FROM
                                DWH_SNAPSHOT.DIM_PRODUCT
                        )
                    WHERE
                        DATE(SNAPSHOT_CREATED_AT)   =       MAX_SNAP_DATE
                    AND PRODUCT_LINE                IS      NOT NULL
                    AND HENRY_CATEGORY_1            IS      NOT NULL
                    AND BRAND                       IN      ('Alita', 'Basics', 'Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio', 'Blackdog BKK')
                    AND PARENT_PRODUCT_LINE         NOT IN  ('3rd Party', 'Cosmetics', 'Accessories', 'Free Gift', 'Giveaway')
                    AND PRODUCT_LINE                NOT IN  ('Bags', 'Shoes', '3P Bags', 'Beauty', 'Lifestyle')
                    AND HENRY_CATEGORY_1            NOT IN  ('Accessories', 'Bags', 'Bath&Body', 'Beverage Container', 'Cosmetics', 'Hair', 'Miscellaneous', 'Shoes', 'Skin Care', 'Stationery', 'Intimates')
                    AND ID_PRODUCT                  NOT IN  (19, 41, 14196, 14197, 14198, 14199, 14200, 14201, 14202, 14203, 14204, 14205, 14206, 14207, 14208, 17131, 17132, 17654, 17182, 35653)
                )
            ,   CALENDAR        AS  (
                    SELECT
                        FULL_DATE
                    ,   WEEK_OF_MONTH_NUMBER    AS  WEEK_IN_MONTH
                    ,   YEAR_ID
                    ,   MONTH_ID
                    FROM
                        DWH.DIM_CALENDAR
                )
            ,   STORE_INFO      AS  (
                    SELECT DISTINCT CASE
                                        WHEN ID_STORE = '5a7c011b6878b592402d76bd' THEN '371'
                                        WHEN ID_STORE = '5b9890f4d49bab0743f5689e' THEN '241'
                                        WHEN ID_STORE = '5b989110a9afac06f24b80ce' THEN '371'
                                        WHEN ID_STORE = '5b55a681868e636a8ab1eaa2' THEN '261'
                                        WHEN ID_STORE = '5bbedd0bc3cef17af5b1a3ab' THEN '11'
                                        WHEN ID_STORE = 'temp01' THEN '211'
                                        WHEN ID_STORE = 'temp02' THEN '221'
                                        WHEN ID_STORE = '5bb2e6ac1e5abf2252df039c' THEN '251'
                                        ELSE ID_STORE
                                    END AS ID_STORE ,
                                    LOWER(STORE_NAME) STORE_NAME
                    FROM DWH.DIM_STORE )
            ,   STORE_NAME AS (
                    SELECT ID_STORE ,
                       CASE
                           WHEN ( ID_STORE = '12'
                                 OR ID_STORE = '211'
                                 OR ID_STORE = '341' ) THEN 1
                           WHEN ( ID_STORE = '261'
                                 OR ID_STORE = '11'
                                 OR ID_STORE = '251'
                                 OR ID_STORE = '371'
                                 OR ID_STORE = '351'
                                 OR ID_STORE = '401'
                                 OR ID_STORE = '411'
                                 OR ID_STORE = '441'
                                 OR ID_STORE = '111' ) THEN 2
                           WHEN ( ID_STORE = '15'
                                 OR ID_STORE = '35'
                                 OR ID_STORE = '32'
                                 OR ID_STORE = '42'
                                 OR ID_STORE = '221'
                                 OR ID_STORE = '331'
                                 OR ID_STORE = '311'
                                 OR ID_STORE = '321'
                                 OR ID_STORE = '301'
                                 OR ID_STORE = '361'
                                 OR ID_STORE = '381'
                                 OR ID_STORE = '431'
                                 OR ID_STORE = '391'
                                 OR ID_STORE = '421' ) THEN 3
                           WHEN ( ID_STORE = '231'
                                 OR ID_STORE = '241'
                                 OR ID_STORE = '281' ) THEN 4
                           ELSE NULL
                       END AS STORE_CLUSTER
                        FROM
                            STORE_INFO
                )
            ,   PROMOTION   AS  (
                    SELECT
                        DISTINCT
                        ID
                    ,   CATEGORY
                    FROM
                        DWH.DIM_PROMOTION
                )
            SELECT
                FSO.ID_PRODUCT
            ,   ID_SHOP
            ,   CASE
                WHEN ID_SHOP = '1' THEN 'TH'
                WHEN ID_SHOP = '2' THEN 'SG'
                WHEN ID_SHOP = '5' THEN 'ID'
                WHEN ID_SHOP = '11' THEN 'MY'
                ELSE NULL
            END AS ID_SHOP_NAME
            ,   FSO.ID_STORE
            ,   STORE_CLUSTER
            ,   WAREHOUSE
            ,   WEEK_IN_MONTH
            ,   CAMPAIGN_NAME
            ,   IS_MEGA_CAMPAIGN_ORDER
            ,   CASE
                WHEN COALESCE( CASE WHEN ID_SHOP = '1' THEN DATE_DIFF('day', DATE(DATE_RELEASED), DATE(TRANSACTION_TIME)) ELSE NULL END , CASE WHEN ID_SHOP = '5' THEN DATE_DIFF('day', DATE(DATE_RELEASED_ID), DATE(TRANSACTION_TIME)) ELSE NULL END, CASE WHEN ID_SHOP = '2' THEN DATE_DIFF('day', DATE(DATE_RELEASED_MY), DATE(TRANSACTION_TIME)) ELSE NULL END, CASE WHEN ID_SHOP = '11' THEN DATE_DIFF('day', DATE(DATE_RELEASED_MY), DATE(TRANSACTION_TIME)) ELSE NULL END) <= 28 THEN 1
                ELSE 0
            END AS IS_NEW_ARRIVALS
            ,   SUB_PRODUCT_LINE
            ,   HENRY_CATEGORY_1
            --,	FSO.PROMOTION_NAME
            --,	CATEGORY
            ,CASE WHEN ( CATEGORY = 'inventory-nr'
                        OR LOWER(FSO.PROMOTION_NAME) LIKE '%[s-p]%' ) THEN 'clearance'
                  WHEN ( CATEGORY <> 'inventory-nr' OR COALESCE( CASE WHEN ID_SHOP = '1' THEN DATE_DIFF('day', DATE(DATE_RELEASED), DATE(TRANSACTION_TIME)) ELSE NULL END , CASE WHEN ID_SHOP = '5' THEN DATE_DIFF('day', DATE(DATE_RELEASED_ID), DATE(TRANSACTION_TIME)) ELSE NULL END, CASE WHEN ID_SHOP = '2' THEN DATE_DIFF('day', DATE(DATE_RELEASED_MY), DATE(TRANSACTION_TIME)) ELSE NULL END, CASE WHEN ID_SHOP = '11' THEN DATE_DIFF('day', DATE(DATE_RELEASED_MY), DATE(TRANSACTION_TIME)) ELSE NULL END) <= 28 ) THEN 'new_arrivals' ELSE 'non_clearance'
             END AS PRODUCT_TYPE -- RECHECK
            ,	CASE
                  WHEN ( REPLACE(LOWER(FSO.PROMOTION_NAME), ' ', '') LIKE '%itemdiscount%'
                        OR LOWER(FSO.PROMOTION_NAME) LIKE '%[s-p]%' ) THEN (GROSS_REVENUE_USD - REVENUE_USD)/GROSS_REVENUE_USD
                  ELSE 0
              END AS ITEM_DU
            ,	CASE
                  WHEN (LOWER(FSO.PROMOTION_NAME) LIKE '%voucher%'
                        OR LOWER(FSO.PROMOTION_NAME) LIKE '%[v]%') THEN (GROSS_REVENUE_USD - REVENUE_USD)/GROSS_REVENUE_USD
                  ELSE 0
              END AS VOUCHER_DU
            FROM
                FACT_SALES_OFF  FSO
            LEFT JOIN
                DIM_PRODUCT DP
            ON
                FSO.ID_PRODUCT  =   DP.ID_PRODUCT
            LEFT JOIN
                CALENDAR    CA
            ON
                DATE(TRANSACTION_TIME)  =   DATE(FULL_DATE)
            LEFT JOIN
                STORE_NAME  STORE
            ON
                FSO.ID_STORE    =   STORE.ID_STORE
            LEFT JOIN
                PROMOTION
            on
                CAST(PROMOTION.ID  AS  varchar)    =   CAST(FSO.ID_PROMOTION   AS  varchar)
            WHERE
                DP.ID_PRODUCT           IS      NOT NULL
            AND 3 IS NOT null
            AND STORE_CLUSTER IS NOT null
            AND DATE(TRANSACTION_TIME)  BETWEEN DATE('{start_date}')    AND DATE('{end_date}')  -- insert date here

    """

    raw = wr.athena.read_sql_query(sql, database="dwh_snapshot")

    return raw
