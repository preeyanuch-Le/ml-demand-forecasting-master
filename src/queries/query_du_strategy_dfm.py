from src.utils.pomelo_utils import Hal


def query_du_strategy_dfm(start_date, end_date):
    hal = Hal()
    sql = f"""
SELECT fs2.id_order AS ID_ORDER
	   ,dp.id_product AS ID_PRODUCT
	   ,fs2.id_shop AS ID_SHOP
	   ,CASE WHEN fs2.id_shop = 5 THEN 'ID'
	   		 WHEN fs2.id_shop = 2 THEN 'MY'
             WHEN fs2.id_shop = 11 THEN 'MY'
             WHEN (fs2.id_shop = 14 AND date(fs2.order_date) <= '2021-09-30') THEN 'MY'
             ELSE 'TH'
             END AS WAREHOUSE
	   ,ca.week_of_month_number AS WEEK_IN_MONTH
	   ,CASE WHEN fs2.cart_rule_name LIKE '%4.4%' OR fs2.specific_price_rule_name LIKE '%4.4%' OR fs2.cart_rule_name LIKE '%4/4%' OR fs2.specific_price_rule_name LIKE '%44Promo%' OR fs2.specific_price_rule_name LIKE '%44Clear%'
	   		 OR fs2.cart_rule_name LIKE '%5.5%' OR fs2.specific_price_rule_name LIKE '%5.5%' OR fs2.cart_rule_name LIKE '%5/5%'
			 OR fs2.cart_rule_name LIKE '%6.6%' OR fs2.specific_price_rule_name LIKE '%6.6%' OR fs2.specific_price_rule_name LIKE '%6/6%'
			 OR fs2.cart_rule_name LIKE '%7.7%' OR fs2.specific_price_rule_name LIKE '%7.7%' OR fs2.cart_rule_name LIKE '%July Mega%'
             OR fs2.cart_rule_name LIKE '%8.8%' OR fs2.specific_price_rule_name LIKE '%8.8%'
             OR fs2.cart_rule_name LIKE '%9.9%' OR fs2.specific_price_rule_name LIKE '%9.9%' OR fs2.cart_rule_name LIKE '%99Pomelo%' OR fs2.cart_rule_name LIKE '%9/9%'
             OR fs2.cart_rule_name LIKE '%10.10%' OR fs2.specific_price_rule_name LIKE '%10.10%' OR fs2.cart_rule_name LIKE '%10/10%' OR fs2.specific_price_rule_name LIKE '%1010%'
             OR fs2.cart_rule_name LIKE '%11.11%' OR fs2.specific_price_rule_name LIKE '%11.11%' OR fs2.cart_rule_name LIKE '%11/11%' OR fs2.specific_price_rule_name LIKE '%1111%'
             OR fs2.cart_rule_name LIKE '%12.12%' OR fs2.specific_price_rule_name LIKE '%12.12%' OR fs2.cart_rule_name LIKE '%12/12%' OR fs2.specific_price_rule_name LIKE '%1212%'
             OR lower(fs2.specific_price_rule_name) LIKE '%mega%'
             THEN 'mega'
             WHEN fs2.cart_rule_name LIKE '%Pomelo Birthday%' OR fs2.specific_price_rule_name LIKE '%Pomelo Birthday%'
             OR fs2.cart_rule_name LIKE '%POMELO BIRTHDAY%'
             OR fs2.cart_rule_name LIKE '%BDAY%'
             OR fs2.cart_rule_name LIKE '%BD%'
             OR fs2.cart_rule_name LIKE '%BDay%'
             OR fs2.cart_rule_name LIKE '%Bday%'
             OR fs2.cart_rule_name LIKE '%Birthday Campaign%'
             THEN 'pml birthday'
             WHEN fs2.cart_rule_name LIKE '%BLACK FRIDAY%'
             OR fs2.cart_rule_name LIKE '%Black Friday%'
             THEN 'black friday'
             WHEN lower(fs2.cart_rule_name) LIKE '%halloween%'
             THEN 'halloween'
             ELSE 'normal_drop'
             END AS CAMPAIGN_NAME
       ,case when (cart_rule_name like '%4/4%'
	or fs2.specific_price_rule_name like '%44Promo%'
	or fs2.specific_price_rule_name like '%44Clear%'
	or fs2.cart_rule_name like '%4.4%'
	or fs2.specific_price_rule_name like '%4.4%'
	or cart_rule_name like '%5/5%'
	or fs2.specific_price_rule_name like '%5/5%'
	or fs2.cart_rule_name like '%5.5%'
	or fs2.specific_price_rule_name like '%5.5%'
	or cart_rule_name like '%6/6 %'
	or fs2.specific_price_rule_name like '%6/6%'
	or fs2.cart_rule_name like '%6.6%'
	or fs2.specific_price_rule_name like '%6.6%'
	or cart_rule_name like '%7/7%'
	or fs2.cart_rule_name like '%7.7%'
	or cart_rule_name like '%8/8%'
	or fs2.cart_rule_name like '%8.8%'
	or cart_rule_name like '%9/9%'
	or fs2.cart_rule_name like '%99Pomelo%'
	or fs2.cart_rule_name like '%9.9%'
	or cart_rule_name like '%10/10%'
	or fs2.cart_rule_name like '%10.10%'
	or fs2.cart_rule_name like '%1010%'
	or fs2.specific_price_rule_name like '%1010%'
	or specific_price_rule_name like '%10/10%'
	or fs2.specific_price_rule_name like '%10.10%'
	or cart_rule_name like '%11/11%'
	or fs2.cart_rule_name like '%11.11%'
	or fs2.cart_rule_name like '%1111%'
	or fs2.specific_price_rule_name like '%11/11%'
	or fs2.specific_price_rule_name like '%11.11%'
	or fs2.specific_price_rule_name like '%1111%'
	or fs2.cart_rule_name like '%12/12%'
	or fs2.cart_rule_name like '%12.12%'
	or fs2.cart_rule_name like '%1212%'
	or fs2.specific_price_rule_name like '%12/12%'
	or fs2.specific_price_rule_name like '%12.12%'
	or fs2.specific_price_rule_name like '%1212%'
    or lower(fs2.cart_rule_name) like '%pomelo birthday%'
    or lower(fs2.specific_price_rule_name) like '%pomelo birthday%'
   	or lower(fs2.cart_rule_name) like '%bday%'
   	or lower(fs2.specific_price_rule_name) like '%bday%'
    or lower(fs2.cart_rule_name) like '%bd%'
    or lower(fs2.specific_price_rule_name) like '%bd%'
    or lower(fs2.cart_rule_name) like '%birthday%'
    or lower(fs2.specific_price_rule_name) like '%black friday%'
    or lower(fs2.cart_rule_name) like '%black friday%'
    or lower(fs2.specific_price_rule_name) like '%black friday%'
    or lower(fs2.cart_rule_name) LIKE '%blackfriday%'
    or lower(fs2.specific_price_rule_name) like '%blackfriday%'
    or lower(fs2.cart_rule_name) like '%july mega%'
    or lower(fs2.specific_price_rule_name) like '%july mega%'
    or lower(fs2.cart_rule_name) like '%jul%'
    or lower(fs2.specific_price_rule_name) like '%jul%'
    or lower(fs2.cart_rule_name) like '%halloween%'
    or lower(fs2.specific_price_rule_name) like '%halloween%' )
    then 1
    else 0
    end as is_mega_campaign_order
       ,CASE WHEN fs2.specific_price_rule_type = 'inventory-nr' THEN 'clearance'
	   		 WHEN COALESCE(CASE WHEN fs2.id_shop = 5 THEN date(fs2.order_date) - date(dp.date_released_id) ELSE NULL END,
	   		 			   CASE WHEN fs2.id_shop = 2 THEN date(fs2.order_date) - date(dp.date_released_my) ELSE NULL END,
	   		 			   CASE WHEN fs2.id_shop = 11 THEN date(fs2.order_date) - date(dp.date_released_my) ELSE NULL END,
	   		 			   CASE WHEN (fs2.id_shop = 14 AND date(fs2.order_date) <= '2021-09-30') THEN date(fs2.order_date) - date(dp.date_released_my) ELSE NULL END,
	   		 			   CASE WHEN (fs2.id_shop = 14 AND date(fs2.order_date) >= '2021-10-01') THEN date(fs2.order_date) - date(dp.date_released) ELSE NULL END,
	   		 			   CASE WHEN fs2.id_shop NOT IN (5,2,11,14) THEN date(fs2.order_date) - date(dp.date_released) ELSE NULL END
	   		 -- Change New Arrivals from 14 to 28 Days
	   		 			   ) <= 28 THEN 'new_arrivals'
	   		 ELSE 'non_clearance'
	   		 END AS PRODUCT_TYPE
	   ,CASE WHEN fs2.specific_price_rule_type = 'inventory-nr' THEN 1
	   		 ELSE 0
	   		 END AS is_clearance
	   ,CASE WHEN COALESCE(CASE WHEN fs2.id_shop = 5 THEN date(fs2.order_date) - date(dp.date_released_id) ELSE NULL END,
	   		 			   CASE WHEN fs2.id_shop = 2 THEN date(fs2.order_date) - date(dp.date_released_my) ELSE NULL END,
	   		 			   CASE WHEN fs2.id_shop = 11 THEN date(fs2.order_date) - date(dp.date_released_my) ELSE NULL END,
	   		 			   CASE WHEN (fs2.id_shop = 14 AND date(fs2.order_date) <= '2021-09-30') THEN date(fs2.order_date) - date(dp.date_released_my) ELSE NULL END,
	   		 			   CASE WHEN (fs2.id_shop = 14 AND date(fs2.order_date) >= '2021-10-01') THEN date(fs2.order_date) - date(dp.date_released) ELSE NULL END,
	   		 			   CASE WHEN fs2.id_shop NOT IN (5,2,11,14) THEN date(fs2.order_date) - date(dp.date_released) ELSE NULL END
	   		 -- Change New Arrivals from 14 to 28 Days
	   		 			   ) <= 28 THEN 1
	   		 ELSE 0
	   		 END AS is_new_arrivals
	   ,CASE WHEN fs2.specific_price_rule_name LIKE '%hero%'
             OR fs2.specific_price_rule_name LIKE '%Hero%'
             THEN 1
             ELSE 0
             END AS is_hero_product
	   ,LOWER(dp.product_line) AS sub_product_line
	   ,LOWER(dp.henry_category_1) AS henry_category_1
	   ,fs2.item_discount_usd/fs2.gross_revenue_usd AS ITEM_DU
FROM dwh.fact_sales AS fs2
LEFT JOIN dwh.dim_product dp ON fs2.id_product_attribute = dp.id_product_attribute
LEFT JOIN dwh.fact_inventory_snapshot iv ON fs2.id_product_attribute = iv.id_product_attribute AND date(fs2.order_date) = date(iv.snapshot_date) AND fs2.id_shop = iv.id_shop
LEFT JOIN dwh.dim_calendar ca ON date(fs2.order_date) = date(ca.full_date)
WHERE date(fs2.order_date) BETWEEN '{start_date}' AND '{end_date}'
AND lower(fs2.order_type) NOT IN ('marketplace', 'giveaway')
AND dp.product_line IS NOT NULL
AND dp.henry_category_1 IS NOT NULL
AND dp.parent_product_line NOT IN ('Cosmetics','Accessories','Free Gift', 'Giveaway')
AND fs2.payment_type NOT IN ('free_order')
AND dp.product_line NOT IN ('Bags', 'Shoes', '3P Bags', 'Beauty', 'Lifestyle')
AND dp.henry_category_1 NOT IN ('Accessories','Bags','Bath&Body','Beverage Container','Cosmetics','Hair','Miscellaneous','Shoes','Skin Care','Stationery', 'Intimates')
AND dp.id_product not in (19, 41, 14196, 14197, 14198, 14199, 14200, 14201, 14202, 14203, 14204, 14205, 14206, 14207, 14208, 17131, 17132, 17654, 17182, 35653)
AND (lower(cart_rule_name) NOT like '%employee%'
	 AND lower(cart_rule_name) NOT like '%test%'
	 AND lower(cart_rule_name) NOT like '%uniform%'
	 AND lower(cart_rule_name) NOT like '%vm office%'
	 AND lower(cart_rule_name) NOT like '%influencer%'
	 AND lower(cart_rule_name) NOT like '%pr %'
	 OR cart_rule_name IS NULL)
AND fs2.gross_units_sold > 0
AND fs2.gross_revenue_usd > 0
AND is_clearance + is_new_arrivals <= 1
-- Inhouse Only
AND dp.parent_product_line <> '3rd Party'
AND dp.product_line <> '3P Apparel'
AND dp.brand IN ('Alita','Basics','Pomelo', 'BEET by Pomelo', 'Holiday Collection', 'Pomelo x Tex Saverio','Blackdog BKK')
        """
    raw = hal.get_pandas_df(sql)
    return raw
