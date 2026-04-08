import awswrangler as wr
from src.utils.decorators import timeit

@timeit
def get_tot_store_traffic():
    sql = """
-- Store traffic
SELECT 
date(fot.counter_datetime) as full_date 
         , store_name
         , CASE
            WHEN store_name = 'Central World' THEN '261'
            WHEN store_name = 'Emquartier' THEN '251'
            WHEN store_name = 'IconSiam' THEN '11'
            WHEN store_name = 'Interchange21' THEN '241'
            WHEN store_name = 'Mega Bangna' THEN '211'
            WHEN store_name = 'MEGABANGNA' THEN '211'
            WHEN store_name = 'CRC' THEN '231'
            WHEN store_name = 'Central Pinklao' THEN '221'
            WHEN store_name = 'Pomelo Singapore' THEN '12'
            WHEN store_name = 'Rama9' THEN '311'
            WHEN store_name = 'Phuket' THEN '301'
            WHEN store_name = 'Silom' THEN '281'
            WHEN store_name = 'Rama3' THEN '321'
            WHEN store_name = 'Zpell' THEN '331'
            WHEN store_name = 'Central_Lardprao' THEN '341'
            WHEN store_name = 'Ari' THEN '291'
            WHEN store_name = 'NWM' THEN '351'
            WHEN store_name = 'T21' THEN '361'
            WHEN store_name = 'SCT' THEN '371'
            WHEN store_name = 'FSI' THEN '381'
            WHEN store_name = 'NEX' THEN '32'
            WHEN store_name = 'CPJ' THEN '15'
            WHEN store_name = 'JEM' THEN '42'
            WHEN store_name = 'KOTA' THEN '35'
            WHEN store_name = 'seaconbangkae' THEN '401'
            when store_name = '1UT' THEN '111'
            when store_name = 'CFE' THEN '411'
            when store_name = 'CPR' THEN '431'
            when store_name = 'CC' THEN '441'
            ELSE null
            END as id_store
         , case when store_name in ('CPJ', 'KOTA') then 5
                when store_name in ('Pomelo Singapore', 'JEM', 'NEX') then 2
                when store_name in ('1UT') then 11
           else 1
           end as id_shop
         , sum(traffic_in) as tot_store_traffic
from dwh.fact_offline_traffic fot
where fot.counter_datetime is not null
and traffic_in > 0
group by 1,2,3
"""
    tot_store_traffic = wr.athena.read_sql_query(sql, database="dwh")
    return tot_store_traffic