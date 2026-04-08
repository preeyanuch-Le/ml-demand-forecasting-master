import awswrangler as wr
from src.utils.decorators import timeit

@timeit
def get_calendar():
    sql = """
SELECT full_date,
case when ca.week_of_year_number  in (1,2,3,4,5,6,7,8,9) 
then concat(cast(ca.year_id as varchar),'0',CAST(ca.week_of_year_number AS VARCHAR))
when ca.week_of_year_number = 53 or ca.week_of_year_number = 54
then concat(cast(ca.year_id+1 as varchar),'01')
else concat(cast(ca.year_id as varchar),cast(ca.week_of_year_number as varchar)) 
end as year_week_id
                           from dwh.dim_calendar ca
"""
    calendar = wr.athena.read_sql_query(sql, database="dwh")
    return calendar
