import awswrangler as wr


def year_week_id():
    sql = f"""
    SELECT DISTINCT full_date,
    CASE WHEN ca.week_of_year_number  in (1,2,3,4,5,6,7,8,9) 
         THEN concat(cast(ca.year_id as varchar),'0',CAST(ca.week_of_year_number AS VARCHAR))
         WHEN ca.week_of_year_number = 53 or ca.week_of_year_number = 54
         THEN concat(cast(ca.year_id+1 as varchar),'01')
         ELSE concat(cast(ca.year_id as varchar),cast(ca.week_of_year_number as varchar)) 
            END as year_week_id
    FROM dwh.dim_calendar ca
        """
    calendar = wr.athena.read_sql_query(sql, database="dwh")
    return calendar
