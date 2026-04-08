import awswrangler as wr


def week_in_month_v2():
    sql = """
            SELECT
                DISTINCT
                DATE(FULL_DATE)         AS  FULL_DATE
            ,   YEAR_ID
            ,   MONTH_ID
            ,   WEEK_OF_YEAR_NUMBER     AS  WEEK_ID
            ,   WEEK_OF_MONTH_NUMBER    AS  WEEK_IN_MONTH
            FROM
                DWH.DIM_CALENDAR
            WHERE
                CAST(YEAR_ID    AS  int)    BETWEEN 2016    AND 2030
        """
    raw = wr.athena.read_sql_query(sql, database="dwh")

    return raw
