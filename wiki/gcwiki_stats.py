
import mysql.connector
from mysql.connector import errorcode

from . import config
from . import kube_connect

class wiki_db:
    def __init__(self):
        self.DB = kube_connect.db_connection()
        while (not bool(self.DB.check_connection())):
            continue 

        self.conn = mysql.connector.connect(**config)
        self.cursor = self.conn.cursor()

    def query_first(self, cursor, query):
        cursor.execute(query)
        result = cursor.fetchone()
        val = result[0] if result else None
        return val

    def pages_created_in_interval(self, cursor, interval, prev_interval=None):
        return self.query_first(cursor, queries.query_pages_created_in_interval(interval, prev_interval))

    def mw_page_count(self, cursor):
        return self.query_first(cursor, queries.query_mw_page_count())

    def revisions_page_count(self, cursor):
        return self.query_first(cursor, queries.query_mw_edited_page_count());

    def total_edits(self, cursor):
        return self.query_first(cursor, queries.query_total_edits())

    def unique_edited_in_interval(self, cursor, interval, prev_interval=None):
        return self.query_first(cursor, queries.query_unique_edited_in_interval(interval, prev_interval))

    def total_edited_in_interval(self, cursor, interval, prev_interval=None):
        return self.query_first(cursor, queries.query_total_edited_in_interval(interval, prev_interval))

    def edited_with_percent_count(self):
        try:
            with self.conn as conn:  # Use context manager
                with conn.cursor() as cursor:
                    print()
                    count = self.revisions_page_count(cursor)
                    edits = self.total_edits(cursor)

                    intervals = ["6 MONTH", "1 YEAR", "2 YEAR", "3 YEAR", "4 YEAR", "5 YEAR", "10 YEAR"]
                    last_interval = None

                    # Add "Created Pages" to headers
                    headers = ["Interval", "Start", "Edited Pages", "% Pages Edited", "Page Edits", "% Edits", "Created Pages", 
                               "Cumulative Edited Pages", "% Cumulative Pages Edited", "Cumulative Page Edits", 
                               "% Cumulative Edits", "Months Since Last", "% Pages Edited/Month", "% Edits/Month"]
                    print("|".join(headers))

                    row_data = ["Total", "", count, 100, edits, 100, "-", "-", "-", "-"]
                    print("|".join(str(x) for x in row_data))

                    for interval in intervals:
                        unique_edited = self.unique_edited_in_interval(cursor, interval, last_interval)
                        total_edited = self.total_edited_in_interval(cursor, interval, last_interval)
                        cumulative_unique_edited = self.unique_edited_in_interval(cursor, interval, None)
                        cumulative_total_edited = self.total_edited_in_interval(cursor, interval, None)
                        
                        # Get the count of pages created in the interval
                        created_pages = self.pages_created_in_interval(cursor, interval, last_interval)

                        months = 6
                        if last_interval:
                            cursor.execute(f"""SELECT PERIOD_DIFF(
                                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL {last_interval}), '%Y%m'), 
                                    DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL {interval}), '%Y%m')
                                    )""")
                            months = cursor.fetchone()[0]
                        
                        # Add created_pages to row_data
                        row_data = [
                            f"{interval} ago",
                            f"{f"until {last_interval} ago" if last_interval else 'today'}",
                            unique_edited,
                            round(unique_edited * 100 / count, 2),
                            total_edited,
                            round(total_edited * 100 / edits, 2),
                            created_pages,  # Added here

                            cumulative_unique_edited,
                            round(cumulative_unique_edited * 100 / count, 2),
                            cumulative_total_edited,
                            round(cumulative_total_edited * 100 / edits, 2),

                            months if months else "-",  
                            round(unique_edited * 100 / count / months, 2) if months else "-",
                            round(total_edited * 100 / edits / months, 2) if months else "-"
                        ]
                        
                        print("|".join(str(x) for x in row_data))
                        
                        last_interval = interval
                 
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            pass
  
    def close_connection(self):
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn.is_connected():
            self.conn.close()
        if hasattr(self, 'DB'):
            self.DB.terminate()

class queries:
    def query_mw_page_count():
        # https://www.mediawiki.org/wiki/Manual:Article_count
        return """
SELECT count(distinct(page_id))
FROM pagelinks
INNER JOIN page ON pl_from = page_id
WHERE page_namespace = 0
AND page_is_redirect = 0;
  ;"""

    def query_mw_edited_page_count():
        return f"""SELECT count(*) as edited FROM page WHERE page_namespace=0 and page_is_redirect=0"""

    def query_total_edits():
        return """
SELECT count(*) from revision
JOIN page on rev_page = page_id
WHERE page_namespace = 0
AND page_is_redirect = 0;
  ;"""

    def query_unique_edited_in_interval(interval, prev_interval=None):
        return f"""
SELECT count(*) AS edited FROM page p WHERE p.page_namespace=0 AND p.page_is_redirect=0 
  AND (
    SELECT rev_timestamp 
    FROM revision r 
    WHERE r.rev_id = p.page_latest 
      {f"AND r.rev_timestamp < DATE_SUB(CURDATE(), INTERVAL {prev_interval})" if prev_interval else ""}
  ) >= DATE_SUB(CURDATE(), INTERVAL {interval})
"""

    def query_total_edited_in_interval(interval, prev_interval=None):
        return f"""
SELECT count(*) FROM revision r 
JOIN page p on r.rev_page = p.page_id WHERE p.page_namespace=0 
AND p.page_is_redirect=0 and r.rev_timestamp >= DATE_SUB(CURDATE(), INTERVAL {interval})
{f"AND r.rev_timestamp < DATE_SUB(CURDATE(), INTERVAL {prev_interval})" if prev_interval else ""}
"""

    def query_pages_created_in_interval(interval, prev_interval=None):
            return f"""
SELECT COUNT(*) FROM page 
WHERE page_namespace = 0 
AND page_is_redirect = 0 
AND page_touched >= DATE_SUB(CURDATE(), INTERVAL {interval}) 
{f"AND page_touched < DATE_SUB(CURDATE(), INTERVAL {prev_interval})" if prev_interval else ""}
"""