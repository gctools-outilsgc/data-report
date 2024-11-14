
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

    def mw_page_count(self, cursor):
        # https://www.mediawiki.org/wiki/Manual:Article_count
        query = """
SELECT count(distinct(page_id))
FROM pagelinks
INNER JOIN page ON pl_from = page_id
WHERE page_namespace = 0
AND page_is_redirect = 0;
  ;"""
        cursor.execute(query)
        result = cursor.fetchone()
        count = result[0] if result else None
        return count

    def revisions_page_count(self, cursor):
        query = f"""SELECT count(*) as edited FROM page WHERE page_namespace=0 and page_is_redirect=0"""
        cursor.execute(query)
        result = cursor.fetchone()
        count = result[0] if result else None
        return count

    def total_edits(self, cursor):
        query = """
SELECT count(*) from revision
JOIN page on rev_page = page_id
WHERE page_namespace = 0
AND page_is_redirect = 0;
  ;"""
        cursor.execute(query)
        result = cursor.fetchone()
        count = result[0] if result else None
        return count

    def unique_edited_in_interval(self, cursor, interval, prev_interval=None):
        query = f"""
SELECT count(*) AS edited FROM page p WHERE p.page_namespace=0 AND p.page_is_redirect=0 
  AND (
    SELECT rev_timestamp 
    FROM revision r 
    WHERE r.rev_id = p.page_latest 
      {f"AND r.rev_timestamp < DATE_SUB(CURDATE(), INTERVAL {prev_interval})" if prev_interval else ""}
  ) >= DATE_SUB(CURDATE(), INTERVAL {interval})
"""
        cursor.execute(query)
        result = cursor.fetchone()
        count = result[0] if result else None
        return count

    def total_edited_in_interval(self, cursor, interval, prev_interval=None):
        query = f"""
SELECT count(*) FROM revision r 
JOIN page p on r.rev_page = p.page_id WHERE p.page_namespace=0 
AND p.page_is_redirect=0 and r.rev_timestamp >= DATE_SUB(CURDATE(), INTERVAL {interval})
{f"AND r.rev_timestamp < DATE_SUB(CURDATE(), INTERVAL {prev_interval})" if prev_interval else ""}
"""
        cursor.execute(query)
        result = cursor.fetchone()
        count = result[0] if result else None
        return count

    def edited_with_percent_count(self):
        try:
            with self.conn as conn:  # Use context manager
                with conn.cursor() as cursor:
                    count = self.revisions_page_count(cursor)
                    edits = self.total_edits(cursor)
                    print(f"Total Pages: {count}")
                    print(f"Total Edits: {edits}")
                    intervals = ["6 MONTH", "1 YEAR", "2 YEAR", "5 YEAR", "10 YEAR"]
                    last_interval = None
                    for interval in intervals:
                        print()
                        unique_edited = self.unique_edited_in_interval(cursor, interval, last_interval)
                        print(f"Results for {interval}:")
                        print(f"Edited Pages: {unique_edited}")
                        print(f"Percentage of pages edited: {round(unique_edited * 100 / count, 2)}")
                        total_edited = self.total_edited_in_interval(cursor, interval, last_interval)
                        print(f"Page edits: {total_edited}")
                        print(f"Percentage of edits: {round(total_edited * 100 / edits, 2)}")
                        last_interval = interval
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            pass
  
    def unedited_page_stats(self):
        try:
            # Function to generate the SQL query for unedited pages
            def generate_query(interval):
                query = f"""SELECT 
                            (
                                SELECT COUNT(*)
                                FROM page
                                WHERE page_namespace = 0 
                                  AND page_is_redirect=0
                                  AND page_id NOT IN (
                                    SELECT rev_page 
                                    FROM revision 
                                    WHERE rev_timestamp >= DATE_SUB(CURDATE(), INTERVAL {interval})
                                  )
                            ) AS 'Unedited Pages',
                            (SELECT COUNT(*) FROM page WHERE page_namespace = 0) AS 'Total Pages',
                            ROUND(
                                (
                                    SELECT COUNT(*)
                                    FROM page
                                    WHERE page_namespace = 0 
                                  AND page_is_redirect=0
                                      AND page_id NOT IN (
                                        SELECT rev_page 
                                        FROM revision 
                                        WHERE rev_timestamp >= DATE_SUB(CURDATE(), INTERVAL {interval})
                                      )
                                ) * 100 / (SELECT COUNT(*) FROM page WHERE page_namespace = 0 AND page_is_redirect=0), 2
                            ) AS 'Percentage of Unedited Pages';"""
                return query

            # Run the queries for various time intervals
            intervals = ["6 MONTH", "1 YEAR", "2 YEAR", "5 YEAR", "10 YEAR"]
            for interval in intervals:
                query = generate_query(interval)
                self.cursor.execute(query)
                results = self.cursor.fetchall()

                # Process and use the results here (e.g., print, save to file, etc.)
                print(f"Results for {interval}:")
                for row in results:
                    print(row)

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            self.conn.close()

    def close_connection(self):
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn.is_connected():
            self.conn.close()
        if hasattr(self, 'DB'):
            self.DB.terminate()