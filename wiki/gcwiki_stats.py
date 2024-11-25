import mysql.connector
from . import config
from . import kube_connect

import json

intervals = ["6 MONTH", "1 YEAR", "2 YEAR", "3 YEAR", "4 YEAR", "5 YEAR", "10 YEAR", "20 YEAR"]
report_file = 'report_data.json'
processed_file = 'processed_report_data.json'

class print_cursor:
    query = None
    def execute(self, query):
        self.query = query

    def fetchone(self):
        return [{ 'query': self.query }]

class wiki_db:
    def connect(self):
        self.DB = kube_connect.db_connection()
        while (not bool(self.DB.check_connection())):
            continue 

        self.conn = mysql.connector.connect(**config)
        self.cursor = self.conn.cursor()

    def query_first(self, cursor, query):
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                raise ValueError(f"No results found for: {query}")  
        except Exception as e:
            print(f"ERROR: Query '{query}' failed with error: {e}") 
            raise 

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

    def months_since(self, cursor, interval, last_interval):
        return self.query_first(cursor, queries.months_since(interval, last_interval))

    def move_value_to_parent(self, data):
        if isinstance(data, dict):
            for key, value in list(data.items()):  # Use list() to avoid modifying dict during iteration
                if isinstance(value, dict) and "value" in value:
                    data[key] = float(value["value"])
                else:
                    self.move_value_to_parent(value)
        elif isinstance(data, list):
            for item in data:
                self.move_value_to_parent(item)
        return data

    def generate_edit_report(self, what_to_do):
        print(f"Generating report step {what_to_do}")
        if what_to_do == 'generate':
           data = self.generate_data(print_cursor())
           with open(report_file, 'w') as f:
                json.dump(data, f)
                print(f"Data written to {report_file}")
                return

        if what_to_do == 'process':
            with open(processed_file, 'r') as f:
                data = self.move_value_to_parent(json.load(f))
                print(f"{data}")
                self.generate_report(data)
                return

        try:
            self.connect()
            with self.conn as conn:
                with conn.cursor() as cursor:
                    data = self.generate_data(cursor)
                    self.generate_report(data)
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            pass

    def generate_data(self, cursor):
        q = {}

        q['count'] = self.revisions_page_count(cursor)
        q['edits'] = self.total_edits(cursor)

        last_interval = None

        for interval in intervals:
            q[interval] = {}  
            q[interval]['unique_edited'] = self.unique_edited_in_interval(cursor, interval, last_interval)
            q[interval]['total_edited'] = self.total_edited_in_interval(cursor, interval, last_interval)
            q[interval]['cumulative_unique_edited'] = self.unique_edited_in_interval(cursor, interval, None)
            q[interval]['cumulative_total_edited'] = self.total_edited_in_interval(cursor, interval, None)

            q[interval]['created_pages'] = self.pages_created_in_interval(cursor, interval, last_interval)

            q[interval]['months'] = self.months_since(cursor, interval, last_interval) if last_interval else 6

            last_interval = interval

        return q

        
    def generate_report(self, q):
        last_interval = None

        headers = ["Interval", "Start", "Edited Pages", "% Pages Edited", "Page Edits", "% Edits", "Created Pages",
                "Cumulative Edited Pages", "% Cumulative Pages Edited", "Cumulative Page Edits",
                "% Cumulative Edits", "Months Since Last", "% Pages Edited/Month", "% Edits/Month"]
        print("|".join(headers))

        row_data = ["Total", "", q['count'], 100, q['edits'], 100, "-", "-", "-", "-"]  
        print("|".join(str(x) for x in row_data))

        for interval in intervals:
            i = q[interval]
            months = i['months'] if last_interval else 6
            row_data = [
                f"{interval} ago",
                f"{f'until {last_interval} ago' if last_interval else 'today'}",
                i['unique_edited'],
                round(i['unique_edited'] * 100 / q['count'], 2),
                i['total_edited'],
                round(i['total_edited'] * 100 / q['edits'], 2),
                i['created_pages'],

                i['cumulative_unique_edited'],
                round(i['cumulative_unique_edited'] * 100 / q['count'], 2),
                i['cumulative_total_edited'],
                round(i['cumulative_total_edited'] * 100 / q['edits'], 2),

                months if months else "-",
                round(i['unique_edited'] * 100 / q['count'] / months, 2) if months else "-",
                round(q[interval]['total_edited'] * 100 / q['edits'] / months, 2) if months else "-"
            ]

            print("|".join(str(x) for x in row_data))

            last_interval = interval
        print("""

* 'Edited Pages' is the number of pages that have been edited in the given interval.
"* 'Page Edits' is the total number of edits made in the given interval.")
"* 'Created Pages' is the number of pages created in the given interval.")
"* 'Cumulative Edited Pages' is the number of pages that have been edited since the beginning of the wiki.")
"* 'Cumulative Page Edits' is the total number of edits made since the beginning of the wiki.")
"* 'Months Since Last' is calculated as the number of months between the start of the current interval and the start of the previous interval.")
"* '/Month' calculations divide totals by the number of months in the interval.
""")
  
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

    def months_since(interval, last_interval):
            return f"""SELECT PERIOD_DIFF(
DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL {last_interval}), '%Y%m'), 
DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL {interval}), '%Y%m')
)"""