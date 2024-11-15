import sys
from wiki.gcwiki_stats import wiki_db 

if __name__ == "__main__":
    wiki = wiki_db()
    # wiki.unedited_page_stats()
    # set generate_only is true if the command line -g is passed
    generate_only = True if "-g" in sys.argv else False

    wiki.edited_with_percent_count()
