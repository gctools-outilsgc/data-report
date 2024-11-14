from wiki.gcwiki_stats import wiki_db 

if __name__ == "__main__":
    wiki = wiki_db()
    # wiki.unedited_page_stats()
    wiki.edited_with_percent_count()
