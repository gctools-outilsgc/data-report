import sys
from wiki.gcwiki_stats import wiki_db 

if __name__ == "__main__":
    what_to_do = {"-g": "generate", "-p": "process"}.get(sys.argv[1], "execute") if len(sys.argv) > 1 else "execute"

    wiki_report = wiki_db()
    wiki_report.generate_edit_report(what_to_do)
