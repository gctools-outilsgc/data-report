total_users = """#total users {YEAR}
SELECT COUNT(user.user_registration) 
FROM `user`
WHERE user.user_registration < {END_TIME}"""
delta_users = """#Delta users {YEAR}
SELECT COUNT(user.user_registration)
FROM `user` 
WHERE user.user_registration < {END_TIME} AND user.user_registration > {START_TIME}"""

total_articles = """#Total articles {YEAR}
SELECT COUNT(*) FROM revision r
WHERE rev_timestamp = (
	SELECT MIN(rev_timestamp) FROM revision r2
    WHERE r.rev_page = r2.rev_page
) AND rev_timestamp < {END_TIME}"""
delta_articles = """#Delta articles {YEAR}
SELECT COUNT(*) FROM revision r
WHERE rev_timestamp = (
	SELECT MIN(rev_timestamp) FROM revision r2
    WHERE r.rev_page = r2.rev_page
) AND rev_timestamp < {END_TIME} AND rev_timestamp > {START_TIME}"""

total_edits = """#Total edits {YEAR}
SELECT COUNT(*) FROM revision r
WHERE r.rev_timestamp < {END_TIME}"""
delta_edits = """#Delta edits {YEAR}
SELECT COUNT(*) FROM revision r
WHERE r.rev_timestamp < {END_TIME} AND r.rev_timestamp > {START_TIME}"""


