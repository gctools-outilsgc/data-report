query = """# 1. delta user {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) as {FUNCTIONAL_NAME}delta_user FROM elggusers_entity ue
    JOIN elggentities ee ON ee.guid = ue.guid
    WHERE ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}delta_user
UNION
# 2. delta groups {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) as {FUNCTIONAL_NAME}delta_group FROM elgggroups_entity ue
    JOIN elggentities ee ON ee.guid = ue.guid
    WHERE ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}delta_group
UNION
# 3. delta career mp {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) FROM elggentities ee
    WHERE ee.subtype = 70
    AND ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}career_mp_delta
UNION
# 4. delta discussions {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) FROM elggentities ee
    WHERE ee.subtype = 7
    AND ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}discussions_delta
UNION
# 5. delta discussion comments {DISPLAY_NAME}
SELECT COUNT(*) FROM (SELECT * FROM elggentities WHERE subtype = 66) ee
JOIN (
    SELECT * FROM elggentities
    WHERE subtype = 7
) parent_ee ON parent_ee.guid = ee.container_guid
WHERE ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
UNION
# 6. delta blogs {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) FROM elggentities ee
    WHERE ee.subtype = 5
    AND ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}blogs_delta
UNION
# 7. delta blog comments {DISPLAY_NAME}
SELECT COUNT(*) FROM (SELECT * FROM elggentities WHERE subtype = 64) ee
JOIN (
    SELECT * FROM elggentities
    WHERE subtype = 5
) parent_ee ON parent_ee.guid = ee.container_guid
WHERE ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
UNION
# 8. delta wire {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) FROM elggentities ee
    WHERE ee.subtype = 17
    AND ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}wire_delta
UNION
# 9. delta pages {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) FROM elggentities ee
    WHERE ee.subtype = 9
    AND ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}pages_delta
UNION
# 10. delta page comments {DISPLAY_NAME}
SELECT COUNT(*) FROM (SELECT * FROM elggentities WHERE subtype = 64) ee
JOIN (
    SELECT * FROM elggentities
    WHERE subtype = 9
) parent_ee ON parent_ee.guid = ee.container_guid
WHERE ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
UNION
# 11. delta events {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) FROM elggentities ee
    WHERE ee.subtype = 20
    AND ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}events_delta
UNION
# 12. delta files {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) FROM elggentities ee
    WHERE ee.subtype = 1
    AND ee.time_created > {INITIAL_TIME} AND ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}files_delta
UNION
# 13. total user {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) as {FUNCTIONAL_NAME}user FROM elggusers_entity ue
    JOIN elggentities ee ON ee.guid = ue.guid
    WHERE ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}total_user
UNION
# 14. total groups {DISPLAY_NAME}
SELECT * FROM (
    SELECT COUNT(*) as {FUNCTIONAL_NAME}group FROM elgggroups_entity ue
    JOIN elggentities ee ON ee.guid = ue.guid
    WHERE ee.time_created < {END_TIME}
) {FUNCTIONAL_NAME}total_group"""
