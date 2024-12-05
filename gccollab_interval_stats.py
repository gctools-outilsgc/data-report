import subprocess

# Database connection details
db = "collab_test"

intervals = {
    "6 months": "INTERVAL 6 MONTH",
    "1 year": "INTERVAL 1 YEAR",
    "2 years": "INTERVAL 2 YEAR",
    "3 years": "INTERVAL 3 YEAR",
    "4 years": "INTERVAL 4 YEAR",
    "5 years": "INTERVAL 5 YEAR",
    "10 years": "INTERVAL 10 YEAR",
}

def execute_query(query):
    command = f"sudo mysql --batch -N {db} -e \"{query}\""
    result = subprocess.check_output(command, shell=True)
    return result.decode('utf-8').strip()

# Calculate total count
total_count = int(execute_query("SELECT COUNT(*) FROM elggentities WHERE type != 'user' AND subtype != 31"))

# Header
print("Interval | Start | Created Content | % Content Created | Cumulative Created Content | % Cumulative Content Created")

# Total row
print(f"Total | - | {total_count} | 100.00 | - | -")

prev_value = None
cumulative_count = 0

for key, value in intervals.items():
    # Calculate values for the current interval (between current and previous period)
    if prev_value:
        created_count = int(execute_query(f"SELECT COUNT(*) FROM elggentities WHERE type != 'user' AND subtype != 31 AND time_created >= UNIX_TIMESTAMP(DATE_SUB(CURDATE(), {value})) AND time_created < UNIX_TIMESTAMP(DATE_SUB(CURDATE(), {prev_value}))"))
    else:
        created_count = int(execute_query(f"SELECT COUNT(*) FROM elggentities WHERE type != 'user' AND subtype != 31 AND time_created >= UNIX_TIMESTAMP(DATE_SUB(CURDATE(), {value}))"))  # Corrected query for the first interval

    # Calculate cumulative count using a separate query
    cumulative_count = int(execute_query(f"SELECT COUNT(*) FROM elggentities WHERE type != 'user' AND subtype != 31 AND time_created >= UNIX_TIMESTAMP(DATE_SUB(CURDATE(), {value}))"))

    # Calculate percentages
    percent_pages_created = round(created_count / total_count * 100, 2) if total_count else 0
    percent_cumulative_pages_created = round(cumulative_count / total_count * 100, 2) if total_count else 0

    # Determine the start value
    if prev_value:
        start_value = f"{prev_key} ago"
    else:
        start_value = "today"

    # Print the row
    print(f"{key} | {start_value} | {created_count} | {percent_pages_created} | {cumulative_count} | {percent_cumulative_pages_created}")

    prev_value = value
    prev_key = key
