import json
import subprocess

mysql_query = "sudo mysql --batch"  # Assuming this is the correct command

def execute_queries_from_file(input_filename, output_filename):
    with open(input_filename, 'r') as f:
        data = json.load(f)

    for key, value in data.items():
        if isinstance(value, dict) and "query" in value:
            query = value["query"].replace("(", "\\(").replace(")", "\\)")  # Escape parentheses
            command = f"echo {query} | {mysql_query}"
            print(f"Executing command: {command}")
            result = subprocess.check_output(command, shell=True).decode('utf-8').strip()
            data[key] = {"value": result}
        else:
            data[key] = {"value": value}

    with open(output_filename, 'w') as f:
        json.dump(data, f)

# Example usage
input_filename = 'report_data.json'
output_filename = 'processed_report_data.json'
execute_queries_from_file(input_filename, output_filename)
