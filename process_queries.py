import json
import subprocess

mysql_query = "sudo mysql --batch" # "echo 1" 

def execute_queries_from_file(input_filename, output_filename):
    with open(input_filename, 'r') as f:
        data = json.load(f)

    def process_data(data):
        for key, value in data.items():
            if isinstance(value, dict):
                if "query" in value:
                    query = value["query"].replace('"', '\\"')
                    command = f"echo \"{query}\" | {mysql_query}"
                    result = subprocess.check_output(command, shell=True).decode('utf-8').strip()
                    print(f"Executed: {key}: {result}")
                    data[key] = {"value": result}
                else:
                    process_data(value)
            else:
                data[key] = {"value": value}

    process_data(data)

    with open(output_filename, 'w') as f:
        json.dump(data, f)

input_filename = 'report_data.json'
output_filename = 'processed_report_data.json'
execute_queries_from_file(input_filename, output_filename)
