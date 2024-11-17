import os
import json
from fastavro import writer, parse_schema

# Function for reading data from JSON files
def read_json_files(directory):
    sales_data = []
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory, file_name)
            with open(file_path, 'r') as f:
                data = json.load(f)
                sales_data.extend(data)
    return sales_data

# Function for recording data in Avro format
def write_to_avro(data, avro_file_path, schema):
    parsed_schema = parse_schema(schema)  
    with open(avro_file_path, 'wb') as out_file:
        writer(out_file, parsed_schema, data) 