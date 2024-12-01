import os
import json
from flask import Flask, request, jsonify
from fastavro import writer, parse_schema
import shutil

app = Flask(__name__)

@app.route("/", methods=["POST"])
def convert_to_avro():
    data = request.get_json()
    raw_dir = data.get("raw_dir")
    stg_dir = data.get("stg_dir")

    if not raw_dir or not stg_dir:
        return jsonify({"error": "raw_dir and stg_dir must be provided"}), 400

    if not os.path.exists(raw_dir):
        return jsonify({"error": f"{raw_dir} does not exist"}), 400

    # Cleaning the stg directory
    if os.path.exists(stg_dir):
        shutil.rmtree(stg_dir)
    os.makedirs(stg_dir)

    sales_data = read_json_files(raw_dir)

    avro_schema_path = os.path.join(os.path.dirname(__file__), "avro_schema.json")
    with open(avro_schema_path, 'r') as schema_file:
        schema = json.load(schema_file)

    avro_file_path = os.path.join(stg_dir, f"sales_{os.path.basename(raw_dir)}.avro")

    write_to_avro(sales_data, avro_file_path, schema)

    return jsonify({"status": "success", "stg_dir": stg_dir}), 201


if __name__ == "__main__":
    app.run(port=8082)