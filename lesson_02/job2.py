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

    # Schema for Avro
    schema = {
        "doc": "Sales data",
        "name": "Sales",
        "namespace": "example.avro",
        "type": "record",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "price", "type": "float"},
            {"name": "quantity", "type": "int"},
            {"name": "date", "type": "string"},
        ]
    }

    # Processing all files in raw_dir
    for filename in os.listdir(raw_dir):
        if filename.endswith(".json"):
            with open(os.path.join(raw_dir, filename), "r") as f:
                sales_data = json.load(f)

            avro_file = os.path.join(stg_dir, f"{filename.replace('.json', '')}.avro")
            with open(avro_file, "wb") as out:
                writer(out, parse_schema(schema), sales_data)

    return jsonify({"status": "success", "stg_dir": stg_dir}), 201


if __name__ == "__main__":
    app.run(port=8082)