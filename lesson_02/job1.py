import os
import json
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import shutil

load_dotenv()
AUTH_TOKEN = os.getenv("AUTH_TOKEN")

app = Flask(__name__)

@app.route("/", methods=["POST"])
def extract_sales_data():
    # Get data from POST-request
    data = request.get_json()
    date = data.get("date")
    raw_dir = data.get("raw_dir")

    # Check params
    if not date or not raw_dir:
        return jsonify({"error": "date and raw_dir must be provided"}), 400

    # Clean raw-directory before recording new files
    if os.path.exists(raw_dir):
        shutil.rmtree(raw_dir)
    os.makedirs(raw_dir)

    # Extract data from API
    page = 1
    all_sales = []
    while True:
        response = requests.get(
            url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
            params={'date': date, 'page': page},
            headers={'Authorization': AUTH_TOKEN},
        )

        if response.status_code != 200:
            break

        sales_data = response.json()
        if not sales_data:
            break

        all_sales.extend(sales_data)
        page += 1

    # Save data in JSON-file
    output_file = os.path.join(raw_dir, f"sales_{date}.json")
    with open(output_file, "w") as f:
        json.dump(all_sales, f, indent=4)

    return jsonify({"status": "success", "output_file": output_file}), 201


if __name__ == "__main__":
    app.run(port=8081)