import os
import json
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import shutil
from utils import fetch_sales_data, save_to_json

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
        sales_data = fetch_sales_data(date, page, auth_token)
        
        # Якщо дані відсутні — виходимо з циклу
        if not sales_data:
            break

        # Формування шляху до файлу
        file_path = os.path.join(raw_dir, f"sales_{date}_{page}.json")

        # Використання функції save_to_json для запису даних у файл
        save_to_json(sales_data, file_path)

        page += 1


    return jsonify({"status": "success", "output_file": output_file}), 201


if __name__ == "__main__":
    app.run(port=8081)