import os
import json
import shutil
import pytest
from fastavro import reader
from avro_utils import read_json_files, write_to_avro

TEST_DIR = "test_data"
RAW_DIR = os.path.join(TEST_DIR, "raw")
STG_DIR = os.path.join(TEST_DIR, "stg")

SCHEMA = {
    "doc": "Sales data",
    "name": "Sales",
    "namespace": "example.avro",
    "type": "record",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "float"},
        {"name": "quantity", "type": "int"},
        {"name": "date", "type": "string"}
    ]
}

EXAMPLE_DATA = [
    {"id": "1", "product": "item1", "price": 10.5, "quantity": 2, "date": "2022-08-09"},
    {"id": "2", "product": "item2", "price": 15.0, "quantity": 3, "date": "2022-08-09"}
]


@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown():
    # Create test directories before tests
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(STG_DIR, exist_ok=True)

    # Create test JSON file
    test_file_path = os.path.join(RAW_DIR, "test_sales.json")
    with open(test_file_path, 'w') as test_file:
        json.dump(EXAMPLE_DATA, test_file)

    # Run tests
    yield

    # Cleaning test directory
    shutil.rmtree(TEST_DIR)


def test_read_json_files():
    # Testing reading data from JSON files
    result = read_json_files(RAW_DIR)
    assert result == EXAMPLE_DATA, "Data read from JSON files does not appear to be extracted."


def test_write_to_avro():
    # Testing data recording in Avro format
    avro_file_path = os.path.join(STG_DIR, "test_sales.avro")
    write_to_avro(EXAMPLE_DATA, avro_file_path, SCHEMA)

    # Check whether the Avro file is running
    assert os.path.exists(avro_file_path), "The Avro file is not created."

    # Read the data from the Avro file and check it instead
    with open(avro_file_path, 'rb') as f:
        avro_reader = reader(f)
        records = [record for record in avro_reader]
        assert records == EXAMPLE_DATA, "The data in the Avro file does not appear to be accurate."