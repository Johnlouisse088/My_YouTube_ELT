import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

# Find today's JSON file → open it → convert the JSON into a Python object → return it.
def load_data():
    # look for the My_YouTbe_ELT{date.today()}.json
    file_path = f"./data/My_YouTbe_ELT{date.today()}.json"

    try:
        logger.info(f"Processing file: YT_data_{date.today()}")  # This helps you know which file your pipeline is processing.
        """
        open(file_path)       → open today's file
        "r"                   → read mode
        encoding="utf-8"      → how text characters are decoded
        as raw_data           → variable representing the opened file
        """
        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)  # Convert JSON → Python
            """
            Example:
            [
                {
                    "video_id": "abc123",
                    "title": "My Video"
                },
                {
                    "video_id": "xyz789",
                    "title": "Another Video"
                }
            ]
            """
        return data # return the list of python objects

    # If the file doesn't exist
    except FileNotFoundError:
        logger.error(f"File not found:{file_path}")
        raise # raise makes the task fail.

    # If the JSON is invalid
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise # raise makes the task fail.
