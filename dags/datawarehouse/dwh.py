from datawarehouse.data_utils import (
    get_conn_cursor,
    close_conn_cursor,
    create_schema,
    create_table,
    get_video_ids,
)
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task


"""
This file is basically handling the staging layer and core layer of your pipeline.
- A staging layer is a temporary area where you store data before transforming it into the final clean data.  
"""
logger = logging.getLogger(__name__) # This creates a logger for this Python file. # the __name__ is current file
table = "yt_api" # the table name

@task
def staging_table():  # load the JSON data into the staging schema and keep it synchronized.

    """
    A staging arear is where data is stored after extracting it from the source and
    before the main transformations/cleaning.
    """
    schema = "staging"

    # conn   → database connection
    # cur    → database cursor
    # start as None so the finally block can safely check whether they were created.
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()  # This connects to PostgreSQL. example output: <connection object at 0x...> <cursor object at 0x...>
        YT_data = load_data()  # list of python objects
        create_schema(schema)  # Creates a PostgreSQL schema
        create_table(schema)   # Responsible for creating the yt_api table
        table_ids = get_video_ids(cur, schema) # example output: table_ids = ["abc123", "xyz456", "qwe789"]

        for row in YT_data:
            """
            Assuming the database table is empty on the first run. It will always goes to the If condition even the 
            YT_data contains 100 videos
            """
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)  # Insert one video (row)
            else:
                if row["video_id"] in table_ids: # Check if the video already exist
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)  # Insert one video (row)

        # Get all IDs from the JSON. then add to set
        ids_in_json = {row["video_id"] for row in YT_data}
        # Find IDs that should be deleted
        ids_to_delete = set(table_ids) - ids_in_json
        """
        Example:
            table_ids = {"A", "B", "C", "D"}
            ids_in_json = {"A", "B", "C"}
            ids_to_delete = table_ids - ids_in_json
            print(ids_to_delete) # {"D"}
        """
        """
            YT_data = data you just pulled/loaded from the JSON file
            table_ids = IDs that already exist in the database table
        """
        if ids_to_delete:
            # handles deleting database records that no longer exist in your latest JSON data.
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


@task
def core_table():  # read staging data, transform it, then synchronize the core schema.

    schema = "core"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()
        create_schema(schema)
        create_table(schema)
        table_ids = get_video_ids(cur, schema)
        current_video_ids = set()
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:
            current_video_ids.add(row["Video_ID"])
            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
            else:
                transformed_row = transform_data(row)
                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    insert_rows(cur, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        # Ensure the connection and cursor are closed
        if conn and cur:
            close_conn_cursor(conn, cur)
