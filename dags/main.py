from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extracted_video_data,
    save_to_json,
)

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

# Define the local timezone
local_tz = pendulum.timezone("Europe/Malta")

# Default Args
default_args = {
    "owner": "dataengineers",           # Owner of the tasks
    "depends_on_past": False,           # Don't depend on previous task runs
    "email_on_failure": False,          # Don't email when a task fails
    "email_on_retry": False,            # Don't email when a task retries
    "email": "data@engineers.com",      # Email address for notifications
    # "retries": 1,                     # Number of retries after failure
    # "retry_delay": timedelta(minutes=5), # Wait time before retrying
    "max_active_runs": 1,               # Allow only one DAG run at a time
    "dagrun_timeout": timedelta(hours=1), # Maximum DAG run duration
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz), # Start scheduling from this date
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz), # Stop scheduling after this date
}

# Variables
staging_schema = "staging"
core_schema = "core"

# DAG 1: Produces raw data as JSON
with DAG(
    dag_id="produce_json",              # Unique DAG name
    default_args=default_args,          # Apply default settings
    description="DAG to produce JSON file with raw data",  # DAG description
    schedule="0 14 * * *",              # Run daily at 2:00 PM
    catchup=False,                       # Don't run missed schedules

# dag_produce is simply a Python variable that references the DAG object.
) as dag_produce:

    # Define tasks
    playlist_id = get_playlist_id()   # Get the playlist ID
    video_ids = get_video_ids(playlist_id)   # Get video IDs from the playlist
    extract_data = extracted_video_data(video_ids)  # Extract data from the videos
    save_to_json_task = save_to_json(extract_data)   # Save extracted data to JSON

    # Trigger the update_db DAG (triggering another DAG)
    # TriggerDagRunOperator will create a task
    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",  # Task name (task_id does NOT need to match the variable name)
        trigger_dag_id="update_db",   # Which DAG to start
    )

    # Set task execution order
    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db

# DAG 2: Update Database
# Create a DAG for updating the database
with DAG(
    dag_id="update_db",  # Unique name of the DAG
    default_args=default_args,  # Use the default DAG settings
    description="DAG to process JSON file and insert data into both staging and core schemas",  # Describe what this DAG does
    catchup=False,  # Don't run missed past schedules=
    schedule=None,  # Only run when manually/externally triggered

# Store this DAG as dag_update
) as dag_update:
    update_staging = staging_table()  # Update the staging database tables
    update_core = core_table()  # Update the core database tables
    # Create a task that triggers the data_quality DAG
    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",  # Name of this task
        trigger_dag_id="data_quality", # DAG that this task will trigger
    )
    # Run staging → core → data quality in order
    update_staging >> update_core >> trigger_data_quality


# DAG 3: Data Quality
# Create a DAG for checking data quality
with DAG(
    dag_id="data_quality", # Unique name of the DAG
    default_args=default_args, # Use the default DAG settings
    description="DAG to check the data quality on both layers in the database",  # Describe what this DAG does
    catchup=False,  # Don't run missed past schedules
    schedule=None,  # Only run when manually/externally triggered

# Store this DAG as dag_quality
) as dag_quality:
    soda_validate_staging = yt_elt_data_quality(staging_schema) # Check data quality in the staging schema
    soda_validate_core = yt_elt_data_quality(core_schema)  # Check data quality in the core schema
    soda_validate_staging >> soda_validate_core  # Check staging first, then core