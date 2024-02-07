from airflow import DAG
from datetime import datetime, timedelta  # Import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2024, 2, 7),
    'retries': 0,  # Number of retries if a task fails
}

# Create the DAG instance
dag = DAG(
    'dag1',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule interval to None for manual triggering
    catchup=False,  # Disable catch-up for past runs
)

# You can add tasks here, but for this example, we won't include any tasks.
