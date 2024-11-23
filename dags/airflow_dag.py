from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import pandas as pd
import os


def extract_data(**kwargs):
    """Extract data from MSSQL database."""
    hook = MsSqlHook(
        mssql_conn_id="mssql_connection_id"
    )  # Use the Airflow connection ID
    query = "SELECT * FROM Cereales_Genetics_Advanced"  #
    connection = hook.get_conn()

    # Using pandas to extract data directly into a DataFrame
    df = pd.read_sql(query, con=connection)
    connection.close()

    # Push the extracted DataFrame to XCom
    kwargs["ti"].xcom_push(key="extracted_data", value=df.to_json(orient="split"))


def transform_data(**kwargs):
    """Perform data transformation."""
    # Pull the extracted data from XCom
    ti = kwargs["ti"]
    extracted_data = ti.xcom_pull(key="extracted_data", task_ids="extract_data")
    df = pd.read_json(extracted_data, orient="split")

    # Example of transformation: Filter rows with Rendement > 75
    df_transformed = df[df["Rendement_Par_Hectare"] > 75]

    # Push the transformed DataFrame to XCom
    ti.xcom_push(key="transformed_data", value=df_transformed.to_json(orient="split"))


def save_to_csv(**kwargs):
    """Save transformed data to a CSV file."""
    ti = kwargs["ti"]
    transformed_data = ti.xcom_pull(key="transformed_data", task_ids="transform_data")
    df = pd.read_json(transformed_data, orient="split")

    # Use the mounted data directory
    output_path = "/opt/airflow/data/clean_data.csv"
    output_dir = os.path.dirname(output_path)

    # Create directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Save the DataFrame to a CSV file
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")


# Set start date to November 22, 2024
start_date = datetime(2024, 11, 22)

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    "etl_cereales_pipeline",
    default_args=default_args,
    description="ETL pipeline to extract, transform, and save cereal data to CSV",
    # Custom cron to run on the 22nd every 3 months
    schedule_interval="0 0 22 2,5,8,11 *",  # At 00:00 on day 22 in February, May, August, and November (every 3 months starting from 22-Nov)
    catchup=False,
) as dag:

    # Your existing tasks remain the same
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    save_task = PythonOperator(
        task_id="save_to_csv",
        python_callable=save_to_csv,
    )

    # Define task dependencies
    extract_task >> transform_task >> save_task
