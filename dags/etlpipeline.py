from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json
from airflow.decorators import task




with DAG(
    dag_id="nasa_postgres_etl_pipeline",
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    ## step 1: Create the table in Postgres if it doesn't exist
    @task
    def create_table_if_not_exists():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE UNIQUE
            media_type VARCHAR(50)
        );
        """

        ## Execute the SQL command to create the table
        pg_hook.run(create_table_sql)
    ## step 2: Fetch data from NASA API
    @task
    def fetch_nasa_data():
        http_operator = SimpleHttpOperator(
            task_id='fetch_nasa_data',
            http_conn_id='nasa_api',
            endpoint='planetary/apod?api_key=DEMO_KEY',
            method='GET',
            headers={"Content-Type": "application/json"},
            response_check=lambda response: response.status_code == 200,
            log_response=True,
        )
        response = http_operator.execute()
        return json.loads(response.text)

