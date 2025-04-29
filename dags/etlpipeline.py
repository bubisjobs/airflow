from airflow import DAG
import requests
# from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.dates import days_ago
import pendulum
import json
from airflow.decorators import task




with DAG(
    dag_id="nasa_postgres_etl_pipeline",
    start_date= pendulum.now().subtract(days=1),
    schedule='@daily',
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
            date DATE UNIQUE,
            media_type VARCHAR(50)
        );
        """

        ## Execute the SQL command to create the table
        pg_hook.run(create_table_sql)
    ## step 2: Fetch data from NASA API
    
    @task
    def fetch_nasa_data():
        response = requests.get(
        url="https://api.nasa.gov/planetary/apod",
        params={"api_key": "A0qj9boz6VdAp1gaRGEyKLXZ0aW7wjWTm3mdVgGk"},
        headers={"Content-Type": "application/json"},
    )
        response.raise_for_status()
        return response.json()

    # def fetch_nasa_data():
        http_operator = HttpOperator(
            task_id='extract_apod',
            http_conn_id='nasa_api', ## Connection ID Defined In Airflow for NASA API
            endpoint='planetary/apod?api_key=A0qj9boz6VdAp1gaRGEyKLXZ0aW7wjWTm3mdVgGk',
            method='GET',
            headers={"Content-Type": "application/json"},
            response_check=lambda response: response.status_code == 200,
            log_response=True,
        )
        # extract_apod = SimpleHttpOperator(
        #     task_id='extract_apod',
        #     http_conn_id='nasa_api',  # Connection ID Defined In Airflow for NASA API
        #     endpoint='planetary/apod',
        #     method='GET',
        #     data = {"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"},
        #     headers={"Content-Type": "application/json"},
        #     log_response=True,
        #     response_filter=lambda response: response.json(),  # Parse the JSON response
        # )
        response = http_operator.execute()
        return json.loads(response.text)

    ## step 3: Transform the data
    @task
    def transform_apod_data(response):
        apod_data = {
            "title": response.get("title", ''),
            "explanation": response.get("explanation", ''),
            "url": response.get("url", ''),
            "date": response.get("date", ''),
            "media_type": response.get("media_type")
        }
        return apod_data
    
    ## step 4: Load the data into Postgres
    @task
    def load_data_to_postgres(apod_data):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        insert_sql = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
        """
        pg_hook.run(insert_sql, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    ## step 5: Define the task dependencies
    create_table_task = create_table_if_not_exists()   
    fetch_data_task = fetch_nasa_data()
    transform_data_task = transform_apod_data(fetch_data_task)
    load_data_task = load_data_to_postgres(transform_data_task)
    create_table_task >> fetch_data_task >> transform_data_task >> load_data_task
