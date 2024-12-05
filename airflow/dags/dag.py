# Define o DAG
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from separate_genre_ratings import separate_genre_ratings
from create_table_in_clickhouse import create_table_in_clickhouse
from enters_data_into_clickhouse import enters_data_into_clickhouse_ratings
from enters_data_into_clickhouse import enters_data_into_clickhouse_movies
from enters_data_into_clickhouse import enters_data_into_clickhouse_ratings_genre
from change_csv_types import change_csv_types

with DAG(
    'etl_recommendation',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once',
    catchup=False,
) as dag:
    
    task_change_csv_types = PythonOperator(
        task_id='change_csv_types',
        python_callable=change_csv_types,
    )
    
    task_separate_genre_ratings = PythonOperator(
        task_id='separate_genre_ratings',
        python_callable=separate_genre_ratings,
    )

    task_create_table_in_clickhouse = PythonOperator(
        task_id='create_table_in_clickhouse',
        python_callable=create_table_in_clickhouse
    )

    task_enters_data_into_clickhouse_ratings = PythonOperator(
        task_id='enters_data_into_clickhouse_ratings',
        python_callable=enters_data_into_clickhouse_ratings
    )

    task_enters_data_into_clickhouse_movies = PythonOperator(
        task_id='enters_data_into_clickhouse_movies',
        python_callable=enters_data_into_clickhouse_movies
    )

    task_enters_data_into_clickhouse_ratings_genre = PythonOperator(
        task_id='enters_data_into_clickhouse_ratings_genre',
        python_callable=enters_data_into_clickhouse_ratings_genre
    )

    task_change_csv_types >> task_separate_genre_ratings >> task_create_table_in_clickhouse >> [task_enters_data_into_clickhouse_ratings, task_enters_data_into_clickhouse_movies, task_enters_data_into_clickhouse_ratings_genre]
