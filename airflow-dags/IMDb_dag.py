# airflow related
from airflow import models
from airflow import DAG
# other packages
from datetime import datetime, timedelta
# import operators from the 'operators' file
from operators import FilesToS3

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    # set your start_date : airflow will run previous dags if dags 
    # since startdate has not run

    'start_date': datetime(2019, 5, 22),
    'project_id' : 'your_project_name',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with models.DAG(
    dag_id='IMDb_dag',
    schedule_interval = timedelta(days=30),
    default_args=default_dag_args
    ) as dag:



    task1 = FilesToS3.FilesToS3Operator(
        task_id='files_to_s3',
        bucket_name = 'YOUR_BUCKET_NAME_HERE',
        dir_path = 'PATH_TO_FILES_DIR_HERE')

    task1