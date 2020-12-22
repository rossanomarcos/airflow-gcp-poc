from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator, DataProcSparkOperator,
    DataprocClusterDeleteOperator,
)
from airflow.utils.dates import days_ago
from datetime import datetime

default_arguments = {"owner": "Rossano Marcos", "start_date": days_ago(1)}

with DAG(
    "modak_dag",
    schedule_interval="0 20 * * *",
    catchup=False,
    default_args=default_arguments,
) as dag:

    dag.doc_md = __doc__

    # BashOperator
    # A simple print date
    # print_date = BashOperator(
    #     task_id='print_date',
    #     bash_command='date'
    # )

    create_cluster = DataprocClusterCreateOperator(
        task_id="create_cluster",
        project_id="rm-airflow-tutorial-demo",
        cluster_name="spark-cluster-{{ ds_nodash }}",
        num_workers=2,
        storage_bucket="rm-logistics-spark-bucket",
        zone="europe-west2-a",
    )

    create_cluster.doc_md = """## Create Dataproc cluster
    This task creates a Dataproc cluster in your project.
    """

    run_pyspak_job_1 = DataProcPySparkOperator(
        task_id="run_pyspak_job_1",
        main="gs://rm-logistics-spark-bucket/pyspark/weekend/gas_composition_count.py",
        cluster_name="spark-cluster-{{ ds_nodash }}",
        dataproc_pyspark_jars="gs://spark-lib/bigquery/spark-bigquery-latest.jar",
    )

    # SPARK_CODE = ('gs://rm-logistics-spark-bucket/pyspark/others/transformation.py')
    SPARK_CODE = ('gs://rm-logistics-spark-bucket/pyspark/others/conv_files_json_to_parquet.py')
    dataproc_job_name = 'spark_job_dataproc'

    run_pyspak_job_2 = DataProcPySparkOperator(
        task_id='run_pyspak_job_2',
        main=SPARK_CODE,
        cluster_name="spark-cluster-{{ ds_nodash }}",
        job_name=dataproc_job_name
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_cluster",
        project_id="rm-airflow-tutorial-demo",
        cluster_name="spark-cluster-{{ ds_nodash }}",
        trigger_rule="all_done",
    )

create_cluster >> run_pyspak_job_1 >> run_pyspak_job_2 >> delete_cluster
