'''
=================================================
Milestone 3

Name  : Khoizuron Syakira
Batch : CODA RMT-023

This program is made to automate the ETL pipeline of the Travel & Tourism booking data.
The data is read and cleaned with PySpark, then saved to MongoDB Atlas.
The schedule runs every Saturday at 09:10, 09:20, and 09:30 starting 1 November 2024.
=================================================
'''

# To set the start_date of the schedule
from datetime import datetime

# This is operator for running terminal command 
from airflow.providers.standard.operators.bash import BashOperator

# DAG class
from airflow.sdk import DAG

# Location of script extract, transform, and load
SCRIPTS = "/workspace/scripts/travel"  # location folder scripts in container

with DAG(
    dag_id="P2M3_khoizuron_syakira_DAG",
    start_date=datetime(2024, 11, 1),  # schedule starting 1 November 2024
    schedule="10,20,30 9 * * 6",  # every Saturday 09:10, 09:20, and 09:30 o'clock
    catchup=False,  # schedules that were missed will not be run
    max_active_runs=1,  # so there will be no 2 Spark job running together
) as dag:

    # First task will run extract.py to read the raw file
    extract = BashOperator(task_id="extract", bash_command=f"python {SCRIPTS}/extract.py")

    # Second task is for running transform.py to clean the data
    transform = BashOperator(task_id="transform", bash_command=f"python {SCRIPTS}/transform.py")

    # Third task is to run load.py for saving the data to MongoDB
    load = BashOperator(task_id="load", bash_command=f"python {SCRIPTS}/load.py")

    extract >> transform >> load  # execution order