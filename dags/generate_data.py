from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator


dag = DAG(
    dag_id="simple_pipeline",
    start_date=datetime(2026, 3, 22),
    schedule="@daily",
    catchup=False
)

# this is my function to generate data - task 1
def generate_data():
    numbers = list(range(1, 11))
    print(f"Generated numbers: {numbers}")
    return numbers

# for task 2, I have to define the function and use XCom to pull data from task 1
# the Kwargs parameter passed in the function contains metadata like task instance, execution date
# and other DAG information, so, since we need the task instance, we have to extract it from Kwargs
def process_data(**kwargs):
    ti = kwargs["ti"]
# using xcom_pull method to extract data saved in the return_value of task 1
    numbers = ti.xcom_pull(task_ids="generate_data")
    squared_numbers = [n**2 for n in numbers]
    print(f"Squared numbers: {squared_numbers}")
    return squared_numbers

# for task 3, I want to save the squared_numbers to a file
def save_data(**kwargs):
    ti = kwargs["ti"]
    squared_numbers = ti.xcom_pull(task_ids="process_data")
# note that this option of a tmp folder which already exist in Linux
# and will not be created in my project directory structure as a folder directory will work but it is ephemeral
# and since i want to be able to easily access the saved txt file, i created a local txt
    #file_path = "/tmp/processed_data.txt"
    file_path = "processed_data.txt"

    with open(file_path, "w") as f:
        for num in squared_numbers:
            f.write(f"{num}\n")

    print(f"Data saved to {file_path}")


# this is the final task
def notify_completion():
    print("Pipeline executed successfully!")


# this is to create the task 1 in Airflow
generate_task = PythonOperator(
    task_id="generate_data",
    python_callable=generate_data,
    dag=dag
)

# i have to create the task2 for Airflow to understand
process_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag
)


# I have to create the task 3
save_task = PythonOperator(
    task_id="save_data",
    python_callable=save_data,
    dag=dag
)


# i have to create the final task
notify_task=PythonOperator(
    task_id="notify_completion",
    python_callable=notify_completion,
    dag=dag
)

# finally, i have to set the task dependencies here
generate_task >> process_task >> save_task >> notify_task