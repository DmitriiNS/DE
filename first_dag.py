from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import random

def hello():
    print("Airflow")
#def gen_random():
#    num1 = random.randint(1, 100)
#    num2 = random.randint(1, 100)
#    print(num1,num2)
def write_to_file():
    num1 = random.randint(1, 100)
    num2 = random.randint(1, 100)
    with open('input.txt', 'r+', encoding='utf-8') as file:
        file.seek(0,2)
        file.write(str(num1) + ' ' +str(num2)+"\n")
    file.close()
def sums_calc():
    sum1 = 0
    sum2 = 0
    with open('input.txt', 'r', encoding='utf-8') as file:
        for row in file:
            row = row.split()
            sum1 += int(row[0])
            sum2 += int(row[1])
    result = sum1 - sum2
    with open('input.txt', 'a', encoding='utf-8') as file:
        file.write(str(result))

with DAG(dag_id="first_dag", start_date=datetime(2023,1,1), schedule="0 0 * * *") as dag:
    
    bash_task = BashOperator(task_id="hello",bash_command="echo hello")
    
    python_task = PythonOperator(task_id="world",python_callable=hello)

    python_task1 = PythonOperator(task_id="2_num_random",python_callable=write_to_file)

    python_task2 = PythonOperator(task_id="write_to_file_sum",python_callable=sums_calc)

    
bash_task>>python_task>>python_task1>>python_task2