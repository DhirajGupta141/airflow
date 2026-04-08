
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator


from datetime import datetime


def choose_branch(**kwargs):
    amount = 100
    if amount < 50:
        return "checkout"
    else:
        return "merge"
    

with DAG("Branch_DAG_DEMO", 
         start_date=datetime(2026, 4, 1), 
         schedule="*/2 * * * *",
         catchup=False
) as dag:
    
    read_raw = BranchPythonOperator(
        task_id="branch_task",
        python_callable=choose_branch,
        dag=dag
    )

    product_page = BashOperator(
        task_id="product_page",
        bash_command="echo 'This is the Product branch!'",
        dag=dag
    )

    checkout_page = BashOperator(
        task_id="checkout_page",
        bash_command="echo 'This is the Checkout branch!'",
        dag=dag
    )

    user_page = BashOperator(
        task_id="user_page",
        bash_command="echo 'This is the User branch!'",
        dag=dag
    )
    alarm = BashOperator(
        task_id="alarm",
        bash_command="echo 'This is the Alarm branch!'",
        dag=dag
    )
    
    notify = BashOperator(
        task_id="notify",
        bash_command="echo 'This is the Notify branch!'",
        dag=dag
    )
    
    checkout = BashOperator(
        task_id="checkout",
        bash_command="echo 'This is the Checkout branch!'",
        dag=dag
    )



    merge = BashOperator(
        task_id="merge",
        bash_command="echo 'This is the Merge branch!'",
        dag=dag
    )


    start = EmptyOperator(task_id="Start")
    end  = EmptyOperator(task_id="End" , trigger_rule = "none_failed")


    start >> [product_page, checkout_page, user_page] 
    [product_page, checkout_page, user_page] >> read_raw 
    read_raw >> [checkout , merge] 
    checkout >> alarm
    merge >> notify
    [alarm, notify] >> end






