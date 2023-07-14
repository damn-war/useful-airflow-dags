from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagModel
from airflow import settings

# dummy task to get an error
def fail_task():
    raise ValueError('Task failed!')

# use the airflow metadata database instead of the airflow cli
# which is incredibly much faster
# but however.... should be used with caution
def pause_dag(context):
    dag_id = context['dag'].dag_id
    session = settings.Session()
    dm = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
    dm.is_paused = True
    session.commit()


dag = DAG(
    'fail_task_pauses_dag',
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule_interval='@daily',
)

task1 = PythonOperator(
    task_id='WILL_FAIL',
    python_callable=fail_task,
    on_failure_callback=pause_dag,  # Bei einem Fehler wird die Funktion pause_dag aufgerufen
    dag=dag,
)

task2 = DummyOperator(
    task_id='dummy_task_1',
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    depends_on_past=True,
)

task3 = DummyOperator(
    task_id='dummy_task_2',
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    depends_on_past=True,
)

task1 >> [task2, task3]

