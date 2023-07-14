from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.triggers.temporal import TimeDeltaTrigger
from datetime import timedelta, datetime
import time


def sleep_for_seconds():
    time.sleep(30)

def get_dag_identifier_string(dag_name, dag_run_id):
    return f"{dag_name}__{dag_run_id}"


resource_empty_value = "empty"


class ResourceReservationOperator(BaseOperator):
    @apply_defaults
    def __init__(self, resource_list, defer_minutes, *args, **kwargs):
        super(ResourceReservationOperator, self).__init__(*args, **kwargs)
        self.resource_list = resource_list
        self.defer_minutes = defer_minutes

    def execute(self, context, event=None):        
        for resource_name in self.resource_list:
            resource_value = Variable.setdefault(resource_name, default=resource_empty_value)           
            if resource_value != resource_empty_value:
                self.log.info(f'Ressource {resource_name} ist bereits von {resource_value} reserviert.')
                trigger = TimeDeltaTrigger(
                    timedelta(minutes=self.defer_minutes))
                self.defer(trigger=trigger, method_name="execute")
            else:
                dag_name = context['dag'].dag_id
                run_id = context['dag_run'].run_id
                Variable.set(resource_name, get_dag_identifier_string(dag_name, run_id))
                self.log.info(f'Ressource {resource_name} ist nun von {dag_name+run_id} reserviert.')


class ResourceReleaseOperator(BaseOperator):
    @apply_defaults
    def __init__(self, resource_list, *args, **kwargs):
        super(ResourceReleaseOperator, self).__init__(*args, **kwargs)
        self.resource_list = resource_list

    def execute(self, context):
        dag_name = context['dag'].dag_id
        run_id = context['dag_run'].run_id
        for resource_name in self.resource_list:
            resource_value = Variable.get(resource_name, default_var=resource_empty_value)
            
            if resource_value == get_dag_identifier_string(dag_name, run_id):
                Variable.set(resource_name, resource_empty_value)
                self.log.info(f'Ressource {resource_name} wurde freigegeben.')
            else:
                self.log.warn(f'Ressource {resource_name} konnte nicht freigegeben werden, da sie von einer anderen DAG+Run-ID ({resource_value}) reserviert ist.')



default_args = {
    'owner': 'Daniel Krieg',
    'start_date': datetime(2023, 1, 1),
}


with DAG('ressource_dag_1',
         default_args=default_args,
         schedule_interval="0 * * * *",
         catchup=False
         ) as ressource_dag_1:
    
    ressources = [
        "resource_x",
        "resource_y",
    ]

    reserve_resource = ResourceReservationOperator(
        resource_list=ressources,
        defer_minutes = 1,
        task_id='reserve_resource', 
        dag=ressource_dag_1
        )
    release_resource = ResourceReleaseOperator(
        resource_list=ressources, 
        task_id='release_resource', 
        dag=ressource_dag_1
        )

    t1 = DummyOperator(
        task_id='t1',
        dag=ressource_dag_1,
        )
    sleep_task = PythonOperator(
        task_id='sleep_task', 
        python_callable=sleep_for_seconds, 
        dag=ressource_dag_1,
        )
    t2 = DummyOperator(
        task_id='t2',
        dag=ressource_dag_1,
        )

    reserve_resource >> t1 >> sleep_task >> t2 >> release_resource



with DAG('ressource_dag_2',
         default_args=default_args,
         schedule_interval="0 * * * *",
         catchup=False
         ) as ressource_dag_2:

    ressources = [
        "resource_x",
        "resource_y",
    ]

    reserve_resource = ResourceReservationOperator(
        resource_list=ressources,
        task_id='reserve_resource',
        defer_minutes = 1,
        dag=ressource_dag_2
        )
    release_resource = ResourceReleaseOperator(
        resource_list=ressources, 
        task_id='release_resource', 
        dag=ressource_dag_2
        )

    t1 = DummyOperator(
        task_id='t1',
        dag=ressource_dag_2,
        )
    sleep_task = PythonOperator(
        task_id='sleep_task', 
        python_callable=sleep_for_seconds, 
        dag=ressource_dag_2,
        )
    t2 = DummyOperator(
        task_id='t2',
        dag=ressource_dag_2,
        )

    reserve_resource >> t1 >> sleep_task >> t2 >> release_resource


with DAG('ressource_dag_3',
         default_args=default_args,
         schedule_interval="0 * * * *",
         catchup=False
         ) as ressource_dag_3:

    ressources = [
        "resource_x",
        "resource_y",
    ]

    reserve_resource = ResourceReservationOperator(
        resource_list=ressources,
        task_id='reserve_resource',
        defer_minutes = 1,
        dag=ressource_dag_3
        )
    release_resource = ResourceReleaseOperator(
        resource_list=ressources, 
        task_id='release_resource', 
        dag=ressource_dag_3
        )

    t1 = DummyOperator(
        task_id='t1',
        dag=ressource_dag_3,
        )
    sleep_task = PythonOperator(
        task_id='sleep_task', 
        python_callable=sleep_for_seconds, 
        dag=ressource_dag_3,
        )
    t2 = DummyOperator(
        task_id='t2',
        dag=ressource_dag_3,
        )

    reserve_resource >> t1 >> sleep_task >> t2 >> release_resource