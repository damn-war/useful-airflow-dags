# A DAG to check in a GitHub Repository for change of a specific file
# This DAG uses a deferrable sensor
# the sensor is used in a task
# the task is scheduled hourly and the defer sleep time is set to 5 min
# change this for your needs

# Imports
import requests
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import timedelta, datetime
import pytz


# Sensor to check for commits/changes for a file in a GitHub Repository
# Using an Airflow variable to store the last commit timestamp between DAG runs
# and between deferred sensor checks
class DeferrableGutHubSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, repo, file_path, github_token, defer_time_minutes, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repo = repo
        self.file_path = file_path
        self.github_token = github_token
        self.latest_commit = Variable.setdefault("LAST_COMMIT_ID", default="")
        self.defer_time_minutes = defer_time_minutes

    def poke(self, context, event=None):
        if self.has_file_changed():
            return True
        else:
            # Using the async TimeDeltaTrigger to defer for defer_time_minute minutes
            if context['data_interval_end'] < datetime.now(pytz.utc) - timedelta(minutes=self.defer_time_minutes):
                trigger = TimeDeltaTrigger(
                    timedelta(minutes=self.defer_time_minutes))
                self.defer(trigger=trigger, method_name="poke")

    def has_file_changed(self):
        # using classic http requests to get the commit information
        headers = {'Authorization': f'token {self.github_token}'}
        response = requests.get(
            f'https://api.github.com/repos/{self.repo}/commits?path={self.file_path}', headers=headers)
        response.raise_for_status()
        commits = response.json()
        if not commits:
            return False
        latest_commit = commits[0]['commit']['committer']['date']
        if self.latest_commit == "":
            Variable.set("LAST_COMMIT_ID", latest_commit)
        if not self.latest_commit:
            self.latest_commit = latest_commit
            return False
        if latest_commit > self.latest_commit:
            Variable.set("LAST_COMMIT_ID", latest_commit)
            return True
        else:
            self.latest_commit = latest_commit
            return False


default_args = {
    'owner': "Daniel Krieg",
    'start_date': datetime(2023, 7, 10),
}

with DAG(
    'CHECK_FILE_AT_GITHUB',
    default_args=default_args,
    description='A DAG using a deferrable sensor to check commit on a file in github.',
    schedule_interval='@hourly',  # change it as you need to
    catchup=False,
) as dag:

    deferrable_task = DeferrableGutHubSensor(
        task_id='check_file_change',
        repo='USER/REPO',
        file_path='FILE_TO_OBSERVE',
        github_token=Variable.get("github_token", default_var=None), # save token to airflow variable to not have it in code
        defer_time_minutes=5
    )

    # Now, add more downstream task if needed
