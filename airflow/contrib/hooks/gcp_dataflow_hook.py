# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
import logging
import re
import select
import subprocess
import time
import uuid
from typing import Any, Generator, List, Optional, Set

from googleapiclient.discovery import build

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timeout import timeout


# This is the default location
# https://cloud.google.com/dataflow/pipelines/specifying-exec-params
DEFAULT_DATAFLOW_LOCATION = 'us-central1'

# [EWT-1001]: Add another job id pattern to act as a failsafe
JOB_ID_PATTERN = re.compile(
  r'.*console.cloud.google.com/dataflow/jobs/[a-z0-9-]*/(?P<job_id_url>.*)\?project=.*|'
  r'Submitted job: (?P<job_id_java>.*)|'
  r'Created job with id: \[(?P<job_id_python>.*)\]'
)

MONITORING_URL_PATTERN = (
  "https://console.cloud.google.com/dataflow/jobs/{region}/{job_id}?project={project}"
)

# [EWT-1001]: Added new class from open source for dataflow job status
# Open source commit hash: da88ed1943e85850fcdf32c663ec2940c65dbe75
class DataflowJobStatus:
  """
  Helper class with Dataflow job statuses.
  Reference: https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
  """

  JOB_STATE_DONE = "JOB_STATE_DONE"
  JOB_STATE_UNKNOWN = "JOB_STATE_UNKNOWN"
  JOB_STATE_STOPPED = "JOB_STATE_STOPPED"
  JOB_STATE_RUNNING = "JOB_STATE_RUNNING"
  JOB_STATE_FAILED = "JOB_STATE_FAILED"
  JOB_STATE_CANCELLED = "JOB_STATE_CANCELLED"
  JOB_STATE_UPDATED = "JOB_STATE_UPDATED"
  JOB_STATE_DRAINING = "JOB_STATE_DRAINING"
  JOB_STATE_DRAINED = "JOB_STATE_DRAINED"
  JOB_STATE_PENDING = "JOB_STATE_PENDING"
  JOB_STATE_CANCELLING = "JOB_STATE_CANCELLING"
  JOB_STATE_QUEUED = "JOB_STATE_QUEUED"
  FAILED_END_STATES = {JOB_STATE_FAILED, JOB_STATE_CANCELLED}
  SUCCEEDED_END_STATES = {JOB_STATE_DONE, JOB_STATE_UPDATED, JOB_STATE_DRAINED}
  TERMINAL_STATES = SUCCEEDED_END_STATES | FAILED_END_STATES
  AWAITING_STATES = {
    JOB_STATE_RUNNING,
    JOB_STATE_PENDING,
    JOB_STATE_QUEUED,
    JOB_STATE_CANCELLING,
    JOB_STATE_DRAINING,
    JOB_STATE_STOPPED,
  }

# [EWT-1001]: Added new class from open source for dataflow job type
# Open source commit hash: da88ed1943e85850fcdf32c663ec2940c65dbe75
class DataflowJobType:
  """Helper class with Dataflow job types."""

  JOB_TYPE_UNKNOWN = "JOB_TYPE_UNKNOWN"
  JOB_TYPE_BATCH = "JOB_TYPE_BATCH"
  JOB_TYPE_STREAMING = "JOB_TYPE_STREAMING"

# [EWT-1001]: Added new class from open source for dataflow operations
# Open source commit hash: da88ed1943e85850fcdf32c663ec2940c65dbe75
class _DataflowJobsController(LoggingMixin):
  """
  Interface for communication with Google API.
  It's not use Apache Beam, but only Google Dataflow API.
  :param dataflow: Discovery resource
  :param project_number: The Google Cloud Project ID.
  :param location: Job location.
  :param poll_sleep: The status refresh rate for pending operations.
  :param name: The Job ID prefix used when the multiple_jobs option is passed is set to True.
  :param job_id: ID of a single job.
  :param num_retries: Maximum number of retries in case of connection problems.
  :param multiple_jobs: If set to true this task will be searched by name prefix (``name`` parameter),
      not by specific job ID, then actions will be performed on all matching jobs.
  :param drain_pipeline: Optional, set to True if want to stop streaming job by draining it
      instead of canceling.
  :param cancel_timeout: wait time in seconds for successful job canceling
  :param wait_until_finished: If True, wait for the end of pipeline execution before exiting. If False,
      it only submits job and check once is job not in terminal state.
      The default behavior depends on the type of pipeline:
      * for the streaming pipeline, wait for jobs to start,
      * for the batch pipeline, wait for the jobs to complete.
  """

  def __init__(
    self,
    dataflow: Any,
    project_number: str,
    location: str,
    poll_sleep: int = 10,
    name: Optional[str] = None,
    job_id: Optional[str] = None,
    num_retries: int = 0,
    multiple_jobs: bool = False,
    drain_pipeline: bool = False,
    cancel_timeout: Optional[int] = 5 * 60,
    wait_until_finished: Optional[bool] = None,
  ) -> None:

    super().__init__()
    self._dataflow = dataflow
    self._project_number = project_number
    self._job_name = name
    self._job_location = location
    self._multiple_jobs = multiple_jobs
    self._job_id = job_id
    self._num_retries = num_retries
    self._poll_sleep = poll_sleep
    self._cancel_timeout = cancel_timeout
    self._jobs: Optional[List[dict]] = None
    self.drain_pipeline = drain_pipeline
    self._wait_until_finished = wait_until_finished

  def is_job_running(self) -> bool:
    """
    Helper method to check if jos is still running in dataflow
    :return: True if job is running.
    :rtype: bool
    """
    self._refresh_jobs()
    if not self._jobs:
      return False

    for job in self._jobs:
      if job["currentState"] not in DataflowJobStatus.TERMINAL_STATES:
        return True
    return False

  def _get_current_jobs(self) -> List[dict]:
    """
    Helper method to get list of jobs that start with job name or id
    :return: list of jobs including id's
    :rtype: list
    """
    if not self._multiple_jobs and self._job_id:
      return [self.fetch_job_by_id(self._job_id)]
    if self._job_name:
      jobs = self._fetch_jobs_by_prefix_name(self._job_name.lower())
      if len(jobs) == 1:
        self._job_id = jobs[0]["id"]
      return jobs
    else:
      raise Exception("Missing both dataflow job ID and name.")

  def fetch_job_by_id(self, job_id: str) -> dict:
    """
    Helper method to fetch the job with the specified Job ID.
    :param job_id: Job ID to get.
    :type job_id: str
    :return: the Job
    :rtype: dict
    """
    return (
      self._dataflow.projects()
        .locations()
        .jobs()
        .get(
        projectId=self._project_number,
        location=self._job_location,
        jobId=job_id,
      )
        .execute(num_retries=self._num_retries)
    )

  def fetch_job_metrics_by_id(self, job_id: str) -> dict:
    """
    Helper method to fetch the job metrics with the specified Job ID.
    :param job_id: Job ID to get.
    :type job_id: str
    :return: the JobMetrics. See:
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/JobMetrics
    :rtype: dict
    """
    result = (
      self._dataflow.projects()
        .locations()
        .jobs()
        .getMetrics(projectId=self._project_number, location=self._job_location, jobId=job_id)
        .execute(num_retries=self._num_retries)
    )

    self.log.debug("fetch_job_metrics_by_id %s:\n%s", job_id, result)
    return result

  def _fetch_list_job_messages_responses(self, job_id: str) -> Generator[dict, None, None]:
    """
    Helper method to fetch ListJobMessagesResponse with the specified Job ID.
    :param job_id: Job ID to get.
    :type job_id: str
    :return: yields the ListJobMessagesResponse. See:
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse
    :rtype: Generator[dict, None, None]
    """
    request = (
      self._dataflow.projects()
        .locations()
        .jobs()
        .messages()
        .list(projectId=self._project_number, location=self._job_location, jobId=job_id)
    )

    while request is not None:
      response = request.execute(num_retries=self._num_retries)
      yield response

      request = (
        self._dataflow.projects()
          .locations()
          .jobs()
          .messages()
          .list_next(previous_request=request, previous_response=response)
      )

  def fetch_job_messages_by_id(self, job_id: str) -> List[dict]:
    """
    Helper method to fetch the job messages with the specified Job ID.
    :param job_id: Job ID to get.
    :type job_id: str
    :return: the list of JobMessages. See:
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#JobMessage
    :rtype: List[dict]
    """
    messages: List[dict] = []
    for response in self._fetch_list_job_messages_responses(job_id=job_id):
      messages.extend(response.get("jobMessages", []))
    return messages

  def fetch_job_autoscaling_events_by_id(self, job_id: str) -> List[dict]:
    """
    Helper method to fetch the job autoscaling events with the specified Job ID.
    :param job_id: Job ID to get.
    :type job_id: str
    :return: the list of AutoscalingEvents. See:
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#autoscalingevent
    :rtype: List[dict]
    """
    autoscaling_events: List[dict] = []
    for response in self._fetch_list_job_messages_responses(job_id=job_id):
      autoscaling_events.extend(response.get("autoscalingEvents", []))
    return autoscaling_events

  def _fetch_all_jobs(self) -> List[dict]:
    request = (
      self._dataflow.projects()
        .locations()
        .jobs()
        .list(projectId=self._project_number, location=self._job_location)
    )
    all_jobs: List[dict] = []
    while request is not None:
      response = request.execute(num_retries=self._num_retries)
      jobs = response.get("jobs")
      if jobs is None:
        break
      all_jobs.extend(jobs)

      request = (
        self._dataflow.projects()
          .locations()
          .jobs()
          .list_next(previous_request=request, previous_response=response)
      )
    return all_jobs

  def _fetch_jobs_by_prefix_name(self, prefix_name: str) -> List[dict]:
    jobs = self._fetch_all_jobs()
    jobs = [job for job in jobs if job["name"].startswith(prefix_name)]
    return jobs

  def _refresh_jobs(self) -> None:
    """
    Helper method to get all jobs by name
    :return: jobs
    :rtype: list
    """
    self._jobs = self._get_current_jobs()

    if self._jobs:
      for job in self._jobs:
        self.log.info(
          "Google Cloud DataFlow job %s is state: %s",
          job["name"],
          job["currentState"],
        )
    else:
      self.log.info("Google Cloud DataFlow job not available yet..")

  def _check_dataflow_job_state(self, job) -> bool:
    """
    Helper method to check the state of one job in dataflow for this task
    if job failed raise exception
    :return: True if job is done.
    :rtype: bool
    :raise: Exception
    """
    if self._wait_until_finished is None:
      wait_for_running = job.get('type') == DataflowJobType.JOB_TYPE_STREAMING
    else:
      wait_for_running = not self._wait_until_finished

    if job['currentState'] == DataflowJobStatus.JOB_STATE_DONE:
      return True
    elif job['currentState'] == DataflowJobStatus.JOB_STATE_FAILED:
      raise Exception(f"Google Cloud Dataflow job {job['name']} has failed.")
    elif job['currentState'] == DataflowJobStatus.JOB_STATE_CANCELLED:
      raise Exception(f"Google Cloud Dataflow job {job['name']} was cancelled.")
    elif job['currentState'] == DataflowJobStatus.JOB_STATE_DRAINED:
      raise Exception(f"Google Cloud Dataflow job {job['name']} was drained.")
    elif job['currentState'] == DataflowJobStatus.JOB_STATE_UPDATED:
      raise Exception(f"Google Cloud Dataflow job {job['name']} was updated.")
    elif job['currentState'] == DataflowJobStatus.JOB_STATE_RUNNING and wait_for_running:
      return True
    elif job['currentState'] in DataflowJobStatus.AWAITING_STATES:
      return self._wait_until_finished is False
    self.log.debug("Current job: %s", str(job))
    raise Exception(f"Google Cloud Dataflow job {job['name']} was unknown state: {job['currentState']}")

  def wait_for_done(self) -> None:
    """Helper method to wait for result of submitted job."""
    self.log.info("Start waiting for done.")
    self._refresh_jobs()
    while self._jobs and not all(self._check_dataflow_job_state(job) for job in self._jobs):
      self.log.info("Waiting for done. Sleep %s s", self._poll_sleep)
      time.sleep(self._poll_sleep)
      self._refresh_jobs()

  def get_jobs(self, refresh: bool = False) -> List[dict]:
    """
    Returns Dataflow jobs.
    :param refresh: Forces the latest data to be fetched.
    :type refresh: bool
    :return: list of jobs
    :rtype: list
    """
    if not self._jobs or refresh:
      self._refresh_jobs()

    return self._jobs

  # [EWT-1001]: Fixed the method to account for succeeded states as well while cancelling
  def _wait_for_states(self, expected_states: Set[str]):
    """Waiting for the jobs to reach a certain state."""
    if not self._jobs:
      raise ValueError("The _jobs should be set")
    while True:
      self._refresh_jobs()
      job_states = {job['currentState'] for job in self._jobs}
      if not job_states.difference(expected_states):
        return
      unexpected_failed_end_states = expected_states - DataflowJobStatus.FAILED_END_STATES - DataflowJobStatus.SUCCEEDED_END_STATES
      if unexpected_failed_end_states.intersection(job_states):
        unexpected_failed_jobs = [
          job for job in self._jobs if job['currentState'] in unexpected_failed_end_states
        ]
        raise AirflowException(
          "Jobs failed: "
          + ", ".join(
            f"ID: {job['id']} name: {job['name']} state: {job['currentState']}"
              for job in unexpected_failed_jobs
          )
        )
      time.sleep(self._poll_sleep)

  # [EWT-1001]: Fixed the method to take into consideration the case of multiple terminated jobs
  def cancel(self) -> None:
    """Cancels or drains current job"""
    jobs = self.get_jobs()
    if not jobs:
      self.log.info("No jobs to cancel")
      return

    job_ids = [job["id"] for job in jobs if job["currentState"] not in DataflowJobStatus.TERMINAL_STATES]
    # store all existing terminated job states
    found_terminal_job_states = {job["currentState"] for job in jobs if job["currentState"] in DataflowJobStatus.TERMINAL_STATES}
    if job_ids:
      batch = self._dataflow.new_batch_http_request()
      self.log.info("Canceling jobs: %s", ", ".join(job_ids))
      for job in jobs:
        requested_state = (
          DataflowJobStatus.JOB_STATE_DRAINED
          if self.drain_pipeline and job["type"] == DataflowJobType.JOB_TYPE_STREAMING
          else DataflowJobStatus.JOB_STATE_CANCELLED
        )
        batch.add(
          self._dataflow.projects()
            .locations()
            .jobs()
            .update(
            projectId=self._project_number,
            location=self._job_location,
            jobId=job["id"],
            body={"requestedState": requested_state},
          )
        )
      batch.execute()
      if self._cancel_timeout and isinstance(self._cancel_timeout, int):
        timeout_error_message = (
          f"Canceling jobs failed due to timeout ({self._cancel_timeout}s): {', '.join(job_ids)}"
        )
        tm = timeout(seconds=self._cancel_timeout, error_message=timeout_error_message)
        with tm:
          self._wait_for_states({DataflowJobStatus.JOB_STATE_CANCELLED} | found_terminal_job_states)
    else:
      self.log.info("No jobs to cancel")


class _DataflowJob(LoggingMixin):
    def __init__(self, dataflow, project_number, name, location, poll_sleep=10,
                 job_id=None, num_retries=None):
        self._dataflow = dataflow
        self._project_number = project_number
        self._job_name = name
        self._job_location = location
        self._job_id = job_id
        self._num_retries = num_retries
        self._job = self._get_job()
        self._poll_sleep = poll_sleep

    def _get_job_id_from_name(self):
        jobs = self._dataflow.projects().locations().jobs().list(
            projectId=self._project_number,
            location=self._job_location
        ).execute(num_retries=self._num_retries)
        for job in jobs['jobs']:
            if job['name'].lower() == self._job_name.lower():
                self._job_id = job['id']
                return job
        return None

    def _get_job(self):
        if self._job_id:
            job = self._dataflow.projects().locations().jobs().get(
                projectId=self._project_number,
                location=self._job_location,
                jobId=self._job_id).execute(num_retries=self._num_retries)
        elif self._job_name:
            job = self._get_job_id_from_name()
        else:
            raise Exception('Missing both dataflow job ID and name.')

        if job and 'currentState' in job:
            self.log.info(
                'Google Cloud DataFlow job %s is %s',
                job['name'], job['currentState']
            )
        elif job:
            self.log.info(
                'Google Cloud DataFlow with job_id %s has name %s',
                self._job_id, job['name']
            )
        else:
            self.log.info(
                'Google Cloud DataFlow job not available yet..'
            )

        return job

    def wait_for_done(self):
        while True:
            if self._job and 'currentState' in self._job:
                if 'JOB_STATE_DONE' == self._job['currentState']:
                    return True
                elif 'JOB_STATE_RUNNING' == self._job['currentState'] and \
                     'JOB_TYPE_STREAMING' == self._job['type']:
                    return True
                elif 'JOB_STATE_FAILED' == self._job['currentState']:
                    raise Exception("Google Cloud Dataflow job {} has failed.".format(
                        self._job['name']))
                elif 'JOB_STATE_CANCELLED' == self._job['currentState']:
                    raise Exception("Google Cloud Dataflow job {} was cancelled.".format(
                        self._job['name']))
                elif 'JOB_STATE_RUNNING' == self._job['currentState']:
                    time.sleep(self._poll_sleep)
                elif 'JOB_STATE_PENDING' == self._job['currentState']:
                    time.sleep(15)
                else:
                    self.log.debug(str(self._job))
                    raise Exception(
                        "Google Cloud Dataflow job {} was unknown state: {}".format(
                            self._job['name'], self._job['currentState']))
            else:
                time.sleep(15)

            self._job = self._get_job()

    def get(self):
        return self._job


class _Dataflow(LoggingMixin):
    def __init__(self, cmd):
        self.log.info("Running command: %s", ' '.join(cmd))
        self._proc = subprocess.Popen(
            cmd,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True)

    def _line(self, fd):
        if fd == self._proc.stderr.fileno():
            line = self._proc.stderr.readline().decode()
            if line:
                self.log.warning(line.rstrip("\n"))
            return line
        if fd == self._proc.stdout.fileno():
            line = self._proc.stdout.readline().decode()
            if line:
                self.log.info(line.rstrip("\n"))
            return line

        raise Exception("No data in stderr or in stdout.")

    # [EWT-1001]: Added another regex pattern match for job id extraction
    @staticmethod
    def _extract_job(line):
        # Job id info: https://goo.gl/SE29y9.
        # [EWT-361] : Fixes out of date regex to extract job id
        matched_job = JOB_ID_PATTERN.search(line or '')
        if matched_job:
            job_id = matched_job.group('job_id_java') or matched_job.group('job_id_python') or matched_job.group('job_id_url')
            logging.info(f"Job id found from the line: [{line}]")
            logging.info(f"Job id: {job_id}")
            return job_id

    def wait_for_done(self):
        reads = [self._proc.stderr.fileno(), self._proc.stdout.fileno()]
        self.log.info("Start waiting for DataFlow process to complete.")
        job_id = None
        # Make sure logs are processed regardless whether the subprocess is
        # terminated.
        process_ends = False
        while True:
            ret = select.select(reads, [], [], 5)
            if ret is not None:
                for fd in ret[0]:
                    line = self._line(fd)
                    if line:
                        job_id = job_id or self._extract_job(line)
            else:
                self.log.info("Waiting for DataFlow process to complete.")
            if process_ends:
                break
            if self._proc.poll() is not None:
                # Mark process completion but allows its outputs to be consumed.
                process_ends = True
        if self._proc.returncode != 0:
            raise Exception("DataFlow failed with return code {}".format(
                self._proc.returncode))
        return job_id


class DataFlowHook(GoogleCloudBaseHook):

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 poll_sleep=10):
        self.poll_sleep = poll_sleep
        super(DataFlowHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Dataflow service object.
        """
        http_authorized = self._authorize()
        return build(
            'dataflow', 'v1b3', http=http_authorized, cache_discovery=False)

    # [EWT-1001]: Added appropriate args and fn calls for tracking and cancelling
    @GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
    def _start_dataflow(self, variables, name, command_prefix, label_formatter,
        cancel_existing_job=False):
        variables = self._set_variables(variables)
        cmd = command_prefix + self._build_cmd(variables, label_formatter)
        # Track or cancel any already running dataflow job with the same name
        # depending on the op params `add_random_jobname_suffix` and `cancel_existing_job`
        # For more info on how this logic would behave under different scenarios
        # Refer: https://jira.twitter.biz/browse/EWT-1001?focusedCommentId=20722956&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-20722956  # noqa
        tracking_existing_job, job_id = self._track_or_cancel_existing_dataflow_job(
            name, variables, cancel_existing_job)
        if not tracking_existing_job:
            job_id = _Dataflow(cmd).wait_for_done()
        _DataflowJob(self.get_conn(), variables['project'], name,
            variables['region'],
            self.poll_sleep, job_id,
            self.num_retries).wait_for_done()

    @staticmethod
    def _set_variables(variables):
        if variables['project'] is None:
            raise Exception('Project not specified')
        if 'region' not in variables.keys():
            variables['region'] = DEFAULT_DATAFLOW_LOCATION
        return variables

    # [EWT-1001]: Added appropriate args and fn calls for job naming
    def start_java_dataflow(self, job_name, variables, dataflow, job_class=None,
        append_job_name=True, cancel_existing_job=False, context=None):
        self._validate_params(cancel_existing_job, append_job_name)
        if not append_job_name:
            # Add execution time to job name if we are not appending a random suffix
            job_name = self._add_execution_date_to_job_name(job_name, context)

        name = self._build_dataflow_job_name(job_name, append_job_name)
        variables['jobName'] = name

        def label_formatter(labels_dict):
            return ['--labels={}'.format(
                json.dumps(labels_dict).replace(' ', ''))]
        command_prefix = (["java", "-cp", dataflow, job_class] if job_class
                          else ["java", "-jar", dataflow])
        self._start_dataflow(variables, name, command_prefix, label_formatter,
            cancel_existing_job)

    # [EWT-1001]: Added appropriate args and fn calls for job naming
    def start_template_dataflow(self, job_name, variables, parameters, dataflow_template,
        append_job_name=True, cancel_existing_job=False, context=None):
        self._validate_params(cancel_existing_job, append_job_name)
        if not append_job_name:
            # Add execution time to job name if we are not appending a random suffix
            job_name = self._add_execution_date_to_job_name(job_name, context)

        variables = self._set_variables(variables)
        name = self._build_dataflow_job_name(job_name, append_job_name)
        self._start_template_dataflow(
            name, variables, parameters, dataflow_template, cancel_existing_job)

    # [EWT-1001]: Added appropriate args and fn calls for job naming
    def start_python_dataflow(self, job_name, variables, dataflow, py_options,
        append_job_name=True, cancel_existing_job=False, context=None):
        self._validate_params(cancel_existing_job, append_job_name)
        if not append_job_name:
            # Add execution time to job name if we are not appending a random suffix
            job_name = self._add_execution_date_to_job_name(job_name, context)

        name = self._build_dataflow_job_name(job_name, append_job_name)
        variables['job_name'] = name

        def label_formatter(labels_dict):
            return ['--labels={}={}'.format(key, value)
                    for key, value in labels_dict.items()]
        self._start_dataflow(variables, name, ["python3"] + py_options + [dataflow],
                             label_formatter)

    @staticmethod
    def _build_dataflow_job_name(job_name, append_job_name=True):
        base_job_name = str(job_name).replace('_', '-')

        if not re.match(r"^[a-z]([-a-z0-9]*[a-z0-9])?$", base_job_name):
            raise ValueError(
                'Invalid job_name ({}); the name must consist of'
                'only the characters [-a-z0-9], starting with a '
                'letter and ending with a letter or number '.format(base_job_name))

        if append_job_name:
            safe_job_name = base_job_name + "-" + str(uuid.uuid4())[:8]
        else:
            safe_job_name = base_job_name

        return safe_job_name

    @staticmethod
    def _build_cmd(variables, label_formatter):
        command = ["--runner=DataflowRunner"]
        if variables is not None:
            for attr, value in variables.items():
                if attr == 'labels':
                    command += label_formatter(value)
                elif value is None or value.__len__() < 1:
                    command.append("--" + attr)
                else:
                    command.append("--" + attr + "=" + value)
        return command

    # [EWT-1001]: Added appropriate args and fn calls for tracking and cancelling
    def _start_template_dataflow(self, name, variables, parameters,
                                 dataflow_template, cancel_existing_job=False):
        response = None
        tracking_existing_job, job_id = self._track_or_cancel_existing_dataflow_job(
            name, variables, cancel_existing_job)
        if not tracking_existing_job:
            # Builds RuntimeEnvironment from variables dictionary
            # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
            environment = {}
            for key in ['numWorkers', 'maxWorkers', 'zone', 'serviceAccountEmail',
                        'tempLocation', 'bypassTempDirValidation', 'machineType',
                        'additionalExperiments', 'network', 'subnetwork', 'additionalUserLabels']:
                if key in variables:
                    environment.update({key: variables[key]})
            body = {"jobName": name,
                    "parameters": parameters,
                    "environment": environment}
            service = self.get_conn()
            request = service.projects().locations().templates().launch(
                projectId=variables['project'],
                location=variables['region'],
                gcsPath=dataflow_template,
                body=body
            )
            response = request.execute(num_retries=self.num_retries)
        variables = self._set_variables(variables)
        _DataflowJob(self.get_conn(), variables['project'], name, variables['region'],
                     self.poll_sleep, job_id=job_id, num_retries=self.num_retries).wait_for_done()
        return response

    # [EWT-1001]: Added fn to add execution date as suffix to the job name
    def _add_execution_date_to_job_name(self, job_name, context):
        """Adds execution date to the dataflow job name as suffix"""
        self.log.info(
            f"Suffixing the job name {job_name} with the execution time as 'append_job_name' "
            f"is set to False"
        )
        new_job_name = f"{job_name}t{context['execution_date'].strftime('%Y-%m-%d-%H-%M')}"
        self.log.info(f"New job name: {new_job_name}")
        return new_job_name

    # [EWT-1001]: Added fn to validate new input params for dataflow operators
    @staticmethod
    def _validate_params(cancel_existing_job, append_job_name):
        # cancel_existing_job should only be set to True if append_job_name is set to False
        if cancel_existing_job and append_job_name:
            raise AirflowException(
                "'cancel_existing_job' can only be used when 'append_job_name' is set to False"
            )

    # [EWT-1001]: Added fn for job tracking and cancelling
    def _track_or_cancel_existing_dataflow_job(self, job_name, variables, cancel_existing_job):
        """Cancels any already running dataflow job with the same name"""
        try:
            project_id = variables['project']
            location = variables['region']
            dataflow_controller = _DataflowJobsController(
                dataflow=self.get_conn(),
                project_number=project_id,
                name=job_name,
                job_id=None,
                location=location,
                poll_sleep=self.poll_sleep,
                num_retries=self.num_retries,
                drain_pipeline=False,
                cancel_timeout=5*60,
                wait_until_finished=None
            )

            if cancel_existing_job:
                self.log.info(f"Cancelling any already running Dataflow job with the name {job_name}")
                dataflow_controller.cancel()
                return False, None

            else:
                self.log.info(f"Tracking any already running Dataflow job with the name {job_name}")
                jobs = dataflow_controller.get_jobs()
                if not jobs:
                    self.log.info("No existing jobs to track")
                    return False, None

                running_jobs = [
                    job for job in jobs if job["currentState"] not in DataflowJobStatus.TERMINAL_STATES
                ]

                try:
                    running_job_id = [job["id"] for job in running_jobs if job["name"] == job_name][0]
                    self.log.info(
                        f"Active job with id [{running_job_id}] found with job name [{job_name}]. "
                        "Starting to track active job progress"
                    )
                    monitoring_url = MONITORING_URL_PATTERN.format(
                        project=project_id, region=location, job_id=running_job_id
                    )
                    self.log.info(
                        f"To access the Dataflow monitoring console, please navigate to {monitoring_url}"
                    )
                    return True, running_job_id
                except IndexError:
                    # if no job is found with the provided job name
                    self.log.info(f"No running dataflow jobs found for job name {job_name}")
                    return False, None
        except Exception as err:
            err_msg = f"Error occurred when checking for existing dataflow jobs with job name: {job_name}"
            self.log.error(err_msg)
            self.log.error(err)
            raise AirflowException(err_msg)

