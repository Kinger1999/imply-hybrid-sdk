import requests
from requests import Response
from requests.auth import HTTPBasicAuth
import json

class ImplyHybridAuthenticator:

    def __init__(self, endpoint="", username="admin", password="", cert=""):
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.cert = cert
        self.basic_auth = HTTPBasicAuth(self.username, self.password)

    def test(self) -> Response:
        url = f"{self.endpoint}/status"
        response = requests.get(
            url=url,
            verify=self.cert,
            auth=self.basic_auth
        )
        return response


class Cluster:

    def __init__(self, auth: ImplyHybridAuthenticator):
        self.auth = auth

    def _get(self, url) -> Response:
        return requests.get(
            url=url,
            verify=self.auth.cert,
            auth=self.auth.basic_auth
        )

    def health(self) -> Response:
        url = f"{self.auth.endpoint}/status/health"
        return self._get(url=url)

    def status(self) -> Response:
        url = f"{self.auth.endpoint}/status"
        return self._get(url=url)

    def properties(self) -> Response:
        url = f"{self.auth.endpoint}/status/properties"
        return self._get(url=url)


class Task:

    def __init__(self, task: dict):
        self.task = task

    def id(self):
        return self.task['id']

    def group_id(self):
        return self.task['groupId']

    def type(self):
        return self.task['type']

    def created_time(self):
        return self.task['createdTime']

    def queue_insertion_time(self):
        return self.task['queueInsertionTime']

    def status_code(self):
        return self.task['statusCode']

    def status(self):
        return self.task['status']

    def runner_status_code(self):
        return self.task['runnerStatusCode']

    def duration(self):
        return self.task['duration']

    def location(self):
        return self.task['location']

    def location_host(self):
        return self.task['location']['host']

    def location_port(self):
        return self.task['location']['port']

    def location_tls_port(self):
        return self.task['location']['tlsPort']

    def datasource(self):
        return self.task['datasource']

    def error_message(self):
        return self.task['errorMsg']


class ImplyTaskApi:

    def __init__(self, auth: ImplyHybridAuthenticator):
        self.auth = auth

    def _get(self, url) -> Response:
        return requests.get(
            url=url,
            verify=self.auth.cert,
            auth=self.auth.basic_auth
        )

    def list(self) -> Response:
        url = f"{self.auth.endpoint}/druid/indexer/v1/tasks"
        return self._get(url=url)

    def completed(self) -> Response:
        url = f"{self.auth.endpoint}/druid/indexer/v1/tasks?state=complete"
        return self._get(url=url)

    def running(self) -> Response:
        url = f"{self.auth.endpoint}/druid/indexer/v1/tasks?state=running"
        return self._get(url=url)

    def waiting(self) -> Response:
        url = f"{self.auth.endpoint}/druid/indexer/v1/tasks?state=waiting"
        return self._get(url=url)

    def pending(self) -> Response:
        url = f"{self.auth.endpoint}/druid/indexer/v1/tasks?state=pending"
        return self._get(url=url)

    def task_status(self, task_id: str):
        url = f"{self.auth.endpoint}/druid/indexer/v1/task/{task_id}/status"
        return self._get(url)


class ImplyDatasourceApi:

    def __init__(self, auth: ImplyHybridAuthenticator):
        self.auth = auth

    def _get(self, url, params: dict = {}) -> Response:
        return requests.get(
            url=url,
            verify=self.auth.cert,
            auth=self.auth.basic_auth,
            params=params
        )

    def list(self, datasource: str = None) -> Response:
        url = f"{self.auth.endpoint}/druid/coordinator/v1/datasources"
        if datasource is not None:
            url = f"{self.auth.endpoint}/druid/coordinator/v1/datasources/{datasource}"
        return self._get(url=url)


class ImplyClusterCoordinator:

    def __init__(self, auth: ImplyHybridAuthenticator):
        self.auth = auth

    def _get(self, url, params: dict = {}) -> Response:
        return requests.get(
            url=url,
            verify=self.auth.cert,
            auth=self.auth.basic_auth,
            params=params
        )

    def leader(self) -> Response:
        url = f"{self.auth.endpoint}/druid/coordinator/v1/leader"
        return self._get(url=url)

    def load_status(self) -> Response:
        url = f"{self.auth.endpoint}/druid/coordinator/v1/loadstatus"
        return self._get(url=url)

    def load_status_simple(self) -> Response:
        url = f"{self.auth.endpoint}/druid/coordinator/v1/loadstatus?simple"
        return self._get(url=url)

    def load_status_full(self) -> Response:
        url = f"{self.auth.endpoint}/druid/coordinator/v1/loadstatus?full"
        return self._get(url=url)

    def load_status_cluster_view(self) -> Response:
        url = f"{self.auth.endpoint}/druid/coordinator/v1/loadstatus?full&computeUsingClusterView"
        return self._get(url=url)

    def load_queue(self) -> Response:
        url = f"{self.auth.endpoint}/druid/coordinator/v1/loadqueue"
        return self._get(url=url)

    def load_queue_full(self) -> Response:
        url = f"{self.auth.endpoint}/druid/coordinator/v1/loadqueue?full"
        return self._get(url=url)

    def load_queue_simple(self) -> Response:
        url = f"{self.auth.endpoint}/druid/coordinator/v1/loadqueue?simple"
        return self._get(url=url)

    def segments(self, datasource: str = "") -> Response:
        if len(datasource) > 0:
            params = {
                "datasource": datasource
            }
        url = f"{self.auth.endpoint}/druid/coordinator/v1/metadata/segments"
        return self._get(url=url, params=params)

    def metadata_datasources(self):
        url = f"{self.auth.endpoint}/druid/coordinator/v1/metadata/datasources"
        return self._get(url=url)

    def datasources(self, datasource: str = None):
        url = f"{self.auth.endpoint}/druid/coordinator/v1/datasources"
        if datasource is not None:
            url = f"{self.auth.endpoint}/druid/coordinator/v1/datasources/{datasource}"
        return self._get(url=url)

    def datasource_segments(self, datasource: str = None):
        if datasource is not None:
            url = f"{self.auth.endpoint}/druid/coordinator/v1/datasources/{datasource}/segments"
        return self._get(url=url)


class ImplyClusterOverlord:

    def __init__(self, auth: ImplyHybridAuthenticator):
        self.auth = auth

    def _get(self, url, params: dict = {}) -> Response:
        return requests.get(
            url=url,
            verify=self.auth.cert,
            auth=self.auth.basic_auth,
            params=params
        )

    def leader(self) -> Response:
        url = f"{self.auth.endpoint}/druid/indexer/v1/leader"
        return self._get(url=url)

    def tasks(self, params: dict = {}) -> Response:
        url = f"{self.auth.endpoint}/druid/indexer/v1/tasks"
        return self._get(url=url, params=params)

    def tasks_complete(self) -> Response:
        params = {
            "state": "complete"
        }
        return self.tasks(params=params)

    def tasks_running(self) -> Response:
        params = {
            "state": "running"
        }
        return self.tasks(params=params)

    def tasks_pending(self) -> Response:
        params = {
            "state": "pending"
        }
        return self.tasks(params=params)

    def tasks_waiting(self) -> Response:
        params = {
            "state": "waiting"
        }
        return self.tasks(params=params)


class SQLQuery:

    def __init__(self):
        self.query = None
        self.result_format = "object"
        self.context = {}
        self.header = False
        self.parameters = []

    def set_query(self, query: str = None) -> None:
        self.query = query

    def set_result_format(self, result_format: str = None) -> None:
        self.result_format = result_format

    def set_context(self, context: dict = {}) -> None:
        self.context = context

    def set_parameters(self, params: list = []) -> None:
        self.parameters = params

    def to_dict(self):
        return {
            "query": self.query,
            "resultFormat": self.result_format,
            "context": self.context,
            "header": self.header,
            "parameters": self.parameters
        }

    def to_json(self):
        return json.dumps(self.to_dict())


class ImplyClusterQuery:

    def __init__(self, auth: ImplyHybridAuthenticator):
        self.auth = auth

    def _post(self, query: SQLQuery) -> Response:
        url = f"{self.auth.endpoint}/druid/v2/sql/"
        return requests.post(url=url,
                             json=query.to_dict(),
                             verify=self.auth.cert,
                             auth=self.auth.basic_auth)

    def sql_query(self, query: SQLQuery) -> Response:
        return self._post(query)





