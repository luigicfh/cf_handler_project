from .apps import *
import uuid
from google.cloud import tasks_v2
import json

JOB_STATES = ["queued", "completed", "skipped", "error"]


class AbstractService:

    def __init__(self, config: dict, job: dict) -> None:
        self.config = config
        self.job = job
        self.app = None
        self.content_type = {"Content-type": "application/json"}

    def execute_service(self):
        pass

    def handle_success(self, db, collection):

        doc_ref = db.collection(collection).document(
            self.job['id'])

        doc_ref.update(self.job)

        return super().handle_success(db, collection)

    def handle_error(self, error, error_handler, retry_handler, task_info):

        handler = error_handler if self.job['retry_attempt'] < 3 else retry_handler

        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": handler
            }
        }

        task["http_request"]["headers"] = self.content_type

        body = {}

        if "error_handler" in handler:
            body['job'] = self.job
            body['error'] = error
        else:
            self.job['retry_attempt'] += 1
            body = self.job

        task["http_request"]["body"] = json.dumps(body).encode()

        client = tasks_v2.CloudTasksClient()

        parent = client.queue_path(
            task_info['project'],
            task_info['location'],
            task_info['queue']
        )

        client.create_task(
            request={"parent": parent, "task": task}
        )

        return super().handle_error()


class MissionRealty(AbstractService):

    def __init__(self, config: dict, job: dict) -> None:
        self.config = config
        self.job = job
        self.app = SierraInteractive(self.config['params']['apiKey'], 'AT')
        super(MissionRealty, self).__init__(config, job)

    def execute_service(self) -> dict:

        lead = self.app.find_leads(
            f"+1{self.job['request']['phone']}", self.job['request']['email'])

        if not lead:

            lead = self.app.add_new_lead(self.job['request'])

        notes_response = self.app.add_note(lead['id'])

        if not notes_response['success']:

            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = notes_response

        self.job['state'] = JOB_STATES[1]
        self.job['state_msg'] = notes_response

        return self.job