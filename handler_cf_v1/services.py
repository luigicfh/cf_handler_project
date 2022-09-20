from google.cloud import tasks_v2
import json

JOB_STATES = ["queued", "completed", "skipped", "error"]


class AbstractService:

    def __init__(self, config: dict, job: dict, app) -> None:
        self.config = config
        self.job = job
        self.app = app
        self.content_type = {"Content-type": "application/json"}

    def execute_service(self):
        pass

    def handle_success(self, db_data):

        doc_ref = db_data["db"].collection(db_data["collection"]).document(
            self.job['id'])

        doc_ref.update(self.job)

        return self.job

    def handle_error(self, error, error_handler, retry_handler, task_info, recipients, db_data):

        handler = retry_handler if self.job['retry_attempt'] < 3 else error_handler

        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": handler
            }
        }

        task["http_request"]["headers"] = self.content_type

        body = {}

        if "error_handler" in handler:
            self.job['state'] = JOB_STATES[3]
            self.job['state_msg'] = str(error)
            body['job'] = self.job
            body['error'] = str(error)
            task['http_request']['url'] = f"{handler}?to={recipients}"
        else:
            self.job['retry_attempt'] = self.job['retry_attempt'] + 1
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

        # added as a way to update de job state and error messages
        self.handle_success(db_data)

        return handler


class MissionRealty(AbstractService):

    def __init__(self, config: dict, job: dict, app) -> None:
        self.config = config
        self.job = job
        self.app = app
        super().__init__(config, job, app)

    def execute_service(self) -> dict:

        app_instance = self.app(self.config['params']['apiKey'], 'AT')

        notes = self.job['request']['notes'] if self.job['request']['notes'] else self.job['request']['disposition']

        lead = app_instance.find_leads(
            lead_phone=f"+1{self.job['request']['phone']}", lead_email=self.job['request']['email'])

        if not lead:

            lead = app_instance.add_new_lead(self.job['request'])

        lead_id = lead['leadId'] if 'leadId' in lead else lead['id']

        notes_response = app_instance.add_note(
            lead_id, notes)

        if not notes_response['success']:

            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = notes_response

        self.job['state'] = JOB_STATES[1]
        self.job['state_msg'] = notes_response

        return self.job
