from .apps import *
from google.cloud import firestore
import uuid
from datetime import datetime
JOB_STATES = ["queued", "completed", "skipped", "error"]


class AbstractService:

    def __init__(self, config: dict, job: dict, app) -> None:
        self.config = config
        self.job = job
        self.app = app

    def execute_service(self):
        pass


class MissionRealty(AbstractService):

    def __init__(self, config: dict, job: dict, app: SierraInteractive) -> None:
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


class OwnLaHomes(AbstractService):

    def __init__(self, config: dict, job: dict, app: SierraInteractive) -> None:
        self.config = config
        self.job = job
        self.app = app
        super().__init__(config, job, app)

    def execute_service(self):

        app_instance = self.app(self.config['params']['apiKey'], 'AT')

        notes = self.job['request']['notes'] if self.job['request']['notes'] else self.job['request']['disposition']

        lead = app_instance.find_leads(
            lead_phone=f"+1{self.job['request']['phone']}", lead_email=self.job['request']['email'])

        if not lead:

            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = "Lead not found, update skipped"

            return self.job

        lead_id = lead['leadId'] if 'leadId' in lead else lead['id']

        notes_response = app_instance.add_note(
            lead_id, notes)

        if not notes_response['success']:

            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = notes_response

        self.job['state'] = JOB_STATES[1]
        self.job['state_msg'] = notes_response

        return self.job


class MultiLeadUpdate(AbstractService):

    def __init__(self, config: dict, job: dict, app: Five9Custom) -> None:
        self.config = config
        self.job = job
        self.app = app
        super().__init__(config, job, app)

    def execute_service(self):

        app_instance = self.app(self.config['params']
                                ['user'], self.config['params']['password'])

        search_criteria = {
            'contactIdField': 'contact_id',
            'criteria': [{'field': field, 'value': self.job['request'][field]}
                         for field in self.job['request'].keys()]
        }

        contacts = app_instance.search_contacts(search_criteria)

        if contacts is None:
            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = "No records found."
            return self.job

        if len(contacts['records']) == 1000 or len(contacts['records']) == 1:

            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = f"Too many records found: ${len(contacts['records'])}" if len(
                contacts['records']) == 1000 else f"No duplicate contacts found."
            return self.job

        dnc_list = self.get_exact_match(
            contacts['fields'], contacts['records'], self.job['request'])

        if len(dnc_list) == 0:

            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = "No match found in search result."

            return self.job

        dnc_job = {
            "created": datetime.now(),
            "numbers": dnc_list,
            "request": self.job['request']
        }

        dnc_job_id = self.add_to_dnc_jobs(dnc_job)

        self.job['state'] = JOB_STATES[1]
        self.job['state_msg'] = {
            "numbersToDnc": dnc_list,
            "dncJobId": dnc_job_id
        }

        return self.job

    def get_exact_match(self, fields: list, values: list, request: dict):

        dnc_list = []

        indexes = [fields.index(field) for field in request.keys()]

        for value in values:

            extracted_values = [value['values']['data'][index] if value['values']
                                ['data'][index] is not None else "" for index in indexes]

            if extracted_values.sort() == list(request.values()).sort():

                for i in range(3):

                    number_field_index = fields.index(
                        f"number{i+1}")

                    if value['values']['data'][number_field_index] is None:
                        continue

                    dnc_list.append(value['values']['data']
                                    [number_field_index])

        return dnc_list

    def add_to_dnc_jobs(self, doc):

        db = firestore.Client(self.config['params']['project'])

        job_id = str(uuid.uuid4())

        doc_ref = db.collection(
            self.config['params']['dncCollection']).document(job_id)

        doc_ref.set(doc)

        return job_id
