from .apps import *

JOB_STATES = ["queued", "completed", "skipped", "error"]


class AbstractService:

    def __init__(self, config: dict, job: dict) -> None:
        self.config = config
        self.job = job
        self.app = None

    def execute_service(self):
        pass


class MissionRealty(AbstractService):

    def __init__(self, config: dict, job: dict) -> None:
        self.config = config
        self.job = job
        self.app = SierraInteractive(self.config['params']['apiKey'], 'AT')
        super(MissionRealty, self).__init__(config, job)

    def execute_service(self) -> dict:

        lead = self.app.find_leads(
            self.job['request']['phone'], self.job['request']['email'])

        if not lead:

            lead = self.app.add_new_lead(self.job['request'])

        notes_response = self.app.add_note(lead['id'])

        if not notes_response['success']:

            self.job['request']['state'] = JOB_STATES[3]

        self.job['request']['state'] = JOB_STATES[1]

        return self.job
