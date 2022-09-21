JOB_STATES = ["queued", "completed", "skipped", "error"]


class AbstractService:

    def __init__(self, config: dict, job: dict, app) -> None:
        self.config = config
        self.job = job
        self.app = app
        self.content_type = {"Content-type": "application/json"}

    def execute_service(self):
        pass


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
