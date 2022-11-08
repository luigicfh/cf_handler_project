from .apps import *
from .utils import *
import os
from google.cloud import firestore
from bs4 import BeautifulSoup
import requests
from .decorators import func_exec_time
from datetime import datetime

JOB_STATES = ["queued", "completed", "skipped", "error"]
ENV_VAR_MSG = "Specified environment variable is not set."


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

    """
    Job data structure
    {
        "request": {
            'first_name': str,
            'last_name': str,
            'email': str,
            'type_name': str,
            'DNIS': str,
            'ANI': str,
            'campaign_name': str,
            'disposition_name': str
        },
        "state_msg": str or dict (depends on state),
        "service_instance": dict,
        "retry_attempt": int,
        "created": datetime,
        "state": str
    }
    """

    def __init__(self, config: dict, job: dict, app: Five9Custom) -> None:
        self.config = config
        self.job = job
        self.app = app
        self.search_criteria = {
            'contactIdField': 'contact_id',
            'criteria': [{'field': field, 'value': self.job['request'][field]}
                         for field in self.config['params']['searchFields']]
        }
        self.data_to_match = {value: self.job['request'][value]
                              for value in self.config['params']['searchFields']}
        self.number_to_skip = self.job['request']['DNIS'] if self.job['request'][
            'type_name'] != "Inbound" else self.job['request']['ANI']
        super().__init__(config, job, app)

    def execute_service(self):
        if all([value == "" for value in self.data_to_match.values()]):
            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = "All search values are empty"
            return self.job
        app_instance = self.app(
            self.config['params']['user'],
            self.config['params']['password']
        )
        contacts = app_instance.search_contacts(self.search_criteria)
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
            contacts['fields'], contacts['records'], self.data_to_match, self.number_to_skip)
        if len(dnc_list) == 0:
            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = "No match found in search result."
            return self.job
        self.add_to_dnc(dnc_list, app_instance)
        self.send_notification(dnc_list)
        self.job['state'] = JOB_STATES[1]
        self.job['state_msg'] = {
            "numbersToDnc": dnc_list,
            "skippedNumber": self.number_to_skip
        }
        return self.job

    def get_exact_match(self, fields: list, values: list, request: dict, skipped_number: str) -> list:
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
                    if value['values']['data'][number_field_index] == skipped_number:
                        continue
                    dnc_list.append(value['values']['data']
                                    [number_field_index])
        return dnc_list

    def add_to_dnc(self, numbers: list, app_instance) -> int:
        if len(numbers) == 6 or len(numbers) == 5:
            list1 = numbers[:3]
            list2 = numbers[3:]
            response1 = app_instance.configuration.addNumbersToDnc(list1)
            response2 = app_instance.configuration.addNumbersToDnc(list2)
            return response1 + response2
        return app_instance.configuration.addNumbersToDnc(numbers)

    def send_notification(self, dnc_list):
        for_markdown = {
            "lead_name": f"{self.job['request']['first_name']} {self.job['request']['last_name']}",
            "campaign": self.job['request']['campaign_name'],
            "disposition": self.job['request']['disposition_name'],
            "target_number": self.number_to_skip,
            "dnc_numbers": ",".join(dnc_list)
        }
        markdown = generate_markdown(for_markdown)
        sender = os.environ.get('SENDER', ENV_VAR_MSG)
        password = os.environ.get('PASSWORD', ENV_VAR_MSG)
        recipients = os.environ.get('RECIPIENTS', ENV_VAR_MSG).split(",")
        subject = f"AT Central Notifications | Person Of Interest Identified"
        body = f"""
            A new person of interest has been identified for campaign {for_markdown['campaign']}<br>
            All other {len(dnc_list)} numbers were added to the DNC list.<br>∫
            {markdown}
        """
        return send_email(sender, password, recipients, subject, body)


ROT_TYPES = ["spam_detection", "auto_rotation", "on_demand"]
REQ_TYPES = ["auto_request", "spam_request"]


class AniRotationEngine(AbstractService):
    """
    ENV variables
    SENDER=str
    PASSWOR=str
    """

    """
    Service Configuration Structure
    {
        'className': 'str',
        'webHook': 'str',
        'appClassName': 'str',
        'params': {
            'project': 'str',
            'collection': 'str',
            'user': 'str',
            'password': 'str'
        },
        'created': DatetimeWithNanoseconds,
        'webHookDev': 'str',
        'name': 'str'
    }
    """

    """
    Job data structure
    {
        "request": {
            'field': str,
            'type': str,
            'schedule': str
        },
        "state_msg": str or dict (depends on state),
        "service_instance": dict,
        "retry_attempt": int,
        "created": datetime,
        "state": str
    }
    """

    def __init__(self, config: dict, job: dict, app: Five9Custom) -> None:
        self.config = config
        self.job = job
        self.app = app
        self.robo_url = 'https://www.nomorobo.com/lookup/{}'
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.8',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
        }

        super().__init__(config, job, app)

    def execute_service(self):
        db = firestore.Client(self.config['params']['project'])
        ani_rot_collection = self.config['params']['collection']
        field = self.job['request']['field']
        req_type = self.job['request']['type']
        if req_type != ROT_TYPES[2]:
            query = query_doc(db, ani_rot_collection, field,
                              "==", self.job['request']['schedule'])
        else:
            query = get_doc(db, ani_rot_collection,
                            self.job['request']['id'])
        if len(query) == 0:
            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = "No items configured for service."
            return self.job
        if req_type == ROT_TYPES[0]:
            affected_profiles = self._execute_spam_service(
                query, db, ani_rot_collection)
            self.job['state'] = JOB_STATES[1]
            self.job['state_msg'] = {
                "success": True,
                "affected_profiles": affected_profiles
            }
            return self.job
        elif req_type == ROT_TYPES[1]:
            affected_profiles = self._execute_auto_rotation_service(
                query, db, ani_rot_collection)

            self.job['state'] = JOB_STATES[1]
            self.job['state_msg'] = {
                "success": True,
                "affected_profiles": affected_profiles
            }
            return self.job
        elif req_type == ROT_TYPES[2]:
            self._execute_on_demand_service(query)
            self.job['state'] = JOB_STATES[1]
            self.job['state_msg'] = {
                "success": True
            }
        elif req_type in REQ_TYPES:
            self._execute_new_request_service(
                query, db, ani_rot_collection, req_type)
            self.job['state'] = JOB_STATES[1]
            self.job['state_msg'] = {
                "success": True
            }
        return super().execute_service()

    def _execute_on_demand_service(self, config):
        app_instance = self.app(
            self.config['params']['user'],
            self.config['params']['password']
        )
        profile_obj = app_instance.get_campaign_profile(
            config['configuration']['profiles'][0])
        profile_config = {
            "ANI": config['configuration']['aniPool'][0]['ani'],
            "description": profile_obj['description'],
            "dialingSchedule": profile_obj['dialingSchedule'],
            "dialingTimeout": profile_obj['dialingTimeout'],
            "initialCallPriority": profile_obj['initialCallPriority'],
            "maxCharges": profile_obj['maxCharges'],
            "name": profile_obj['name'],
            "numberOfAttempts": profile_obj['numberOfAttempts'],
        }
        app_instance.update_campaign_profile(profile_config)
        old_ani = config['configuration']['aniPool'][1]['ani'] if len(
            config['configuration']['aniPool']) > 1 else 'ANI deleted from pool.'
        return self.notify_change(config['configuration']['aniPool'][0]['ani'], old_ani, ROT_TYPES[2], config['configuration']['notifications']['to'], config['configuration']['notifications']['cc'], config['configuration']['profiles'][0])

    def _execute_new_request_service(self, query, db, collection, req_type):
        for config in query:
            config_dict = config.to_dict()
            self.send_new_request(config_dict, req_type)
            update_doc(db, collection, config.id, config_dict)
        return

    def _execute_auto_rotation_service(self, query, db, collection):
        app_instance = self.app(
            self.config['params']['user'],
            self.config['params']['password']
        )
        affected_profiles = []
        for config in query:
            config_dict = config.to_dict()
            if len(config_dict['configuration']['aniPool']) == 1:
                continue
            if (all([ani['isSpam'] for ani in config_dict['configuration']['aniPool']])):
                continue
            if config_dict['configuration']['aniPool'][1]['isSpam']:
                update_doc(db, collection, config.id, config_dict)
                continue
            new_ani_pool = self.rotate_ani(
                config_dict['configuration']['aniPool'],
                config_dict['configuration']['profiles'][0],
                app_instance)
            config_dict['configuration']['aniPool'] = new_ani_pool
            update_doc(db, collection, config.id, config_dict)
            self.notify_change(
                new_ani_pool[0]['ani'],
                new_ani_pool[-1]['ani'],
                self.job['request']['type'],
                config_dict['configuration']['notifications']['to'],
                config_dict['configuration']['notifications']['cc'],
                config_dict['configuration']['profiles'][0])
            affected_profiles.append(
                config_dict['configuration']['profiles'][0])
        return affected_profiles

    def _execute_spam_service(self, query, db, collection):
        app_instance = self.app(
            self.config['params']['user'],
            self.config['params']['password']
        )
        affected_profiles = []
        for config in query:
            config_dict = config.to_dict()
            if len(config_dict['configuration']['aniPool']) == 1:
                continue
            if (all([ani['isSpam'] for ani in config_dict['configuration']['aniPool']])):
                self.send_new_request(config_dict, REQ_TYPES[1])
                continue
            is_spam = self._spam_detection(
                config_dict['configuration']['aniPool'][0]['ani'])
            if not is_spam:
                continue
            config_dict['configuration']['aniPool'][0]['isSpam'] = True
            if config_dict['configuration']['aniPool'][1]['isSpam']:
                update_doc(db, collection, config.id, config_dict)
                continue
            new_ani_pool = self.rotate_ani(
                config_dict['configuration']['aniPool'],
                config_dict['configuration']['profiles'][0],
                app_instance)
            config_dict['configuration']['aniPool'] = new_ani_pool
            update_doc(db, collection, config.id, config_dict)
            self.notify_change(
                new_ani_pool[0]['ani'],
                new_ani_pool[-1]['ani'],
                self.job['request']['type'],
                config_dict['configuration']['notifications']['to'],
                config_dict['configuration']['notifications']['cc'],
                config_dict['configuration']['profiles'][0])
            affected_profiles.append(
                config_dict['configuration']['profiles'][0])
        return affected_profiles

    def _spam_detection(self, ani):
        ani_with_dashes = "{}-{}-{}".format(ani[:3], ani[3:6], ani[6::])
        with requests.Session() as s:
            response = s.get(url=self.robo_url.format(
                ani_with_dashes), headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        for script in soup(["script", "style", "br", "footer", "ul", "nav"]):
            script.extract()
        text = (soup.get_text().replace('\n', '').strip())
        answer = "404" not in text
        return answer

    def rotate_ani(self, ani_pool: list, profile, client):
        profile = client.get_campaign_profile(profile)
        inbound_campaigns = [
            c['name'] for c in client.get_inbound_campaigns() if c['profileName'] == profile]
        profile_config = {
            "ANI": ani_pool[1]['ani'],
            "description": profile['description'],
            "dialingSchedule": profile['dialingSchedule'],
            "dialingTimeout": profile['dialingTimeout'],
            "initialCallPriority": profile['initialCallPriority'],
            "maxCharges": profile['maxCharges'],
            "name": profile['name'],
            "numberOfAttempts": profile['numberOfAttempts'],
        }
        client.update_campaign_profile(profile_config)
        for campaign in inbound_campaigns:
            client.update_dnis_list(campaign, [ani_pool[1]['ani']])
        deactivated_ani = ani_pool.pop(0)
        deactivated_ani['active'] = False
        ani_pool.append(deactivated_ani)
        ani_pool[0]['active'] = True
        return ani_pool

    def send_new_request(self, config, reason):
        today = datetime.now().isoformat().split("T")[0]
        area_codes = config['configuration']['requestSchedule']['areaCodes']
        amount = 0
        if not area_codes:
            return
        # accounts for first request
        if 'newAniRequestData' not in config['configuration']:
            if config['configuration']['requestSchedule']['onlyWhenSpam']:
                amount = 4
            else:
                amount = 4
            if amount == 0:
                return
            self.send_request(config, amount)
            config['configuration']['newAniRequestData'] = {
                'requested_on': today,
                'reason': reason,
                'amount': amount
            }
        # accounts for normal consecutive request
        elif config['configuration']['newAniRequestData']['reason'] == REQ_TYPES[0]:
            if config['configuration']['requestSchedule']['onlyWhenSpam']:
                amount = 4
            else:
                amount = 4
            if amount == 0:
                return
            self.send_request(config, amount)
            config['configuration']['newAniRequestData'] = {
                'requested_on': today,
                'reason': reason,
                'amount': amount
            }
        else:
            # if all anis are still spam
            if (all([ani['isSpam'] for ani in config['configuration']['aniPool']])):
                return
            if config['configuration']['requestSchedule']['onlyWhenSpam']:
                amount = len(
                    [ani for ani in config['configuration']['aniPool'] if ani['isSpam']])
            else:
                amount = len(
                    [ani for ani in config['configuration']['aniPool']])

            if amount == 0:
                return
            self.send_request(config, amount)
            config['configuration']['newAniRequestData'] = {
                'requested_on': today,
                'reason': reason,
                'amount': amount
            }
        return config

    def send_request(self, config, amount):
        sender = os.environ.get('SENDER', ENV_VAR_MSG)
        password = os.environ.get('PASSWORD', ENV_VAR_MSG)
        recipients = config['configuration']['requestSchedule']['recipients'].split(
        ) + config['configuration']['requestSchedule']['cc'].split()
        subject = "New DID request"
        body = f"""
        Hi {config['configuration']['requestSchedule']['recipients'].split(".")[0]} <br><br>
        Can we please order {amount} new number{"s" if amount > 1 else ""} for {"any of the" if len(config['configuration']['requestSchedule']['areaCodes'].split(",")) > 1 else "the"} area code{"s" if len(config['configuration']['requestSchedule']['areaCodes'].split(",")) > 1 else ""}
         listed below:<br><br>
         {"<br>".join(config['configuration']['requestSchedule']['areaCodes'].split(","))}
         <br>
         Thanks!
        """
        return send_email(sender, password, recipients, subject, body)

    def notify_change(self, new_ani, old_ani, reason, recipients, cc, profile):
        sender = os.environ.get('SENDER', ENV_VAR_MSG)
        password = os.environ.get('PASSWORD', ENV_VAR_MSG)
        recipients_list = recipients.split(",") + cc.split(",")
        if len(recipients_list) == 0:
            return
        subject = f"ANI Rotation Notifications | New ANI Activated For {profile}"
        body = f"""
        A new ANI has been activated for {profile} by the {reason.replace("_", " ").capitalize()} service.<br>
        New ANI: {new_ani}<br>
        Old ANI: {old_ani}
        """
        return send_email(sender, password, recipients_list, subject, body)


class Five9ToMySQL(AbstractService):

    def __init__(self, config: dict, job: dict, app: SQLDB) -> None:
        self.config = config
        self.job = job
        self.app = app
        self.data = self.parse_post_keys(self.job['request'])
        self.table = self.config['params']['db_credentials']['table']
        super().__init__(config, job, app)

    def execute_service(self):
        app_instance = self.app(self.config['params']['db_credentials'])
        table_columns = self.get_db_columns(app_instance)
        values = self.get_db_values(table_columns)
        self.insert(app_instance, table_columns, values)
        self.job['state'] = JOB_STATES[1]
        self.job['state_msg'] = {
            "message": "success"
        }
        return self.job

    def insert(self, db_engine: SQLDB, columns: list, values: list):
        query_string = f"""INSERT INTO {self.table} ({", ".join(columns)}) VALUES ({", ".join(['%s' for col in columns])})"""
        result = db_engine.execute_sql(query_string, values)
        return result

    def set_dynamic_fields(self):
        live_answer = {'live_answer': 'Yes' if self.data['disposition_name']
                       in self.config['params']['live_answer'] else "No"}
        conversation = {'conversation': 'Yes' if self.data['disposition_name']
                        in self.config['params']['conversation'] else "No"}
        created_date_time = {
            'created_date_time': datetime.now()}
        self.data[list(live_answer.keys())[0]] = list(live_answer.values())[0]
        self.data[list(conversation.keys())[0]] = list(
            conversation.values())[0]
        self.data[list(created_date_time.keys())[0]] = list(
            created_date_time.values())[0]

    def get_db_columns(self, db_engine):
        self.set_dynamic_fields()
        query = db_engine.execute_sql(f'SHOW columns FROM {self.table}')
        return [column[0] for column in query if column[0] != 'id' and column[0].lower() in self.data]

    def get_db_values(self, columns):
        return [self.data[col.lower()] for col in columns if col.lower() in self.data]

    def parse_post_keys(self, request):
        parsed_request = {}
        for key in request.keys():
            if " " in key:
                new_key = key.replace(" ", "_").lower()
                parsed_request[new_key] = request[key]
                self.parse_post_date_time(
                    new_key, parsed_request[new_key], parsed_request)
            else:
                new_key = key.lower()
                parsed_request[new_key] = request[key]
                self.parse_post_date_time(
                    new_key, parsed_request[new_key], parsed_request)
        return parsed_request

    def parse_post_date_time(self, new_key, value, request):
        if "date" in new_key and 'time' not in new_key:
            request[new_key] = '{}-{}-{}'.format(
                value[4:6], value[6:8], value[:4])
        if 'date' in new_key and 'time' in new_key:
            request[new_key] = datetime.now().strptime("%m/%d/%Y, %H:%M:%S")


class LeviKvCore(AbstractService):

    def __init__(self, config: dict, job: dict, app: KvCore) -> None:
        self.config = config
        self.job = job
        self.app = app
        self.notes_title = "Appointments Today Notes Update"
        super().__init__(config, job, app)

    def execute_service(self):
        app_instance = self.app(self.config['params']['apiToken'])
        contact = app_instance.get_contact(self.job['request']['email'])
        if contact is None:
            self.job['state'] = JOB_STATES[2]
            self.job['state_msg'] = f"Contact not found with email: {self.job['request']['email']}"
            return self.job
        notes = self.job['request']['comments'] if self.job['request']['comments'] != "" else self.job['request']['disposition_name']
        notes_response = app_instance.update_notes(
            contact['id'], self.notes_title, notes
        )
        self.job['state'] = JOB_STATES[1]
        self.job['state_msg'] = notes_response
        return self.job
