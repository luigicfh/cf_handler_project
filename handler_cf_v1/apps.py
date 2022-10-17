from typing import Any
import requests
import json
from .exceptions import ApiError
from five9 import Five9
from ast import literal_eval
import MySQLdb
import datetime

class SierraInteractive:

    def __init__(self, api_key: str, originating_system: str) -> None:
        self.api_key = api_key
        self.find_leads_ep = "https://api.sierrainteractivedev.com/leads/find?{}"
        self.add_note_ep = "https://api.sierrainteractivedev.com/leads/{}/note"
        self.retrieve_lead_details_ep = "https://api.sierrainteractivedev.com/leads/get/{}"
        self.add_new_lead_ep = "https://api.sierrainteractivedev.com/leads"
        self.headers = {
            "Content-Type": "application/json",
            "Sierra-ApiKey": self.api_key,
            "Sierra-OriginatingSystemName": originating_system
        }

    def find_leads(self, lead_phone: str, lead_email: str) -> Any:
        """
        Returns the lead object of the first record in the array returned by Sierra API.
        :param str lead_phone: the phone number of the lead to search for, i.e. +13233455555.
        :param str lead_email: the email of the lead to search for.
        :return None if no lead is found or the lead data (dict) if at least one is found.
        :raises ApiError when response status code is not equal to 200.
        """

        if not lead_email:

            response = requests.get(
                self.find_leads_ep.format(f'phone={lead_phone.strip()}'),
                headers=self.headers
            )

            if response.status_code != 200:

                raise ApiError(response.status_code)

            json_response = response.json()

            if json_response['data']['totalRecords'] > 0:

                return json_response['data']['leads'][0]

            return None

        response = requests.get(
            self.retrieve_lead_details_ep.format(lead_email.strip()),
            headers=self.headers
        )

        if response.json()['success'] == True:

            return response.json()['data']

        return None

    def add_new_lead(self, payload: dict):
        """
        Returns the lead object of the record created in Sierra API.
        :param dict payload: the data to POST to the Add New Lead EP using the specifications required by Sierra
        https://api.sierrainteractivedev.com/#leads-create
        Example payload:
        {
            "firstName": "John",
            "lastName": "Doe",
            "email": "johndoe@server.com",
            "password": "123456",
            "emailStatus": "TwoWayEmailing",
            "phone": "(123) 456-7890",
            "phoneStatus": "TalkingToProspect",
            "birthDate": "2000-01-21",
            "referralFee": true,
            "sendRegistrationEmail": true,
            "note": "Some note",
            "leadType": 1,
            "source": "Lead source",
            "shortSummary": "Just looking",
            "tags": [ "Tag_1", "Tag_2"],
            "partnerLink": "https://partern-site.com/lead-page/123",
            "assignTo": {
                "agentSiteId": 123456,
                "agentUserId": 234567,
                "agentUserEmail": "agent@site.com"
            }
        }
        :return the lead object (dict) of the record created in Sierra API.
        :raise ApiError when lead creation is not successful.
        """

        if not payload['email']:

            raise Exception("Email is required for creating leads")

        response = requests.post(
            url=self.add_new_lead_ep,
            headers=self.headers,
            data=json.dumps(payload)
        )

        if response.status_code != 200:

            raise ApiError(response.status_code)

        return response.json()['data']

    def add_note(self, lead_id: str, notes: str) -> Any:
        """
        Add note  to lead in Sierra.
        :param str lead_id: the ID of the lead to update.
        :param str notes: the The notes to add to the lead.
        :return dict with success response
        :raises ApiError when response status code is not equal to 200.
        """

        message = {
            "message": notes
        }

        response = requests.post(
            url=self.add_note_ep.format(lead_id),
            headers=self.headers,
            data=json.dumps(message)
        )

        if response.status_code != 200:

            raise ApiError(response.status_code)

        return response.json()


class Five9Custom(Five9):

    def __init__(self, username, password):
        super().__init__(username, password)

    def search_contacts(self, criteria):

        response = self.configuration.getContactRecords(
            lookupCriteria=criteria)

        return literal_eval(str(response))

    def get_campaign_profile(self, profile_name):

        response = self.configuration.getCampaignProfiles(
            namePattern=profile_name)

        return literal_eval(str(response[0]))

    def update_campaign_profile(self, profile_confing):

        return self.configuration.modifyCampaignProfile(profile_confing)

class Five9ToMySQL:

    def __init__(self, request: json, config: dict) -> None:

        self.data = self.parse_post_keys(request)
        self.config = config
        self.connection = MySQLdb.connect(host=config['host'], user=config['db_user'], passwd=config['db_password'], db=config['db'], port=3306, charset='utf8')
        self.table = config['table']
        self.columns = self.get_db_colums()
        self.values = self.get_db_values()

    def parse_post_keys(self, post: json) -> dict:
        parsed_post = {}
        for key in post.keys():
            if " " in key:
                new_key = key.replace(" ", "_").lower()
                parsed_post[new_key] = post[key]
                self.parse_post_date_time(new_key, parsed_post[new_key], parsed_post)
            else:
                new_key = key.lower()
                parsed_post[new_key] = post[key]
                self.parse_post_date_time(new_key, parsed_post[new_key], parsed_post)

        return parsed_post

    def parse_post_date_time(self, new_key: str, value: str, post: dict) -> None:
        if "date" in new_key and 'time' not in new_key:
            post[new_key] = '{}-{}-{}'.format(value[4:6], value[6:8], value[:4])
        elif 'date' in new_key and 'time' in new_key:

            post[new_key] = datetime.now().strptime("%m/%d/%Y, %H:%M:%S")
        else:
            pass 

    def set_dynamic_fields(self) -> None:
        live_answer = {
            'live_answer': 'Yes' if self.data['disposition_name'] in self.config['live_answer'] else 'No'
        }

        conversation = {
            'conversation': 'Yes' if self.data['disposition_name'] in self.config['conversation'] else 'No'
        }

        created_date_time = {
            'created_date_time': datetime.now()
        }

        self.data[list(live_answer.keys())[0]] = list(live_answer.values())[0]
        self.data[list(conversation.keys())[0]] = list(conversation.values())[0]
        self.data[list(created_date_time.keys())[0]] = list(created_date_time.values())[0]

    def get_db_colums(self) -> list:

        self.set_dynamic_fields()

        with self.connection.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM {}".format(self.table))
            columns = cursor.fetchall()
        return [column[0] for column in columns if column[0] != 'id' and column[0].lower() in self.data]

    def get_db_values(self) -> list:

        return [self.data[column.lower()] for column in self.columns if column.lower() in self.data]

    def insert_data(self) -> None:
            
        with self.connection.cursor() as cursor:
            query = "INSERT INTO {} ({}) VALUES ({})".format(self.table, ", ".join(self.columns), ", ".join(["%s"] * len(self.values)))
            cursor.execute(query, self.values)
            self.connection.commit()
        self.connection.close()