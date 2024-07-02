import requests
import os
import json
import asyncio
from dotenv import load_dotenv
from typing import Union, List, Dict, Any

class Zoho:
    def __init__(self):
        """
        Initialize Zoho API client with credentials from environment variables.
        """
        load_dotenv()
        self.base_auth_account = os.getenv('zohobaseauthaccount')
        self.client_id = os.getenv('zohoclientid')
        self.client_secret = os.getenv('zohoclientsecret')
        self.refresh_token = os.getenv('zohorefreshtoken')
        self.zoho_base_url = os.getenv('zohobaseurl')
        self.zoho_token = os.getenv('zohotoken')

    def _refresh(self) -> None:
        """
        Refresh Zoho OAuth token.
        """
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        params = {
            'refresh_token': self.refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'refresh_token'
        }
        url = f"https://{self.base_auth_account}/oauth/v2/token"
        response = requests.post(url, params=params, headers=headers)
        if response.status_code == 200:
            self.zoho_token = response.json()['access_token']
            os.environ['zohotoken'] == response.json()['access_token']
        else:
            raise RuntimeError(f"Failed to refresh token: {response.text}")

    def req_zoho(self, arguments:dict) -> requests.Response:
        """
        Make a request to the Zoho API.

        :param arguments: Dictionary of request parameters. Requires a full or partial url.
        :return: Response object.
        """
        arguments.setdefault('method', 'GET')
        arguments.setdefault('headers', {'Content-Type': 'application/x-www-form-urlencoded'})
        arguments['headers']['Authorization'] = f"Zoho-oauthtoken {self.zoho_token}"

        if not arguments['url'].startswith('https://'):
            arguments['url'] = f"{self.zoho_base_url}/report/{arguments['url']}"

        response = self._perform_request(arguments)

        if response.status_code == 401:
            self._refresh()
            arguments['headers']['Authorization'] = f"Zoho-oauthtoken {self.zoho_token}"
            response = self._perform_request(arguments)
        
        return response

    def _perform_request(self, args:dict) -> requests.Response:
        """
        Perform an HTTP request.

        :param args: Dictionary of request parameters.
        :return: Response object.
        """
        try:
            return requests.request(**args)
        except requests.RequestException as e:
            raise RuntimeError(f"Request failed: {e}")

class Slack:
    """
    A class to interact with the Slack API.
    """

    def __init__(self):
        """
        Initializes the Slack class with the Slack API token.
        """
        self.slack_token = os.getenv('slackapitoken')

    def req_slack(self, arguments: dict) -> requests.Response:
        """
        Makes a request to the Slack API.

        Args:
            arguments (dict): A dictionary containing at least a url suffix.

        Returns:
            requests.Response: The response from the Slack API.
        """
        arguments.setdefault('method', 'POST')
        arguments.setdefault('headers', {'Content-Type': 'application/json'})
        arguments['headers'].update({
            'Authorization': f"Bearer {self.slack_token}",
        })
        arguments['url'] = f"https://slack.com/api/{arguments['url']}"

        if arguments['headers']['Content-Type'] == 'application/json':
            arguments['data'] = json.dumps(arguments['data'])

        response = requests.request(**arguments)
        response.raise_for_status()

        response_data = response.json()
        if not response_data['ok']:
            raise requests.RequestException(response_data)

        return response   
        
    def decode_blocks(self,blocks: Union[Dict[str, Any], List[Any]]) -> str:
        """
        Extracts all values associated with keys 'text' and 'user_id' from a nested dictionary or list,
        and returns them as a single concatenated string.

        Args:
            blocks (Union[Dict[str, Any], List[Any]]): The nested dictionary or list to extract values from.

        Returns:
            str: A string containing all extracted 'text' and 'user_id' values, separated by spaces.
        """
        result = []

        def _recurse(element: Union[Dict[str, Any], List[Any]]):
            """
            Recursively traverses the nested dictionary or list and extracts values for keys 'text' and 'user_id'.

            Args:
                element (Union[Dict[str, Any], List[Any]]): The current element being traversed.
            """
            if isinstance(element, dict):
                for key, value in element.items():
                    if key == "text" or key == "user_id":
                        result.append(value)
                    if isinstance(value, (dict, list)):
                        _recurse(value)
            elif isinstance(element, list):
                for item in element:
                    _recurse(item)

        _recurse(blocks)
        return ' '.join(result)

class Hasura:
    def __init__(self):
        self.hasura_secret = os.getenv('hasurasecret')

    def req_hasura(self,arguments):
        arguments.setdefault('method', 'GET')
        arguments.setdefault('headers', {'Content-Type': 'application/json'})
        arguments['headers'].update({
            'Hasura-Client-Name':'hasura-console',
            'x-hasura-admin-secret':self.hasura_secret
        })
        arguments['url'] = f"https://prod-happy-duck-68.hasura.app/api/rest/{arguments['url']}"

        if arguments['headers']['Content-Type'] == 'application/json':
            arguments['data'] = json.dumps(arguments['data'])

        response = requests.request(**arguments)
        response.raise_for_status()

        return response   

class Fota:
    def __init__(self):
        self.fota_token = os.getenv('fotatoken')

    def req_fota(self, arguments):
        arguments.setdefault('method', 'GET')
        arguments.setdefault('headers', {'Accept':'application/json','Content-Type': 'application/json'})
        arguments['headers'].update({
            "Authorization": self.fota_token
        })
        arguments['url'] = f"https://api.teltonika.lt/{arguments['url']}"

        if arguments['headers']['Content-Type'] == 'application/json':
            arguments['data'] = json.dumps(arguments['data'])

        response = requests.request(**arguments)
        response.raise_for_status()

        return response   

    async def fota_export_devices(self,payload,waittime):
        export_req_args = {'method':'POST','url':'files','data':payload}
        self.req_fota(export_req_args) # Initiate the async devices request first
        await asyncio.sleep(waittime)
        check_file_args = {'url':'files','data':{'sort':'created_at','order':'desc'}}
        # I can't find any place that uses the FOTA async request. Might have something to do with a datapull function?
        # Leave it unfinished for now

class FluidFleet:
    def __init__(self):
        self.fluidfleettoken = os.getenv('fluidfleettoken')
        self.url = os.getenv('fluidfleeturl')
    
    def req_fluid_fleet(self,arguments):
        arguments.setdefault('method', 'GET')
        arguments.setdefault('headers', {'Accept':'application/json','Content-Type': 'application/json'})
        arguments['headers'].update({
            "Authorization": f"Bearer {self.fluidfleettoken}"
        })
        arguments['url'] = self.url + arguments['url']
        if arguments['headers']['Content-Type'] == 'application/json':
            arguments['data'] = json.dumps(arguments['data'])
        response = requests.request(**arguments)
        response.raise_for_status()

        return response   
    
