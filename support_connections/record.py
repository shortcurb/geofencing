import os
import json
import logging

import requests
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv
from typing import Dict, Any, List

class Record:
    """
    A class to represent a record with parsed data.
    """

    def __init__(self, record_dict: Dict[str, Any]) -> None:
        """
        Initializes the Record instance by parsing the input dictionary.

        Parameters
        ----------
        record_dict : dict
            The dictionary containing record data.
        """
        self._parse_dict(record_dict)

    def _parse_dict(self, data: Dict[str, Any], parent_key: str = '') -> None:
        """
        Recursively parses a dictionary and sets attributes on the instance.

        Parameters
        ----------
        data : dict
            The dictionary to parse.
        parent_key : str
            The base key to use for nested attributes.
        """
        for key, value in data.items():
            key = key.replace(' ','_')
            full_key = f"{parent_key}_{key}" if parent_key else key
            if isinstance(value, dict):
                self._parse_dict(value, full_key)
            elif isinstance(value, str):
                try:
                    parsed_value = json.loads(value)
                    if isinstance(parsed_value, dict):
                        self._parse_dict(parsed_value, full_key)
                    else:
                        setattr(self, full_key, value)
                except json.JSONDecodeError:
                    try:
                        parsed_value = datetime.fromisoformat(value)
                        setattr(self, full_key, parsed_value)
                    except ValueError:
                        setattr(self, full_key, value)
            else:
                setattr(self, full_key, value)

    def __getattr__(self, name: str) -> Any:
        """
        Returns None for attributes that do not exist.

        Parameters
        ----------
        name : str
            The name of the attribute.

        Returns
        -------
        None
            If the attribute does not exist.
        """
        return None

    def __repr__(self):
        return f"Record({self.__dict__})"

class RecordFetcher:
    """
    A class to fetch data from a specified API using environment variables for configuration.

    Attributes
    ----------
    base_url : str
        The base URL for the API endpoint.
    auth_token : str
        The authentication token for accessing the API.
    """

    def __init__(self):
        """
        Initializes the DataFetcher by loading environment variables and setting the base URL and authentication token.

        Raises
        ------
        ValueError
            If the 'ccurl' or 'cctoken' environment variables are not set.
        """
        load_dotenv()
        self.base_url = os.getenv('ccurl')
        self.auth_token = os.getenv('cctoken')

        if not self.base_url:
            raise ValueError("The 'ccurl' environment variable is not set.")
        if not self.auth_token:
            raise ValueError("The 'cctoken' environment variable is not set.")
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _get_one_page_historical_records(self, ismi:int, start:datetime, end:datetime, page_number:int) -> List[Any]:
        """
        Fetches a single page of historical records from the API.

        Parameters
        ----------
        ismi : int
            The ISMI for which to fetch records.
        start : datetime
            The start datetime for the records.
        end : datetime
            The end datetime for the records.
        page_num : int
            The page number to fetch.

        Returns
        -------
        dict
            The JSON response from the API.

        Raises
        ------
        RuntimeError
            If the request fails.
        """
        start_str = datetime.strftime(start,'%m/%d/%y %H:%M:%S')
        end_str = datetime.strftime(end,'%m/%d/%y %H:%M:%S')
        url = f"{self.base_url}/responses/{ismi}"
        params = {
            'page':page_number,
            'pageSize':100,
            'startDateUTC':start_str,
            'endDateUTC':end_str,
            'includeJSON':True
            }
        headers = {
            'Authorization':f"Basic {self.auth_token}",
            'Content-Type':'x-www-form-urlencoded'
            }
        
        self.logger.info(f"Fetching page {page_number} of records for ISMI {ismi}")
        response = requests.get(url,params=params,headers = headers)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            self.logger.info(f"API responded with 404, indicating no records between {start} and {end} for ismi {ismi} in page number {page_number}")
            return []
        else:
            self.logger.error(f"API responded with {response.status_code}")
            e = requests.exceptions.HTTPError
            raise RuntimeError(f"Request failed {e}")

    def get_historical_records(self, ismi: int, start: str, end: str) -> List[Dict[str, Any]]:
        """
        Fetches multiple records between a given start and end time for a given ISMI, from the API.

        Parameters
        ----------
        ismi : int
            The ISMI for which to fetch and parse the records.
        start : datetime
            The start datetime for the records.
        end : datetime
            The end datetime for the records.

        Returns
        -------
        list of dict
            A list of record dicts.
        """
        records = []
        page_number = 1
        page_limit = 100

        while page_number < page_limit:
            data = self._get_one_page_historical_records(ismi, start, end, page_number)
            if not data:
                break
            records.extend(data)
            page_number += 1

        return records

    def get_and_parse_historical_records(self, ismi:int, start:datetime, end:datetime) -> 'Record':
        """
        Fetches and parses multiple records for a given ISMI from the API.

        Parameters
        ----------
        ismi : int
            The ISMI for which to fetch and parse the records.

        Returns
        -------
        list of Record
            A list of Record instances.
        """
        records_data = self.get_historical_records(ismi,start,end)
        return [self.parse_record(record) for record in records_data]

    def get_latest_record(self, ismi: int) -> Dict[str, Any]:
        """
        Fetches the latest record for a given ISMI from the API.

        Parameters
        ----------
        ismi : int
            The ISMI (International Station Mobile Identity) for which to fetch the latest record.

        Returns
        -------
        dict
            The JSON response from the API as a dictionary.

        Raises
        ------
        RuntimeError
            If the API request fails.
        """
        url = f"{self.base_url}/responses/lastRecord/{ismi}?includeJSON=true"
        headers = {'Authorization': f"Basic {self.auth_token}"}
        response = requests.get(url, headers=headers)
        
        # Check for HTTP errors
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"Request failed: {e}")

        return response.json()

    def parse_record(self, record_dict: Dict[str, Any]) -> 'Record':
        """
        Parses the record dictionary and returns a Record object.

        Parameters
        ----------
        record_dict : dict
            The dictionary containing record data.

        Returns
        -------
        Record
            An instance of the Record class with parsed data.
        """
        return Record(record_dict)

    def get_and_parse_latest_record(self, ismi: int) -> 'Record':
        """
        Fetches and parses the latest record for a given ismi.

        Parameters
        ----------
        ismi : int
            The ismi for which to fetch and parse the latest record.

        Returns
        -------
        Record
            An instance of the Record class with the parsed data from the latest record.
        """
        return(self.parse_record(self.get_latest_record(ismi)))


if __name__ == '__main__':
    ismi = 310170874498913
    records = RecordFetcher()
    lr = records.get_and_parse_latest_record(ismi)
#    print(lr)
    stop = datetime.now()
    start = stop-timedelta(hours=12)

    ar = RecordFetcher().get_and_parse_historical_records(ismi,start,stop)
    for item in ar:
        for attr, value in item.__dict__.items():
            print(f"{attr}: {value}")
        print('\n')

'''
Example record output:

ismi: 310170874499867
event_date: 2024-06-04 15:42:44+00:00
coordinates_x: -96.10099
coordinates_y: 36.031155
odometer: 12
altitude: 0
speed: 0
fuel_level: 16
door_status: unknown
security_state_can_status: unknown
security_state_vehicle_mode: unknown
security_state_ignition_mode: unknown
security_state_car_status: unknown
security_state_brake_status: unknown
security_state_engine_status: unknown
security_state_charging_status: unknown
security_state_door_status: unknown
battery_health: 4024
event_data_IoItems_6: 86
event_data_IoItems_17: 40
event_data_IoItems_18: 31
event_data_IoItems_19: None
event_data_IoItems_263: 1
event_data_IoItems_380: 0
event_data_IoItems_DTCs: []
event_data_IoItems_AvlBytes: 120
event_data_IoItems_Ignition: 0
event_data_IoItems_GSM_level: 5
event_data_IoItems_Deep_Sleep: 3
event_data_IoItems_GNSS_Status: 3
event_data_IoItems_Battery_Level: 100
event_data_IoItems_Analog_Input_1: 86
event_data_IoItems_Total_Odometer: 319130
event_data_IoItems_Battery_Voltage: 4024
event_data_IoItems_Movement_Sensor: 0
event_data_IoItems_GSM_Operator_Code: 310410
event_data_IoItems_LVCAN_Program_Number: 12234
event_data_IoItems_Digital_Input_Status_1: 0
event_data_IoItems_Digital_Input_Status_2: 0
event_data_IoItems_Digital_Input_Status_3: 0
event_data_IoItems_Digital_Output_1_state: 0
event_data_IoItems_Digital_Output_2_state: 0
event_data_IoItems_External_Power_Voltage: 12074
event_data_AvlItems_Priority: 0
event_data_AvlItems_Timestamp: 06/04/2024 15:42:44
event_data_GpsItems_Angle: 0
event_data_GpsItems_Speed: 0
event_data_GpsItems_Altitude: 0
event_data_GpsItems_Latitude: 36.031155
event_data_GpsItems_Longitude: -96.10099
event_data_GpsItems_Satellites: 0
transmit_date: 2024-06-04 15:42:53+00:00
door_lock_status: unlock
engine_status: mobilize
green_driving_type: 0
crash_detection: 0
green_driving_value: 0.0
over_speeding: 0.0
ignition_state: unknown
sleep_mode: 3
ev_charge_level: None
ev_charge_state: None
ev_battery_range: None
ev_time_to_full_charge: None
ev_charger_type: None
vehicle_type: 1
ev_last_data_date: None
uuid: a7e49484-331f-4fb0-bcd0-ddfe61476843


'''
