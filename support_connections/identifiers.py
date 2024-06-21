import re
import json
import hashlib

from support_connections.database import Database
from typing import Dict, Any, List

class Vehicle_Device:
    """
    Represents a vehicle device with attributes set from a dictionary.
    """
    def __init__(self, vehicle_device_dict: Dict[str, Any]) -> None:
        """
        Initializes the VehicleDevice instance.

        Args:
            vehicle_device_dict (Dict[str, Any]): Dictionary containing vehicle device attributes.
        """
        self._parse_dict(vehicle_device_dict)

    def _parse_dict(self,vehicle_device_dict) -> None:
        """
        Parses a dictionary and sets the object's attributes accordingly.

        Args:
            vehicle_device_dict (Dict[str, Any]): Dictionary to parse.
        """
        print(json.dumps(vehicle_device_dict,indent=2))
        for key,value in vehicle_device_dict.items():
            setattr(self,key,value)

class Identify:
    """
    Identifies vehicle devices based on input strings.
    """
    def find_preliminary_ID(self,input_str:str, context:str = 'vehicle_device') -> List[Vehicle_Device]:
        """
        Finds preliminary IDs based on the input string and context.

        Args:
            input_str (str): The input string to parse.
            context (str, optional): The context for identifying IDs. Defaults to 'vehicle_device'.

        Returns:
            List[VehicleDevice]: List of identified VehicleDevice objects.
        """
        self.id_dict = {}
        possible_delimiters = ['#', ':', ';', '-', '/', '?', '.', ',', '\n']

        # Replace every delimiter in the input string with empty space
        translation_table = str.maketrans({delimiter: ' ' for delimiter in possible_delimiters})
        input_unlimited = input_str.translate(translation_table)

        # There will be other identifiers I may want to include in the future that will be handled differently
        if context == 'vehicle_device':
            fleet_number_re = re.compile(r'^[A-Z]{1,3}\d{1,4}$')
            vehicle_device_re = re.compile(r'^(?=.*\d)[A-Z\d]{6,}')

            # Splitting by space and looping through is better than doing a re.findall() (without position indicators) because a full ismi contains dozens(?) of ismi and imei sub-strings (among other, similar, reasons)
            source_dict = {item:item.upper() for item in input_unlimited.split(' ')}

            fleet_number_matches = [
                {'source_string': item, 'source_upper': upperitem}
                for item, upperitem in source_dict.items()
                if fleet_number_re.search(upperitem)]
            vehicle_device_matches = [
                {'source_string': item, 'source_upper': upperitem}
                for item, upperitem in source_dict.items()
                if vehicle_device_re.search(upperitem)]

            # To account for stupid fleet number 53 that doesn't match any other fleet number
            if '53' in input_unlimited.split(' '):
                fleet_number_matches.append({'source_string':'53','source_upper':'53'})

            # Initialize the database connection if there's something to look up
            if len(fleet_number_matches) + len(vehicle_device_matches) >0:
                self.db = Database()
                self.connection = self.db.connect('work')
            else:
                # Return with an empty list if no identifiers are found
                return []
            
            self.query_base = """
            SELECT v.vin, v.fleetnumber, v.year, v.model, d.ismi, d.imei, d.iccid 
            FROM vehicles v INNER JOIN devices d ON v.ismi=d.ismi
            WHERE
            """

            self._match_fleet_numbers(fleet_number_matches),
            self._match_vehicle_devices(vehicle_device_matches)


            self.db.close()
        # Converts the (unique) device-vehicle identification dictionaries into Vehicle_Device objects and returns the list of them
        return [Vehicle_Device(item) for item in self.id_dict.values()]

    def _match_fleet_numbers(self, fleet_number_matches):
        """
        Matches potential fleet numbers with database records.

        Args:
            fleet_number_matches (List[Dict[str, str]]): List of potential fleet numbers.
        """
        for potential_fleet_number in fleet_number_matches:
            query = f"{self.query_base} fleetnumber LIKE ?"
            result = self.db.execute_query(query, [potential_fleet_number['source_upper']], self.connection)
            if result:
                hash_key = hashlib.sha1(json.dumps(result[0]).encode()).hexdigest()
                result[0].update(potential_fleet_number)
                self.id_dict[hash_key] = result[0]

    def _match_vehicle_devices(self, vehicle_device_matches):
        """
        Matches potential vehicle device IDs with database records.

        Args:
            vehicle_device_matches (List[Dict[str, str]]): List of potential vehicle device IDs.
        """
        conditionals = ['v.vin', 'd.ismi', 'd.imei']
        for potential_vehicle_device_id in vehicle_device_matches:
            id_length = len(potential_vehicle_device_id['source_upper'])
            for condition in conditionals:
                query = f"{self.query_base} RIGHT({condition}, ?) = ?"
                result = self.db.execute_query(query, [id_length, potential_vehicle_device_id['source_upper']], self.connection)
                if result:
                    hash_key = hashlib.sha1(json.dumps(result[0]).encode()).hexdigest()
                    result[0].update(potential_vehicle_device_id)
                    self.id_dict[hash_key] = result[0]

if __name__ == '__main__':
    id = Identify()
    a = id.find_preliminary_ID('p2443 and also 53 and also l5nwg11482')
#    b = id.find_preliminary_ID('1FTVW1EL5NWG11482')

