#!/usr/bin/python
# -*- coding: utf-8 -*-


#
# GoogleSheets API sync mechanism by Andre Goncalves
#

# Global dependencies
import re
import requests
import sys
from datetime import datetime
from collective.gspreadsyncmanager.utils import DATE_FORMAT

try:
    from urllib.parse import urlencode
except ImportError:
    # support python 2
    from urllib import urlencode

# Product dependencies
from collective.gspreadsyncmanager.error_handling.error import raise_error
from collective.gspreadsyncmanager.utils import clean_whitespaces, phonenumber_to_id, generate_person_id, generate_safe_id

# Google spreadsheet dependencies
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from httplib2 import Http


class APIConnection(object):

    #
    # Local definitions to the API connection
    #

    MINIMUM_SIZE = 1
    EMAIL_ADDRESS_DOMAIN = "@intk.com"

    # API mapping field / column
    API_MAPPING = {
        "name": 0,
        "fullname": 19,
        "phone": 25,
        "picture": 26,
        "type": 11,
        "market": 13,
        "start_date": 1,
        "colleague": 0,
        "mentor": 14,
        "team": 15
    }

    #
    # Initialisation methods
    #
    def __init__(self, api_settings):
        
        self.api_settings = api_settings
        self.worksheet_name = api_settings['worksheet_name']
        self.spreadsheet_url = api_settings['spreadsheet_url']
        self.json_key = json.loads(api_settings['json_key'])
        self.scope = api_settings['scope']

        self.client = self.authenticate_api()
        #self.drive = self.authenticate_drive_api()

        self.data = self.init_spreadsheet_data()
        #self.drive_data = self.get_drive_data()

    def init_spreadsheet_data(self):

        spreadsheet = self.client.open_by_url(self.spreadsheet_url)
        worksheet = spreadsheet.worksheet(self.worksheet_name)

        raw_data = worksheet.get_all_values()
        data = self.transform_data(raw_data)
        return data


    def get_all_persons(self):
        #
        # Request the person list from the GoogleSheets API
        #
        return self.data

    def get_person_by_id(self, person_id):
        # 
        # Gets an person by ID 
        # 

        if person_id in self.data.keys():
            return self.data[person_id]
        else:
            raise_error('responseHandlingError', 'Person is not found in the Spreadsheet. ID: %s' %(person_id))

    # Authentication
    """def authenticate_drive_api(self): #TODO: needs validation and error handling
        creds = ServiceAccountCredentials.from_json_keyfile_dict(self.json_key, self.scope)
        http = creds.authorize(Http())
        drive = discovery.build('drive', 'v3', http=http)
        return drive"""

    def authenticate_api(self): #TODO: needs validation and error handling
        creds = ServiceAccountCredentials.from_json_keyfile_dict(self.json_key, self.scope)
        client = gspread.authorize(creds)
        return client

    """def get_drive_data(self):
        data = drive.files().get(fileId="1yNy_9s_nJfnPh8hyb5c3rVApdLhE8k4sGqLPNvKmkQk", fields="name,modifiedTime")
        return data"""

    # Transformations 
    def transform_data(self, raw_data): #TODO: needs validation and error handling
        data = {}
        if len(raw_data) > self.MINIMUM_SIZE:
            
            for row in raw_data[self.MINIMUM_SIZE:]:

                new_person = {}
                for fieldname, sheet_position in self.API_MAPPING.items():
                    new_person[fieldname] = row[sheet_position]

                email_address = self.generate_emailaddress(new_person["name"])
                person_id = generate_person_id(new_person["fullname"])

                new_person['email'] = email_address
                new_person['_id'] = person_id

                data[person_id] = new_person

        return data

    def generate_emailaddress(self, name):
        name = generate_safe_id(name)
        name = clean_whitespaces(name)
        emailaddress = "%s%s" %(name, self.EMAIL_ADDRESS_DOMAIN)
        return emailaddress






    






    

