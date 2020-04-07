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

# Google spreadsheet dependencies
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

class APIConnection(object):

    #
    # Local definitions to the API connection
    #

    MINIMUM_SIZE = 1
    EMAIL_ADDRESS_DOMAIN = "@intk.com"

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
        self.data = self.init_spreadsheet_data()

    def init_spreadsheet_data():

        spreadsheet = self.client.open_by_url(self.spreadsheet_url)
        worksheet = spreadsheet.worksheet(self.worksheet_name)

        raw_data = worksheet.get_all_values()
        data = self.transform_data(raw_data)
        return data


    def get_all_person(self):
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
    def authenticate_api(): #TODO: needs validation and error handling
        creds = ServiceAccountCredentials.from_json_keyfile_dict(self.json_key, self.scope)
        client = gspread.authorize(creds)
        return client

    # Transformations 
    def transform_data(self, raw_data): #TODO: needs validation and error handling
        data = {}
        if len(raw_data) > MINIMUM_SIZE:
            
            for row in raw_data[MINIMUM_SIZE:]:
                name = row[0]
                fullname = row[14]
                phone = row[16]
                picture = row[17]
                _type = row[7]

                email_address = self.generate_emailaddress(name)
                phone_number_id = self.phonenumber_to_id(phone)

                data[phone_number_id] = {"name": name, "fullname": fullname, "picture": picture, "phone": phone, "type": _type, "email": email_address}

        return data

    def generate_emailaddress(self, name):
        name = self.clean_whitespaces(name)
        emailaddress = "%s%s" %(name, self.EMAIL_ADDRESS_DOMAIN)
        return name

    def phonenumber_to_id(self, phone_number):
        return self.clean_whitespaces(phone_number)

    def clean_whitespaces(self, text, to_lowercase=True):
        try:
            if to_lowercase:
                text = text.lower()

            text = "".join(text.split())
            return text
        except:
            return text



    






    

