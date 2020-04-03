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


    def get_all_organization(self):
        #
        # Request the organization list from the GoogleSheets API
        #
        return self.data

    def get_organization_by_id(self, organization_id):
        # 
        # Gets an organization by ID 
        # 

        if organization_id in self.data:
            return self.data[organization_id]
        else:
            raise_error('responseHandlingError', 'Organization is not found in the Spreadsheet. ID: %s' %(organization_id))

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
                google_ads_id = row[1]
                name = row[0]
                selfie = row[4]
                _type = row[7]

                data[google_ads_id] = {"name": name, "selfie": selfie, "type": _type}

        return data

    






    

