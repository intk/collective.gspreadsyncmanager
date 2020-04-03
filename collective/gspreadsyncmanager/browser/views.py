#!/usr/bin/env python
# -*- coding: utf-8 -*-
from Products.Five import BrowserView
from collective.gspreadsyncmanager.api_modules.gsheets.gsheets_api_connection import APIConnection

from collective.gspreadsyncmanager.sync_manager import SyncManager
from collective.gspreadsyncmanager.mapping_cores.gsheets.mapping_core import CORE as SYNC_CORE

# Plone imports
from Products.statusmessages.interfaces import IStatusMessage
from zExceptions import Redirect
from plone.registry.interfaces import IRegistry
from zope.component import getUtility

#
# Product dependencies
#
from collective.gspreadsyncmanager.utils import get_api_settings, get_datetime_today, get_datetime_future
from collective.gspreadsyncmanager.error import raise_error
from collective.gspreadsyncmanager.logging import logger
import plone.apixxxw

# Google Spreadsheets connection
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# TESTS API

def test_get_organization_by_id():
    with plone.api.env.adopt_user(username="admin"):
        # Get API settings from the controlpanel
        api_settings = get_api_settings()
        
        # Create the API connection
        api_connection = APIConnection(api_settings)

        logger("[Status] Start sync organization by id.")

        organization_id = "5686447075"

        organization = api_connection.get_organization_by_id(organization_id=)
        print(organization)

        logger("[Status] Finished sync organization by id.")
        return organization_list

#
# Sync Organization
#
class SyncOrganization(BrowserView):

    def __call__(self):
        return self.sync()

    def sync(self):

        # Get the necessary information to call the api and return a response
        context_organization_id = getattr(self.context, 'organization_id', None)
        redirect_url = self.context.absolute_url()
        messages = IStatusMessage(self.request)

        if context_organization_id:
            try:
                # Get API settings from the controlpanel
                api_settings = get_api_settings()

                # Create the API connection
                api_connection = APIConnection(api_settings)

                # Create the settings for the sync
                # Initiate the sync manager
                sync_options = {"api": api_connection, 'core': SYNC_CORE}
                sync_manager = SyncManager(sync_options)
                
                # Trigger the sync to update one organization
                logger("[Status] Start update of single organization.")
                organization_data = sync_manager.update_organization_by_id(organization_id=context_organization_id)
                logger("[Status] Finished update of single organization.")
                messages.add(u"Organization ID %s is now synced." %(context_organization_id), type=u"info")
            except Exception as err:
                logger("[Error] Error while requesting the sync for the organization ID: %s" %(context_organization_id), err)
                messages.add(u"Organization ID '%s' failed to sync with the api. Please contact the website administrator." %(context_organization_id), type=u"error")
        else:
            messages.add(u"This organization cannot be synced with the API. Organization ID is missing.", type=u"error")
            logger("[Error] Error while requesting the sync for the organization. Organization ID is not available.", "Organization ID not found.")
        
        # Redirect to the original page
        raise Redirect(redirect_url)


