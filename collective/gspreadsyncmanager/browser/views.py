#!/usr/bin/env python
# -*- coding: utf-8 -*-
from Products.Five import BrowserView
from collective.gspreadsyncmanager.api_modules.gsheets.persons.gsheets_api_connection import APIConnection as APIConnectionPersons
from collective.gspreadsyncmanager.api_modules.gsheets.organizations.gsheets_api_connection import APIConnection as APIConnectionOrganizations

from collective.gspreadsyncmanager.sync_manager_persons import SyncManager as SyncManagerPersons
from collective.gspreadsyncmanager.sync_manager_organizations import SyncManager as SyncManagerOrganizations

from collective.gspreadsyncmanager.mapping_cores.gsheets.mapping_core import CORE as SYNC_CORE
from collective.gspreadsyncmanager.mapping_cores.gsheets.mapping_core import CORE_ORGANIZATIONS as SYNC_CORE_ORGANIZATIONS


# Plone imports
from Products.statusmessages.interfaces import IStatusMessage
from zExceptions import Redirect
from plone.registry.interfaces import IRegistry
from zope.component import getUtility

#
# Product dependencies
#
from collective.gspreadsyncmanager.utils import get_api_settings, get_api_settings_persons, get_datetime_today, get_datetime_future, clean_whitespaces, phonenumber_to_id
from collective.gspreadsyncmanager.error_handling.error import raise_error
from collective.gspreadsyncmanager.logging.logging import logger
import plone.api
from collective.taskqueue.interfaces import ITaskQueue
from collective.taskqueue import taskqueue


# Google Spreadsheets connection
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import requests
from requests.auth import HTTPBasicAuth
from plone.registry import Registry
import transaction


# TESTS API

# PERSONS
def test_get_person_by_id():
    with plone.api.env.adopt_user(username="admin"):
        # Get API settings from the controlpanel
        api_settings = get_api_settings_persons()
        
        # Create the API connection
        api_connection = APIConnectionPersons(api_settings)

        logger("[Status] Start sync person by id.")

        person_id_raw = "+316 4275 6041"

        person_id = phonenumber_to_id(person_id_raw)

        person = api_connection.get_person_by_id(person_id=person_id)
        print(person)

        logger("[Status] Finished sync person by id.")
        return person


# ORGANIZATIONS
def test_get_organization_by_id():
    with plone.api.env.adopt_user(username="admin"):
        # Get API settings from the controlpanel
        api_settings = get_api_settings()
        
        # Create the API connection
        api_connection = APIConnectionOrganizatios(api_settings)

        logger("[Status] Start sync organization by id.")

        organization_id = "5686447075"

        organization = api_connection.get_organization_by_id(organization_id=organization_id)
        print(organization)

        logger("[Status] Finished sync organization by id.")
        return organization

#
# Sync Person
#

class QueueSyncPerson(BrowserView):

    def __call__(self):
        return self.queue_sync()

    def queue_sync(self):
        redirect_url = self.context.absolute_url()

        QUEUE_LIMIT = 1
        QUEUE_VIEW = "sync_person"

        queue_view_path = self.context.getPhysicalPath()
        queue_size = len(getUtility(ITaskQueue, name='sync'))

        queue_view_path_url = "/".join(queue_view_path)
        queue_view_url = "%s/%s" %(queue_view_path_url, QUEUE_VIEW)
        
        print("URL: %s" %(queue_view_url))
        print("Queue size: %s" %(queue_size))

        messages = IStatusMessage(self.request)

        if queue_size < QUEUE_LIMIT:
            sync_id = taskqueue.add(url=queue_view_url, queue="sync")
            print("Run sync with ID: '%s'" %(sync_id))
            messages.add(u"Sync ID '%s' is now triggered." %(sync_id), type=u"info")
        else:
            messages.add(u"There is one sync currently running. Try again later.", type=u"warning")

        raise Redirect(redirect_url)

class QueueSyncAllPersons(BrowserView):

    def __call__(self):
        return self.queue_sync()

    def queue_sync(self):
        redirect_url = self.context.absolute_url()

        QUEUE_LIMIT = 1
        QUEUE_VIEW = "sync_all_persons"

        queue_view_path = self.context.getPhysicalPath()
        queue_size = len(getUtility(ITaskQueue, name='sync'))

        queue_view_path_url = "/".join(queue_view_path)
        queue_view_url = "%s/%s" %(queue_view_path_url, QUEUE_VIEW)
        
        print("URL: %s" %(queue_view_url))
        print("Queue size: %s" %(queue_size))

        messages = IStatusMessage(self.request)

        if queue_size < QUEUE_LIMIT:
            sync_id = taskqueue.add(url=queue_view_url, queue="sync")
            print("Run sync with ID: '%s'" %(sync_id))
            messages.add(u"Sync ID '%s' is now triggered." %(sync_id), type=u"info")
        else:
            messages.add(u"There is one sync currently running. Try again later.", type=u"warning")

        raise Redirect(redirect_url)



class SyncPerson(BrowserView):

    def __call__(self):
        return self.sync()

    def sync(self):

        # Get the necessary information to call the api and return a response
        context_person_id_raw = getattr(self.context, 'phone', '')
        context_person_title = getattr(self.context, 'email', '')
        context_person_id = phonenumber_to_id(context_person_id_raw, context_person_title)

        redirect_url = self.context.absolute_url()
        messages = IStatusMessage(self.request)

        if context_person_id:
            try:
                # Get API settings from the controlpanel
                api_settings = get_api_settings_persons()

                # Create the API connection
                api_connection = APIConnectionPersons(api_settings)

                # Create the settings for the sync
                # Initiate the sync manager
                sync_options = {"api": api_connection, 'core': SYNC_CORE}
                sync_manager = SyncManagerPersons(sync_options)
                
                # Trigger the sync to update one organization
                logger("[Status] Start update of single person.")
                person_data = sync_manager.update_person_by_id(person_id=context_person_id)
                logger("[Status] Finished update of single person.")
                messages.add(u"Person ID '%s' is now synced." %(context_person_id), type=u"info")
            except Exception as err:
                logger("[Error] Error while requesting the sync for the person ID: '%s'" %(context_person_id), err)
                messages.add(u"Person ID '%s' failed to sync with the api. Please contact the website administrator." %(context_person_id), type=u"error")
        else:
            messages.add(u"This person cannot be synced with the API. Person ID is missing.", type=u"error")
            logger("[Error] Error while requesting the sync for the person. Person ID is not available.", "Person ID not found.")
        

        # Redirect to the original page
        raise Redirect(redirect_url)


class SyncAllPersonsAJAX(BrowserView):

    def __call__(self):
        return self.sync()

    def sync(self):

        messages = IStatusMessage(self.request)
        
        try:
            # Get API settings from the controlpanel
            api_settings = get_api_settings_persons()
            plone.api.portal.set_registry_record("sync_complete", False)

            # Create the API connection
            api_connection = APIConnectionPersons(api_settings)

            # Create the settings for the sync
            # Initiate the sync manager
            sync_options = {"api": api_connection, 'core': SYNC_CORE}
            sync_manager = SyncManagerPersons(sync_options)
            
            # Trigger the sync to update one organization
            logger("[Status] Start update of all persons.")
            person_data = sync_manager.update_persons(create_and_unpublish=True)
            logger("[Status] Finished update of all persons.")
        except Exception as err:
            logger("[Error] Error while requesting the sync for all persons.", err)
        
        return True

class SyncAllPersons(BrowserView):

    def __call__(self):
        return self.sync()

    def sync(self):

        redirect_url = self.context.absolute_url()
        messages = IStatusMessage(self.request)
        
        try:
            # Get API settings from the controlpanel
            api_settings = get_api_settings_persons()

            # Create the API connection
            api_connection = APIConnectionPersons(api_settings)

            # Create the settings for the sync
            # Initiate the sync manager
            sync_options = {"api": api_connection, 'core': SYNC_CORE}
            sync_manager = SyncManagerPersons(sync_options)
            
            # Trigger the sync to update one organization
            logger("[Status] Start update of all persons.")
            person_data = sync_manager.update_persons(create_and_unpublish=True)
            logger("[Status] Finished update of all persons.")
            messages.add(u"Persons are now synced.", type=u"info")
        except Exception as err:
            logger("[Error] Error while requesting the sync for all persons.", err)
            messages.add(u"Sync of persons with the api failed. Please contact the website administrator.", type=u"error")
        
        # Redirect to the original page
        raise Redirect(redirect_url)


class RequestSyncAllPersons(BrowserView):

    def __call__(self):
        return self.sync()

    def sync(self):
        view_name = "@@sync_all_persons_ajax"
        current_url = self.context.absolute_url()
        if current_url:
            if current_url[len(current_url)-1] != "/":
                current_url = current_url + "/"

            request_url = "%s%s" %(current_url, view_name)
            
            res = requests.get(request_url, auth=HTTPBasicAuth('##', '##'))
            res.connection.close()
            return True
        else:
            return False


# # # # # # # # # # # #
# Sync Organization # #
# # # # # # # # # # # #
class SyncOrganization(BrowserView):

    def __call__(self):
        return self.sync()

    def sync(self):

        # Get the necessary information to call the api and return a response
        context_organization_id = getattr(self.context, 'google_ads_id', '')

        redirect_url = self.context.absolute_url()
        messages = IStatusMessage(self.request)

        if context_organization_id:
            try:
                # Get API settings from the controlpanel
                api_settings = get_api_settings()

                # Create the API connection
                api_connection = APIConnectionOrganizations(api_settings)

                # Create the settings for the sync
                # Initiate the sync manager
                sync_options = {"api": api_connection, 'core': SYNC_CORE_ORGANIZATIONS}
                sync_manager = SyncManagerOrganizations(sync_options)
                
                # Trigger the sync to update one organization
                logger("[Status] Start update of single organization.")
                person_data = sync_manager.update_organization_by_id(organization_id=context_organization_id)
                logger("[Status] Finished update of single organization.")
                messages.add(u"Organization ID '%s' is now synced." %(context_organization_id), type=u"info")
            except Exception as err:
                logger("[Error] Error while requesting the sync for the organization ID: '%s'" %(context_organization_id), err)
                messages.add(u"Organization ID '%s' failed to sync with the api. Please contact the website administrator." %(context_organization_id), type=u"error")
        else:
            messages.add(u"This organization cannot be synced with the API. Organization ID is missing.", type=u"error")
            logger("[Error] Error while requesting the sync for the organization. Organization ID is not available.", "Organization ID not found.")
        

        # Redirect to the original page
        raise Redirect(redirect_url)

class SyncAllOrganizations(BrowserView):

    def __call__(self):
        return self.sync()

    def sync(self):

        redirect_url = self.context.absolute_url()
        messages = IStatusMessage(self.request)

        try:
            # Get API settings from the controlpanel
            api_settings = get_api_settings()

            # Create the API connection
            api_connection = APIConnectionOrganizations(api_settings)

            # Create the settings for the sync
            # Initiate the sync manager
            sync_options = {"api": api_connection, 'core': SYNC_CORE_ORGANIZATIONS}
            sync_manager = SyncManagerOrganizations(sync_options)
            
            # Trigger the sync to update one organization
            logger("[Status] Start update of all organization.")
            person_data = sync_manager.update_organizations(create_and_unpublish=True)
            logger("[Status] Finished update of all organization.")
        except Exception as err:
            logger("[Error] Error while requesting the sync for all organizations", err)
            messages.add(u"Sync of all organizations ID failed. Please contact the website administrator.", type=u"error")

        # Redirect to the original page
        raise Redirect(redirect_url)



