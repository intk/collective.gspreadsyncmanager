#!/usr/bin/env python
# -*- coding: utf-8 -*-
from Products.Five import BrowserView
from collective.gspreadsyncmanager.api_connection import APIConnection
from collective.gspreadsyncmanager.sync_manager import SyncManager
from collective.gspreadsyncmanager.mapping_core import CORE as SYNC_CORE
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



# TESTS API


def test_get_organizations_future():
    with plone.api.env.adopt_user(username="admin"):
        # Get API settings from the controlpanel
        api_settings = get_api_settings()

        # Create the API connection
        api_connection = APIConnection(api_settings)

        # Create the settings for the sync
        # Initiate the sync manager
        sync_options = {"api": api_connection, 'core': SYNC_CORE}
        sync_manager = SyncManager(sync_options)

        all_upcoming_organizations = sync_manager.get_all_upcoming_organizations()

        print "Total upcoming organizations in the website: %s" %(len(all_upcoming_organizations))

        return None

def test_sync_organization_list():
    with plone.api.env.adopt_user(username="admin"):
        # Get API settings from the controlpanel
        api_settings = get_api_settings()

        # Create the API connection
        api_connection = APIConnection(api_settings)

        # Create the settings for the sync
        # Initiate the sync manager
        sync_options = {"api": api_connection, 'core': SYNC_CORE}
        sync_manager = SyncManager(sync_options)

        dateFrom = get_datetime_today(as_string=True)
        dateUntil = get_datetime_future(as_string=True)
        
        logger("[Status] Start sync organization list test.")
        organization_list = sync_manager.update_organization_list_by_date(date_from=dateFrom, date_until=dateUntil, create_and_unpublish=True)
        logger("[Status] Finished sync organization list test.")
        return organization_list


def test_sync_availability():
    with plone.api.env.adopt_user(username="admin"):
        # Get API settings from the controlpanel
        api_settings = get_api_settings()

        # Create the API connection
        api_connection = APIConnection(api_settings)

        # Create the settings for the sync
        # Initiate the sync manager
        sync_options = {"api": api_connection, 'core': SYNC_CORE}
        sync_manager = SyncManager(sync_options)

        dateFrom = get_datetime_today(as_string=True)
        dateUntil = get_datetime_future(as_string=True)

        logger("[Status] Start availability sync test.")
        synced_availability_list = sync_manager.update_availability_by_date(date_from=dateFrom, date_until=dateUntil, create_new=True)
        logger("[Status] Finished availability sync test.")
        print "Total organizations to update availability: %s" %(len(organization_list))
        return organization_list

def test_api_arrangement_list_call():
    with plone.api.env.adopt_user(username="admin"):
        # Get API settings from the controlpanel
        api_settings = get_api_settings()

        # Create the API connection
        api_connection = APIConnection(api_settings)

        dateFrom = get_datetime_today(as_string=True)
        dateUntil = get_datetime_future(as_string=True)

        logger("[Status] Start api call test to get arrangement list.")
        arrangement_list_response = api_connection.get_arrangement_list_by_date(date_from=dateFrom, date_until=dateUntil)
        logger("[Status] Finished api call test to get arrangement list.")
        

        # find arrangements for organization 2869
        find_organization_id = 2869
        arrangement_list = []

        for product in arrangement_list_response:
            product_arrangement_list = product.get('arrangements', [])
            for arrangement in product_arrangement_list:
                organization = arrangement.get('organization', None)
                if organization:
                    organization_id = organization.get('id', '')
                    if organization_id == find_organization_id:
                        new_arrangement = arrangement
                        new_arrangement['product_id'] = product.get('id', '')
                        arrangement_list.append(new_arrangement)
                        break

        print arrangement_list
        print "Total arrangments for organization ID '%s': %s" %(find_organization_id, len(arrangement_list))

        return arrangement_list



#
# Organization hourly sync
#
class SyncOrganizationsAvailability(BrowserView):

    def __call__(self):
        return self.sync()

    def sync(self):
        redirect_url = self.context.absolute_url()
        messages = IStatusMessage(self.request)

        # Get API settings from the controlpanel
        api_settings = get_api_settings()

        # Create the API connection
        api_connection = APIConnection(api_settings)

        # Create the settings for the sync
        # Initiate the sync manager
        sync_options = {"api": api_connection, 'core': SYNC_CORE}
        sync_manager = SyncManager(sync_options)

        dateFrom = get_datetime_today(as_string=True)
        dateUntil = get_datetime_future(as_string=True)

        try:
            logger("[Status] Start availability sync.")
            synced_availability_list = sync_manager.update_availability_by_date(date_from=dateFrom, date_until=dateUntil)
            logger("[Status] Finished availability sync.")
            messages.add(u"Organizations availability is now synced.", type=u"info")
        except Exception as err:
            logger("[Error] Error while requesting the sync for the organizations availability.", err)
            messages.add(u"Organizations availability failed to sync with the api. Please contact the website administrator.", type=u"error")

        raise Redirect(redirect_url)


#
# Organization List sync
#
class SyncOrganizationsList(BrowserView):

    def __call__(self):
        return self.sync()

    def sync(self):
        redirect_url = self.context.absolute_url()
        messages = IStatusMessage(self.request)

        # Get API settings from the controlpanel
        api_settings = get_api_settings()

        # Create the API connection
        api_connection = APIConnection(api_settings)

        # Create the settings for the sync
        # Initiate the sync manager
        sync_options = {"api": api_connection, 'core': SYNC_CORE}
        sync_manager = SyncManager(sync_options)

        dateFrom = get_datetime_today(as_string=True)
        dateUntil = get_datetime_future(as_string=True)

        try:
            logger("[Status] Start syncing organization list.")
            organization_list = sync_manager.update_organization_list_by_date(date_from=dateFrom, date_until=dateUntil, create_and_unpublish=True)
            logger("[Status] Syncing organization list finished.")
            messages.add(u"Organization list is now synced.", type=u"info")
        except Exception as err:
            logger("[Error] Error while requesting the sync for the organization list.", err)
            messages.add(u"Organization list failed to sync with the api. Please contact the website administrator.", type=u"error")

        raise Redirect(redirect_url)

#
# Organization Availability
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


