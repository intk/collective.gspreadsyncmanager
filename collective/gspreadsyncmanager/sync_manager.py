#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# GoogleSheets API sync mechanism by Andre Goncalves
#
import plone.api
import transaction

# Plone dependencies
from zope.schema.interfaces import ITextLine, ITuple, IBool
from plone.app.textfield.interfaces import IRichText
from plone.app.textfield.value import RichTextValue
from zope.schema import getFieldsInOrder
from plone.event.interfaces import IEventAccessor
from datetime import datetime
from zope.component import getUtility
from plone.i18n.normalizer.interfaces import IIDNormalizer

# Product dependencies
from collective.organization.interfaces import IOrganization

# Error handling
from .error_handling.error import raise_error

# Logging module
from .logging.logging import logger

# Utils
from .utils import str2bool, normalize_id
from .utils import get_datetime_today, get_datetime_future, DATE_FORMAT

class SyncManager(object):
    #
    # Init methods 
    #  
    DEFAULT_CONTENT_TYPE = "Organization" # TODO: should come from settings
    DEFAULT_FOLDER = "/organizations" # TODO: should come from settings
    

    def __init__(self, options):
        self.options = options
        self.gsheets_api = self.options['api']
        self.CORE = self.options['core']
        self.fields_schema = getFieldsInOrder(IOrganization)

    #
    # Sync operations 
    #
    def update_organization_by_id(self, organization_id, organization_data=None):
        organization = self.find_organization(organization_id)

        if not organization_data:
            organization_data = self.gsheets_api.get_organization_by_id(organization_id)

        updated_organization = self.update_organization(organization_id, organization, organization_data)
        return updated_organization

    def update_organization_list_by_date(self, date_from, date_until, create_and_unpublish=False):
        organization_list = self.gsheets_api.get_organization_list_by_date(date_from=date_from, date_until=date_until)
        
        if create_and_unpublish:
            website_organizations = self.get_all_organizations(date_from=date_from)
            self.sync_organization_list(organization_list, website_organizations)
        else:
            self.update_organization_list(organization_list)
        
        return organization_list

    #
    # CRUD operations
    #

    # UPDATE
    def update_organization(self, organization_id, organization, organization_data):
        updated_organization = self.update_all_fields(organization, organization_data, arrangement_list)
        logger("[Status] Organization with ID '%s' is now updated. URL: %s" %(organization_id, organization.absolute_url()))
        return updated_organization

    def update_organization_list(self, organization_list):
        for organization in organization_list:
            organization_id = organization.get('id', '')
            try:
                organization_data = self.update_organization_by_id(organization_id)
            except Exception as err:
                logger("[Error] Error while requesting the sync for the organization ID: %s" %(organization_id), err)
        
        return organization_list

    # CREATE
    def create_organization(self, organization_id):
        organization_data = self.gsheets_api.get_organization_availability(organization_id)
        
        try:
            title = organization_data['title']
            description = organization_data.get('subtitle', '')

            new_organization_id = normalize_id(title)
            container = self.get_container()
            new_organization = plone.api.content.create(container=container, type=self.DEFAULT_CONTENT_TYPE, id=new_organization_id, safe_id=True, title=title, description=description)
            logger("[Status] Organization with ID '%s' is now created. URL: %s" %(organization_id, new_organization.absolute_url()))
            updated_organization = self.update_organization(organization_id, new_organization, organization_data)
        except Exception as err:
            logger("[Error] Error while creating the organization ID '%s'" %(organization_id), err)
            return None
    
    def create_new_organizations(self, organizations_data, website_data):
        new_organizations = [api_id for api_id in organizations_data.keys() if api_id not in website_data.keys()]
        created_organizations = [self.create_organization(organization_id) for organization_id in new_organizations]
        return new_organizations

    # CREATE OR UPDATE
    def sync_organization_list(self, organization_list, website_organizations):

        website_data = self.build_website_data_dict(website_organizations)

        for organization in organization_list:
            organization_id = str(organization.get('id', ''))
            if organization_id in website_data.keys():
                consume_organization = website_data.pop(organization_id)
                try:
                    organization_data = self.update_organization_by_id(organization_id)
                except Exception as err:
                    logger("[Error] Error while updating the organization ID: %s" %(organization_id), err)
            else:
                try:
                    new_organization = self.create_organization(organization_id)
                except Exception as err:
                    logger("[Error] Error while creating the organization ID: %s" %(organization_id), err)
        
        if len(website_data.keys()) > 0:
            unpublished_organizations = [self.unpublish_organization(organization_brain.getObject()) for organization_brain in website_data.values()]

        return organization_list

    # GET
    def get_all_organizations(self):
        results = plone.api.content.find(portal_type=self.DEFAULT_CONTENT_TYPE)
        return results

    

     # FIND
    def find_organization(self, organization_id):
        organization_id = self.safe_value(organization_id)
        result = plone.api.content.find(organization_id=organization_id)
        if result:
            return result[0].getObject()
        else:
            raise_error("organizationNotFoundError", "Organization with ID '%s' is not found in Plone" %(organization_id))

    # DELETE
    def delete_organization_by_id(self, organization_id):
        obj = self.find_organization(organization_id=organization_id)
        self.delete_organization(obj)  

    def delete_organization(self, organization):
        plone.api.content.delete(obj=organization)

    # PLONE WORKLFLOW - publish
    def publish_organization(self, organization):
        plone.api.content.transition(obj=organization, to_state="published")

    # PLONE WORKLFLOW - unpublish
    def unpublish_organization(self, organization):
        plone.api.content.transition(obj=organization, to_state="private")
        logger("[Status] Unpublished organization with ID: '%s'" %(getattr(organization, 'organization_id', '')))
        return organization

    def unpublish_organization_by_id(self, organization_id):
        obj = self.find_organization(organization_id=organization_id)
        self.unpublish_organization(obj)


    #
    # CRUD utils
    # 
    def get_organization_data_from_list_by_id(self, organization_brain, organizations_data):
        organization_id = getattr(organization_brain, 'organization_id', None)
        if organization_id and organization_id in organizations_data:
            return organizations_data[organization_id]
        else:
            logger("[Error] Organization data for '%s' cannot be found." %(organization_brain.getURL()), "requestHandlingError")
            return None

    def build_organizations_data_dict(self, api_organizations):
        organizations_data = {}
        for api_organization in api_organizations:
            if 'id' in api_organization:
                organizations_data[self.safe_value(api_organization['id'])] = api_organization
            else:
                logger('[Error] Organization ID cannot be found in the API JSON: %s' %(api_organization), 'requestHandlingError')
        return organizations_data

    def build_website_data_dict(self, website_organizations):
        website_organizations_data = {}
        for website_organization in website_organizations:
            organization_id = getattr(website_organization, 'organization_id', None)
            if organization_id:
                website_organizations_data[self.safe_value(organization_id)] = website_organization
            else:
                logger('[Error] Organization ID value cannot be found in the brain url: %s' %(website_organization.getURL()), 'requestHandlingError')
        return website_organizations_data

    def get_container(self):
        container = plone.api.content.get(path=self.DEFAULT_FOLDER)
        return container

    # FIELDS
    def match(self, field):
        # Find match in the core
        if field in self.CORE.keys():
            if self.CORE[field]:
                return self.CORE[field]
            else:
                logger("[Warning] API field '%s' is ignored in the fields mapping" %(field), "Field ignored in mapping.")
                return False
        else:
            # log field not match
            logger("[Error] API field '%s' does not exist in the fields mapping" %(field), "Field not found in mapping.")
            return False

    def update_field(self, organization, fieldname, fieldvalue):
        plonefield_match = self.match(fieldname)

        if plonefield_match:
            try:
                if not hasattr(organization, plonefield_match):
                    logger("[Error] Plone field '%s' does not exist" %(plonefield_match), "Plone field not found")
                    return None
                transform_value = self.transform_special_fields(organization, fieldname, fieldvalue)
                if transform_value:
                    return transform_value
                else:
                    setattr(organization, plonefield_match, self.safe_value(fieldvalue))
                    return fieldvalue
            except Exception as err:
                logger("[Error] Exception while syncing the API field '%s'" %(fieldname), err)
                return None
        else:
            return None

    def update_all_fields(self, organization, organization_data):
        self.clean_all_fields(organization)
        updated_fields = [(self.update_field(organization, field, organization_data[field]), field) for field in organization_data.keys()]
        organization = self.validate_organization_data(organization, organization_data)
        return organization

    

    #
    # Sanitising/validation methods
    #
    def safe_value(self, fieldvalue):
        if isinstance(fieldvalue, bool):
            return fieldvalue
        elif isinstance(fieldvalue, int):
            fieldvalue_safe = "%s" %(fieldvalue)
            return fieldvalue_safe
        else:
            return fieldvalue

    def clean_all_fields(self, organization):

        # get all fields from schema
        for fieldname, field in self.fields_schema:
            if fieldname not in ['organization_id']: # TODO: required field needs to come from the settings
                self.clean_field(organization, fieldname, field)
            
        # extra fields that are not in the behavior
        # location
        setattr(organization, 'location', "")

        return organization

    def clean_field(self, organization, fieldname, field):
        if ITextLine.providedBy(field):
            setattr(organization, fieldname, "")
        elif ITuple.providedBy(field):
            setattr(organization, fieldname, [])
        elif IBool.providedBy(field):
            setattr(organization, fieldname, False)
        elif IRichText.providedBy(field):
            richvalue = RichTextValue("", 'text/html', 'text/html')
            setattr(organization, fieldname, richvalue)
        else:
            logger("[Error] Field '%s' type is not recognised. " %(fieldname), "Field cannot be cleaned before sync.")

        return organization

    def validate_organization_data(self, organization, organization_data):
        validated = True # Needs validation
        if validated:
            organization.reindexObject()
            transaction.get().commit()
            return organization
        else:
            raise_error("validationError", "Organization is not valid. Do not commit changes to the database.")

    #
    # Transform special fields
    # Special methods
    #
    def transform_special_fields(self, organization, fieldname, fieldvalue):
        special_field_handler = self.get_special_fields_handlers(fieldname)
        if special_field_handler:
            if fieldvalue:
                special_field_value = special_field_handler(organization, fieldname, fieldvalue)
                return special_field_value
            else:
                if fieldname in ['ranks']:
                    return RichTextValue("", 'text/html', 'text/html')
                return fieldvalue
        return False

    def get_special_fields_handlers(self, fieldname):
        SPECIAL_FIELDS_HANDLERS = {
            "title": self._transform_organization_title,
            "type": self._transform_organization_type
        }

        if fieldname in SPECIAL_FIELDS_HANDLERS:
            return SPECIAL_FIELDS_HANDLERS[fieldname]
        else:
            return None

    def _transform_organization_title(self, organization, fieldname, fieldvalue):
        setattr(organization, 'organization_title', fieldvalue)
        return fieldvalue

    def _transform_organization_type(self, organization, fieldname, fieldvalue):
        current_subjects = organization.Subject()
        if 'frontpage-slideshow' in current_subjects:
            subjects = ['frontpage-slideshow', fieldvalue]
            organization.setSubject(subjects)
        else:
            organization.setSubject([fieldvalue])
            
        return [fieldvalue]
