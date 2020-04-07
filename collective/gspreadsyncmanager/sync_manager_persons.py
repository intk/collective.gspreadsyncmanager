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
from collective.person.interfaces import IPerson

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
    DEFAULT_CONTENT_TYPE = "Person" # TODO: should come from settings
    DEFAULT_FOLDER = "/en/about/team" # TODO: should come from settings
    

    def __init__(self, options):
        self.options = options
        self.gsheets_api = self.options['api']
        self.CORE = self.options['core']

    #
    # Sync operations 
    #
    def update_person_by_id(self, person_id, person_data=None):
        person = self.find_person(person_id)

        if not person_data:
            person_data = self.gsheets_api.get_person_by_id(person_id)

        updated_person = self.update_person(person_id, person, person_data)
        return updated_person


    def update_person_list_by_date(self, date_from, date_until, create_and_unpublish=False):
        person_list = self.gsheets_api.get_person_list_by_date(date_from=date_from, date_until=date_until)
        
        if create_and_unpublish:
            website_persons = self.get_all_persons(date_from=date_from)
            self.sync_person_list(person_list, website_persons)
        else:
            self.update_person_list(person_list)
        
        return person_list

    #
    # CRUD operations
    #

    # UPDATE
    def update_person(self, person_id, person, person_data):
        updated_person = self.update_all_fields(person, person_data)
        logger("[Status] Person with ID '%s' is now updated. URL: %s" %(person_id, person.absolute_url()))
        return updated_person

    def update_person_list(self, person_list):
        for person in person_list:
            person_id = person.get('id', '')
            try:
                person_data = self.update_person_by_id(person_id)
            except Exception as err:
                logger("[Error] Error while requesting the sync for the person ID: %s" %(person_id), err)
        
        return person_list

    # CREATE
    def create_person(self, person_id):
        person_data = self.gsheets_api.get_person_availability(person_id)
        
        try:
            title = person_data['title']
            description = person_data.get('subtitle', '')

            new_person_id = normalize_id(title)
            container = self.get_container()
            new_person = plone.api.content.create(container=container, type=self.DEFAULT_CONTENT_TYPE, id=new_person_id, safe_id=True, title=title, description=description)
            logger("[Status] Person with ID '%s' is now created. URL: %s" %(person_id, new_person.absolute_url()))
            updated_person = self.update_person(person_id, new_person, person_data)
        except Exception as err:
            logger("[Error] Error while creating the person ID '%s'" %(person_id), err)
            return None
    
    def create_new_persons(self, persons_data, website_data):
        new_persons = [api_id for api_id in persons_data.keys() if api_id not in website_data.keys()]
        created_persons = [self.create_person(person_id) for person_id in new_persons]
        return new_persons

    # CREATE OR UPDATE
    def sync_person_list(self, person_list, website_persons):

        website_data = self.build_website_data_dict(website_persons)

        for person in person_list:
            person_id = str(person.get('id', ''))
            if person_id in website_data.keys():
                consume_person = website_data.pop(person_id)
                try:
                    person_data = self.update_person_by_id(person_id)
                except Exception as err:
                    logger("[Error] Error while updating the person ID: %s" %(person_id), err)
            else:
                try:
                    new_person = self.create_person(person_id)
                except Exception as err:
                    logger("[Error] Error while creating the person ID: %s" %(person_id), err)
        
        if len(website_data.keys()) > 0:
            unpublished_persons = [self.unpublish_person(person_brain.getObject()) for person_brain in website_data.values()]

        return person_list

    # GET
    def get_all_persons(self):
        results = plone.api.content.find(portal_type=self.DEFAULT_CONTENT_TYPE)
        return results

    

     # FIND
    def find_person(self, person_id):
        person_id = self.safe_value(person_id)
        result = plone.api.content.find(person_id=person_id)
        if result:
            return result[0].getObject()
        else:
            raise_error("personNotFoundError", "Person with ID '%s' is not found in Plone" %(person_id))

    # DELETE
    def delete_person_by_id(self, person_id):
        obj = self.find_person(person_id=person_id)
        self.delete_person(obj)  

    def delete_person(self, person):
        plone.api.content.delete(obj=person)

    # PLONE WORKLFLOW - publish
    def publish_person(self, person):
        plone.api.content.transition(obj=person, to_state="published")

    # PLONE WORKLFLOW - unpublish
    def unpublish_person(self, person):
        plone.api.content.transition(obj=person, to_state="private")
        logger("[Status] Unpublished person with ID: '%s'" %(getattr(person, 'person_id', '')))
        return person

    def unpublish_person_by_id(self, person_id):
        obj = self.find_person(person_id=person_id)
        self.unpublish_person(obj)

    #
    # CRUD utils
    # 
    def get_person_data_from_list_by_id(self, person_brain, persons_data):
        person_id = getattr(person_brain, 'person_id', None)
        if person_id and person_id in persons_data:
            return persons_data[person_id]
        else:
            logger("[Error] Person data for '%s' cannot be found." %(person_brain.getURL()), "requestHandlingError")
            return None

    def build_persons_data_dict(self, api_persons):
        persons_data = {}
        for api_person in api_persons:
            if 'id' in api_person:
                persons_data[self.safe_value(api_person['id'])] = api_person
            else:
                logger('[Error] Person ID cannot be found in the API JSON: %s' %(api_person), 'requestHandlingError')
        return persons_data

    def build_website_data_dict(self, website_persons):
        website_persons_data = {}
        for website_person in website_persons:
            person_id = getattr(website_person, 'person_id', None)
            if person_id:
                website_persons_data[self.safe_value(person_id)] = website_person
            else:
                logger('[Error] Person ID value cannot be found in the brain url: %s' %(website_person.getURL()), 'requestHandlingError')
        return website_persons_data

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

    def update_field(self, person, fieldname, fieldvalue):
        plonefield_match = self.match(fieldname)

        if plonefield_match:
            try:
                if not hasattr(person, plonefield_match):
                    logger("[Error] Plone field '%s' does not exist" %(plonefield_match), "Plone field not found")
                    return None

                transform_value = self.transform_special_fields(person, fieldname, fieldvalue)
                if transform_value:
                    return transform_value
                else:
                    setattr(person, plonefield_match, self.safe_value(fieldvalue))
                    return fieldvalue
            except Exception as err:
                logger("[Error] Exception while syncing the API field '%s'" %(fieldname), err)
                return None
        else:
            return None

    def update_all_fields(self, person, person_data):
        self.clean_all_fields(person)
        updated_fields = [(self.update_field(person, field, person_data[field]), field) for field in person_data.keys()]
        person = self.validate_person_data(person, person_data)
        return person

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

    def clean_all_fields(self, person):

        # get all fields from schema
        for fieldname in self.CORE.values():
            if fieldname not in ['person_id']: # TODO: required field needs to come from the settings
                self.clean_field(person, fieldname)

        return person

    def clean_field(self, person, fieldname):
        try:
            setattr(person, fieldname, "")
        except:
            logger("[Error] Field '%s' type is not recognised. " %(fieldname), "Field cannot be cleaned before sync.")

        return person

    def validate_person_data(self, person, person_data):
        validated = True # Needs validation
        if validated:
            person.reindexObject()
            transaction.get().commit()
            return person
        else:
            raise_error("validationError", "Person is not valid. Do not commit changes to the database.")

    #
    # Transform special fields
    # Special methods
    #
    def transform_special_fields(self, person, fieldname, fieldvalue):
        special_field_handler = self.get_special_fields_handlers(fieldname)
        if special_field_handler:
            if fieldvalue:
                special_field_value = special_field_handler(person, fieldname, fieldvalue)
                return special_field_value
            else:
                if fieldname in ['ranks']:
                    return RichTextValue("", 'text/html', 'text/html')
                return fieldvalue
        return False

    def get_special_fields_handlers(self, fieldname):
        SPECIAL_FIELDS_HANDLERS = {
            "title": self._transform_person_title,
            "type": self._transform_person_type
        }

        if fieldname in SPECIAL_FIELDS_HANDLERS:
            return SPECIAL_FIELDS_HANDLERS[fieldname]
        else:
            return None

    def _transform_person_title(self, person, fieldname, fieldvalue):
        setattr(person, 'title', fieldvalue)
        return fieldvalue

    def _transform_person_type(self, person, fieldname, fieldvalue):
        current_subjects = person.Subject()
        if 'team' in current_subjects:
            subjects = ['team', fieldvalue]
            person.setSubject(subjects)
        else:
            person.setSubject([fieldvalue])
            
        return [fieldvalue]
