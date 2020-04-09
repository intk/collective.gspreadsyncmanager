#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# GoogleSheets API sync mechanism by Andre Goncalves
#
import plone.api
import transaction
import requests
from zope.component import queryAdapter, queryMultiAdapter
from plone.uuid.interfaces import IUUID
from zope import event

# Plone dependencies
from zope.schema.interfaces import ITextLine, ITuple, IBool
from plone.app.textfield.interfaces import IRichText
from plone.app.textfield.value import RichTextValue
from zope.schema import getFieldsInOrder
from plone.event.interfaces import IEventAccessor
from datetime import datetime
from zope.component import getUtility
from plone.i18n.normalizer.interfaces import IIDNormalizer
from plone.namedfile.file import NamedBlobImage, NamedBlobFile
from plone.app.multilingual.interfaces import ITranslationManager

# Product dependencies
from collective.person.interfaces import IPerson
from eea.cache.event import InvalidateMemCacheEvent

# Error handling
from .error_handling.error import raise_error

# Logging module
from .logging.logging import logger

# Utils
from .utils import str2bool, normalize_id, phonenumber_to_id
from .utils import get_datetime_today, get_datetime_future, DATE_FORMAT

class SyncManager(object):
    #
    # Init methods 
    #  
    DEFAULT_CONTENT_TYPE = "Person" # TODO: should come from settings
    DEFAULT_FOLDER = "/en/about/team" # TODO: should come from settings
    DOWNLOAD_URL_TEMPLATE = "https://drive.google.com/u/1/uc?id=%s&export=download"
    MAIN_LANGUAGE = "en"
    EXTRA_LANGUAGES = ["nl"]
    TRANSLATABLE_FIELDS = ['title', 'phone', 'email', 'pictureUrl', 'image']

    DEFAULT_FOLDERS = {
        "en": "/en/about/team",
        "nl": "/nl/over/team"
    }

    def __init__(self, options):
        self.options = options
        self.gsheets_api = self.options['api']
        self.CORE = self.options['core']

    #
    # Sync operations 
    #
    def update_person_by_id(self, person_id, person_data=None):
        if person_id:
            person = self.find_person(person_id)

            if not person_data:
                person_data = self.gsheets_api.get_person_by_id(person_id)

            updated_person = self.update_person(person_id, person, person_data)

            # translate person
            for extra_language in self.EXTRA_LANGUAGES:
                translated_person = self.translate_person(updated_person, person_id, extra_language)

            if not person_data:
                cache_invalidated = self.invalidate_cache()

            return updated_person
        else:
            return None

    def update_persons(self, create_and_unpublish=False):
        person_list = self.gsheets_api.get_all_persons()
        
        if create_and_unpublish:
            website_persons = self.get_all_persons()
            self.sync_person_list(person_list, website_persons)
        else:
            self.update_person_list(person_list)

        cache_invalidated = self.invalidate_cache()
        
        return person_list

    #
    # CRUD operations
    #

    # UPDATE
    def update_person(self, person_id, person, person_data):
        updated_person = self.update_all_fields(person, person_data)

        state = plone.api.content.get_state(obj=person)
        if state != "published":
            updated_person = self.publish_person(person)

        logger("[Status] Person with ID '%s' is now updated. URL: %s" %(person_id, person.absolute_url()))
        return updated_person


    # TRANSLATIONS

    def translate_person(self, person, person_id, language="nl"):
        translated_person = self.check_translation_exists(person, language)

        if translated_person:
            translated_person = self.update_person_translation(person, translated_person)
            logger("[Status] '%s' translation of Person with ID '%s' is now updated. URL: %s" %(language, person_id, translated_person.absolute_url()))
        else:
            translated_person = self.create_person_translation(person, language)
            logger("[Status] Person with ID '%s' is now translated to '%s'. URL: %s" %(person_id, language, translated_person.absolute_url()))

        state = plone.api.content.get_state(obj=translated_person)
        if state != "published":
            translated_person = self.publish_person(translated_person)

        translated_person = self.validate_person_data(translated_person, None)
        return translated_person

    def check_translation_exists(self, person, language):
        has_translation = ITranslationManager(person).has_translation(language)
        if has_translation:
            translated_person = ITranslationManager(person).get_translation(language)
            return translated_person
        else:
            return False

    def update_person_translation(self, person, translated_person):
        translated_person = self.copy_fields_to_translation(person, translated_person)
        return translated_person

    def create_person_translation(self, person, language):
        ITranslationManager(person).add_translation(language)
        translated_person = ITranslationManager(person).get_translation(language)
        translated_person = self.copy_fields_to_translation(person, translated_person)
        return translated_person

    def copy_fields_to_translation(self, person, translated_person):

        for fieldname in self.TRANSLATABLE_FIELDS:
            setattr(translated_person, fieldname, getattr(person, fieldname, ''))

        original_subjects = person.Subject()
        translated_person.setSubject(original_subjects)

        return translated_person


    # CREATE
    def create_person(self, person_id, person_data=None):
        if not person_data:
            person_data = self.gsheets_api.get_person_by_id(person_id)
        
        try:
            title = person_data['fullname']
            new_person_id = normalize_id(title)

            container = self.get_container()
            new_person = plone.api.content.create(container=container, type=self.DEFAULT_CONTENT_TYPE, id=new_person_id, safe_id=True, title=title)
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

        for person in person_list.values():
            person_id = str(person.get('_id', ''))

            if person_id:
                # Update
                if person_id in website_data.keys():
                    consume_person = website_data.pop(person_id)
                    try:
                        person_data = self.update_person_by_id(person_id, person)
                    except Exception as err:
                        logger("[Error] Error while updating the person ID: %s" %(person_id), err)
                # Create
                else:
                    try:
                        new_person = self.create_person(person_id, person)
                    except Exception as err:
                        logger("[Error] Error while creating the person ID: '%s'" %(person_id), err)
            else:
                # TODO: log error
                pass
        
        if len(website_data.keys()) > 0:
            unpublished_persons = [self.unpublish_person(person_brain.getObject()) for person_brain in website_data.values()]

        return person_list

    def update_person_list(self, person_list):
        for person in person_list.values():
            person_id = person.get('_id', '')
            try:
                person_data = self.update_person_by_id(person_id, person)
            except Exception as err:
                logger("[Error] Error while requesting the sync for the person ID: '%s'" %(person_id), err)
        
        return person_list

    # GET
    def get_all_persons(self):
        results = plone.api.content.find(portal_type=self.DEFAULT_CONTENT_TYPE, Language=self.MAIN_LANGUAGE)
        return results

     # FIND
    def find_person(self, person_id):
        person_id = self.safe_value(person_id)
        result = plone.api.content.find(person_id=person_id, Language=self.MAIN_LANGUAGE)

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
        logger("[Status] Published person with ID: '%s'" %(phonenumber_to_id(getattr(person, 'phone', ''), getattr(person, "title", ""))))
        return person

    # PLONE WORKLFLOW - unpublish
    def unpublish_person(self, person):
        plone.api.content.transition(obj=person, to_state="private")
        person.reindexObject()
        logger("[Status] Unpublished person with ID: '%s'" %(phonenumber_to_id(getattr(person, 'phone', ''), getattr(person, "title", ""))))

        translated_person = self.check_translation_exists(person, 'nl') #TODO: needs fix for language
        if translated_person:
            plone.api.content.transition(obj=translated_person, to_state="private")
            translated_person.reindexObject()
            logger("[Status] Unpublished person translation with ID: '%s'" %(phonenumber_to_id(getattr(person, 'phone', ''), getattr(person, "title", ""))))

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
            if fieldname not in ['person_id', 'pictureUrl']: # TODO: required field needs to come from the settings
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
            special_field_value = special_field_handler(person, fieldname, fieldvalue)
            return special_field_value

        return False

    def get_special_fields_handlers(self, fieldname):
        SPECIAL_FIELDS_HANDLERS = {
            "title": self._transform_person_title,
            "type": self._transform_person_type,
            "picture": self._transform_person_picture
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
        if 'frontpage' in current_subjects:
            subjects = ['frontpage', fieldvalue]
            person.setSubject(subjects)
        elif 'frontpage-collection' in current_subjects:
            subjects = ['frontpage-collection', fieldvalue]
            person.setSubject(subjects)
        else:
            person.setSubject([fieldvalue])
            
        return [fieldvalue]

    def _transform_person_picture(self, person, fieldname, fieldvalue):
        url = fieldvalue

        current_url = getattr(person, 'pictureUrl', None)

        if url:
            if not current_url:
                image_created_url = self.add_image_to_person(url, person)
                setattr(person, 'pictureUrl', url)
            elif current_url != url:
                image_created_url = self.add_image_to_person(url, person)
                setattr(person, 'pictureUrl', url)
            else:
                setattr(person, 'pictureUrl', url)
                return url

            return url
        else:
            setattr(person, 'image', None)
            return url

    def get_drive_file_id(self, url):
        try:
            if "drive.google.com/open" in url:
                file_id = url.split("id=")[1]
            else:
                file_id = url.split("/")[5]
            return file_id
        except:
            # TODO: log error
            return None

    def generate_image_url(self, url):
        
        file_id = self.get_drive_file_id(url)

        if file_id:
            image_url = self.DOWNLOAD_URL_TEMPLATE %(file_id)
            return image_url
        else:
            # TODO: log error
            return None

    # Utils
    def download_image(self, url):
        if url:
            try:
                img_request = requests.get(url, stream=True)
                if img_request:
                    if 'text/xml' in img_request.headers.get('content-type'):
                        # TODO : Log error
                        return None
                    img_data = img_request.content
                    return img_data
                else:
                    # TODO: log error
                    return None
            except:
                # TODO: log error
                return None
        else:
            return None

    def get_image_blob(self, img_data):
        if img_data:
            image_data = img_data
            img_blob = NamedBlobImage(data=image_data)

            return img_blob
        else:
            return None

    def add_image_to_person(self, url, person):
        image_url = self.generate_image_url(url)
        image_data = self.download_image(image_url)
        image_blob = self.get_image_blob(image_data)

        if image_blob:
            setattr(person, 'image', image_blob)
            return url
        else:
            return url

    def invalidate_cache(self):
        """container = self.get_container()
        uid = queryAdapter(container, IUUID)
        if uid:
            event.notify(InvalidateMemCacheEvent(raw=True, dependencies=[uid]))"""
        return True





