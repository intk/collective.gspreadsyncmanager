#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# GoogleSheets API sync mechanism by Andre Goncalves
#
from email.mime import image
import plone.api
import transaction
import requests
from zope.component import queryAdapter, queryMultiAdapter
from plone.uuid.interfaces import IUUID
from zope import event
import imghdr

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
#from eea.cache.event import InvalidateMemCacheEvent
#from collective.taxonomy.interfaces import ITaxonomy
from zope.component import queryUtility

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
    DEFAULT_CONTENT_TYPE = "organization" # TODO: should come from settings
    ALTERNATIVE_ID = "google_ads_id"

    DOWNLOAD_URL_TEMPLATE = "https://drive.google.com/u/1/uc?id=%s&export=download"
    MAIN_LANGUAGE = "en"
    EXTRA_LANGUAGES = ["nl"]
    TRANSLATABLE_FIELDS = ['title', 'google_ads_id', 'pictureUrl', 'image'] #'taxonomy_cultural_organizations']

    DEFAULT_FOLDER = "/en/organizations" # TODO: should come from settings
    DEFAULT_FOLDERS = {
        "en": "/en/organizations",
        "nl": "/nl/organisaties"
    }

    TAXONOMY_NAME = "taxonomy_cultural_organizations" # TODO: should come from settings


    def __init__(self, options):
        self.options = options
        self.gsheets_api = self.options['api']
        self.CORE = self.options['core']

        #self.taxonomy_utility = queryUtility(ITaxonomy, name='collective.taxonomy.cultural_organizations')
        self.taxonomy_data = None #self.taxonomy_utility.data

    #
    # Sync operations 
    #
    def update_organization_by_id(self, organization_id, organization_data=None, translate=True):

        if organization_id:
            organization = self.find_organization(organization_id)

            if not organization_data:
                organization_data = self.gsheets_api.get_organization_by_id(organization_id)

            updated_organization = self.update_organization(organization_id, organization, organization_data)

            if not organization_data:
                cache_invalidated = self.invalidate_cache()

            return updated_organization
        else:
            return None

    def update_organizations(self, create_and_unpublish=False):
        organization_list = self.gsheets_api.get_all_organizations()
        
        if create_and_unpublish:
            website_organizations = self.get_all_organizations()
            self.sync_organization_list(organization_list, website_organizations)
        else:
            self.update_organization_list(organization_list)

        cache_invalidated = self.invalidate_cache()
        
        transaction.get().commit()
        
        return organization_list

    #
    # CRUD operations
    #

    # UPDATE
    def update_organization(self, organization_id, organization, organization_data, translate=True):
        updated_organization = self.update_all_fields(organization, organization_data)

        updated_organization = self.publish_based_on_current_state(organization)

        # DO NOT translate
        #    for extra_language in self.EXTRA_LANGUAGES:
        #        translated_organization = self.translate_organization(updated_organization, organization_id, extra_language)

        organization = self.validate_organization_data(organization, organization_data)

        logger("[Status] Organization with ID '%s' is now updated. URL: %s" %(organization_id, organization.absolute_url()))
        return updated_organization


    # TRANSLATIONS

    def translate_organization(self, organization, organization_id, language="nl"):
        translated_organization = self.check_translation_exists(organization, language)

        if translated_organization:
            translated_organization = self.update_organization_translation(organization, translated_organization)
            logger("[Status] '%s' translation of Organization with ID '%s' is now updated. URL: %s" %(language, organization_id, translated_organization.absolute_url()))
        else:
            translated_organization = self.create_organization_translation(organization, language)
            logger("[Status] Organization with ID '%s' is now translated to '%s'. URL: %s" %(organization_id, language, translated_organization.absolute_url()))

        translated_organization = self.publish_based_on_current_state(translated_organization)
        translated_organization = self.validate_organization_data(translated_organization, None)
        
        return translated_organization

    def check_translation_exists(self, organization, language):
        has_translation = ITranslationManager(organization).has_translation(language)
        if has_translation:
            translated_organization = ITranslationManager(organization).get_translation(language)
            return translated_organization
        else:
            return False

    def update_organization_translation(self, organization, translated_organization):
        translated_organization = self.copy_fields_to_translation(organization, translated_organization)
        return translated_organization

    def create_organization_translation(self, organization, language):
        ITranslationManager(organization).add_translation(language)
        translated_organization = ITranslationManager(organization).get_translation(language)
        translated_organization = self.copy_fields_to_translation(organization, translated_organization)
        return translated_organization

    def copy_fields_to_translation(self, organization, translated_organization):

        for fieldname in self.TRANSLATABLE_FIELDS:
            setattr(translated_organization, fieldname, getattr(organization, fieldname, ''))

        original_subjects = organization.Subject()
        translated_organization.setSubject(original_subjects)

        return translated_organization


    # CREATE
    def create_organization(self, organization_id, organization_data=None):
        if not organization_data:
            organization_data = self.gsheets_api.get_organization_by_id(organization_id)
        
        try:
            title = organization_data['name']
            new_organization_id = normalize_id(title)

            container = self.get_container()
            new_organization = plone.api.content.create(container=container, type=self.DEFAULT_CONTENT_TYPE, id=new_organization_id, safe_id=True, title=title)
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

        for organization in organization_list.values():
            organization_id = str(organization.get('_id', ''))

            if organization_id:
                # Update
                if organization_id in website_data.keys():
                    consume_organization = website_data.pop(organization_id)
                    try:
                        organization_data = self.update_organization_by_id(organization_id, organization)
                    except Exception as err:
                        logger("[Error] Error while updating the organization ID: %s" %(organization_id), err)
                # Create
                else:
                    try:
                        new_organization = self.create_organization(organization_id, organization)
                    except Exception as err:
                        logger("[Error] Error while creating the organization ID: '%s'" %(organization_id), err)
            else:
                # TODO: log error
                pass
        
        if len(website_data.keys()) > 0:
            unpublished_organizations = [self.unpublish_organization(organization_brain.getObject()) for organization_brain in website_data.values()]

        return organization_list

    def update_organization_list(self, organization_list):
        for organization in organization_list.values():
            organization_id = organization.get('_id', '')
            try:
                organization_data = self.update_organization_by_id(organization_id, organization)
            except Exception as err:
                logger("[Error] Error while requesting the sync for the organization ID: '%s'" %(organization_id), err)
        
        return organization_list

    # GET
    def get_all_organizations(self):
        results = plone.api.content.find(portal_type=self.DEFAULT_CONTENT_TYPE, Language=self.MAIN_LANGUAGE)
        return results

     # FIND
    def find_organization(self, organization_id):
        organization_id = self.safe_value(organization_id)
        result = plone.api.content.find(organization_id=organization_id, Language=self.MAIN_LANGUAGE)

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
    def publish_based_on_current_state(self, organization):
        state = plone.api.content.get_state(obj=organization)
        if state != "published":
            if getattr(organization, 'preview_image', None):
                updated_organization = self.publish_organization(organization)
        else:
            if not getattr(organization, 'preview_image', None):
                updated_organization = self.unpublish_organization(organization)

        return organization

    def publish_organization(self, organization):
        plone.api.content.transition(obj=organization, to_state="published")
        logger("[Status] Published organization with ID: '%s'" %(getattr(organization, 'google_ads_id', '')))
        return organization

    # PLONE WORKLFLOW - unpublish
    def unpublish_organization(self, organization):
        #plone.api.content.transition(obj=organization, to_state="private")
        #logger("[Status] Unpublished organization with ID: '%s'" %(getattr(organization, 'google_ads_id', '')))

        #translated_organization = self.check_translation_exists(organization, 'nl') #TODO: needs fix for language
        #if translated_organization:
        #    plone.api.content.transition(obj=translated_organization, to_state="private")
        #    logger("[Status] Unpublished organization translation with ID: '%s'" %(getattr(organization, 'google_ads_id', '')))

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

    def build_website_data_dict(self, website_organizations):
        website_organizations_data = {}
        for website_organization in website_organizations:
            organization_id = getattr(website_organization, 'organization_id', None)
            if not organization_id:
                organization_id = getattr(website_organization, self.ALTERNATIVE_ID, None)
            if organization_id:
                website_organizations_data[self.safe_value(organization_id)] = website_organization
            else:
                logger('[Error] Organization ID value cannot be found in the brain. URL: %s' %(website_organization.getURL()), 'requestHandlingError')
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
        for fieldname in self.CORE.values():
            if fieldname not in ['organization_id', 'pictureUrl', 'google_ads_id']: # TODO: required field needs to come from the settings
                self.clean_field(organization, fieldname)

        return organization

    def clean_field(self, organization, fieldname):
        try:
            setattr(organization, fieldname, "")
        except:
            logger("[Error] Field '%s' type is not recognised. " %(fieldname), "Field cannot be cleaned before sync.")

        return organization

    def validate_organization_data(self, organization, organization_data):
        validated = True # Needs validation
        if validated:
            organization.reindexObject(idxs=["Title", "country", "Subject", "organization_id"])
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
            special_field_value = special_field_handler(organization, fieldname, fieldvalue)
            return special_field_value

        return False

    def get_special_fields_handlers(self, fieldname):
        SPECIAL_FIELDS_HANDLERS = {
            "title": self._transform_organization_title,
            "type": self._transform_organization_type,
            "picture": self._transform_organization_picture,
            "country": self._transform_organization_country
        }

        if fieldname in SPECIAL_FIELDS_HANDLERS:
            return SPECIAL_FIELDS_HANDLERS[fieldname]
        else:
            return None

    def _transform_organization_title(self, organization, fieldname, fieldvalue):
        setattr(organization, 'title', fieldvalue)
        return fieldvalue

    def _transform_organization_type(self, organization, fieldname, fieldvalue):
        current_subjects = organization.Subject()
        if 'frontpage' in current_subjects:
            subjects = ['frontpage', fieldvalue]
            organization.setSubject(subjects)
        elif 'main-organization-page' in current_subjects:
            subjects = ['main-organization-page', fieldvalue]
            organization.setSubject(subjects)
        else:
            organization.setSubject([fieldvalue])

        """taxonomy_id = self.get_taxonomy_id(fieldvalue)
        taxonomies = getattr(organization, self.TAXONOMY_NAME, [])

        if not taxonomies:
            taxonomies = []

        if taxonomy_id not in taxonomies:
            taxonomies.append(taxonomy_id)
            setattr(organization, self.TAXONOMY_NAME, taxonomies)"""
            
        return [fieldvalue]

    def _transform_organization_country(self, organization, fieldname, fieldvalue):
        if fieldvalue:
            all_countries = fieldvalue.split(',')
            all_countries_transform = [country.strip() for country in all_countries]
            current_subjects = organization.Subject()
            for country in all_countries_transform:
                current_subjects = list(current_subjects)
                current_subjects.append(country)
            organization.setSubject(current_subjects)
            setattr(organization, 'country', all_countries_transform[0])
        else:
            setattr(organization, 'country', '')

        return [fieldvalue]


    def get_taxonomy_id(self, taxonomy):
        taxonomy_id = None
        
        for taxonomy_name in self.taxonomy_data[self.MAIN_LANGUAGE]:
            if taxonomy in taxonomy_name:
                taxonomy_id = self.taxonomy_data[self.MAIN_LANGUAGE][taxonomy_name]
                return taxonomy_id

        return taxonomy_id


    def _transform_organization_picture(self, organization, fieldname, fieldvalue):
        url = fieldvalue

        current_url = getattr(organization, 'pictureUrl', None)

        if url:
            if not current_url:
                image_created_url = self.add_image_to_organization(url, organization)
                setattr(organization, 'pictureUrl', url)
            elif current_url != url:
                image_created_url = self.add_image_to_organization(url, organization)
                setattr(organization, 'pictureUrl', url)
            else:
                setattr(organization, 'pictureUrl', url)
                return url

            return url
        else:
            setattr(organization, 'preview_image', None)
            return url

    def get_drive_file_id(self, url):
        try:
            if "drive.google.com/open" in url:
                file_id = url.split("id=")[1]
                file_id = file_id.split('&')[0]
                file_id = file_id.split('?')[0]
            else:
                file_id = url.split("/")[5]
                file_id = file_id.split('&')[0]
                file_id = file_id.split('?')[0]
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
                    img_headers = img_request.headers.get('content-type')

                    if 'text/xml' in img_headers or "text/html" in img_headers:
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


    def is_valid_image(self, image_data):
        image_type = imghdr.what(None, image_data)
        if image_type:
            return True
        else:
            return False

    def add_image_to_organization(self, url, organization):
        
        image_id = self.get_drive_file_id(url)
        image_data = self.gsheets_api.download_media_by_id(image_id)

        if self.is_valid_image(image_data):
            image_blob = self.get_image_blob(image_data)
        else:
            image_blob = None

        if image_blob:
            setattr(organization, 'preview_image', image_blob)
            return url
        else:
            setattr(organization, 'preview_image', None)
            return url

    def invalidate_cache(self):
        """container = self.get_container()
        uid = queryAdapter(container, IUUID)
        if uid:
            event.notify(InvalidateMemCacheEvent(raw=True, dependencies=[uid]))"""
        return True





