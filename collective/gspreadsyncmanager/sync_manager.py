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
from plone.organization.interfaces import IEventAccessor
from datetime import datetime
from zope.component import getUtility
from plone.i18n.normalizer.interfaces import IIDNormalizer

# Product dependencies
from collective.behavior.organization.behavior import IOrganization
from .error import raise_error
from .logging import logger
from .utils import str2bool, normalize_id

from collective.gspreadsyncmanager.utils import get_datetime_today, get_datetime_future, DATE_FORMAT

class SyncManager(object):
    #
    # Init methods 
    # 
    DEFAULT_CONTENT_TYPE = "Event"
    DEFAULT_FOLDER = "/programma"
    
    AVAILABILITY_FIELDS = ['onsale', 'organizationStatus', 'statusMessage']
    PERFORMANCE_STATUSES_TEXT = {
        "ONSALE": "Bestellen",
        "SOLDOUT": "Uitverkocht",
        "CANCELLED": "Geannuleerd",
        "ONHOLD": "Tijdelijk onbeschikbaar",
        "NOSALE": "Externe verkoop"
    }
    REDIRECT_URL = ""

    def __init__(self, options):
        self.options = options
        self.gsheets_api = self.options['api']
        self.CORE = self.options['core']
        self.fields_schema = getFieldsInOrder(IOrganization)

    #
    # Sync operations
    #
    def update_organization_by_id(self, organization_id, arrangement_list=None):
        organization = self.find_organization(organization_id)
        organization_data = self.gsheets_api.get_organization_availability(organization_id)

        if not arrangement_list:
            arrangement_list = self.gsheets_api.get_arrangement_list_by_organization_id(organization_id, date_from=get_datetime_today(as_string=True), date_until=get_datetime_future(as_string=True))

        updated_organization = self.update_organization(organization_id, organization, organization_data, arrangement_list)

        return updated_organization

    def update_organization_list_by_date(self, date_from, date_until, create_and_unpublish=False):
        organization_list = self.gsheets_api.get_organization_list_by_date(date_from=date_from, date_until=date_until)
        
        if create_and_unpublish:
            website_organizations = self.get_all_organizations(date_from=date_from)
            self.sync_organization_list(organization_list, website_organizations)
        else:
            self.update_organization_list(organization_list)
        
        return organization_list

    def update_availability_by_date(self, date_from, date_until):
        website_organizations = self.get_all_organizations(date_from=date_from)
        api_organizations = self.gsheets_api.get_organization_list_by_date(date_from=date_from, date_until=date_until)
        
        organizations_data = self.build_organizations_data_dict(api_organizations)
        updated_availability = self.update_availability(organizations_data, website_organizations)

        return updated_availability

    #
    # CRUD operations
    #
    def update_organization(self, organization_id, organization, organization_data, arrangement_list=None):
        updated_organization = self.update_all_fields(organization, organization_data, arrangement_list)
        logger("[Status] Organization with ID '%s' is now updated. URL: %s" %(organization_id, organization.absolute_url()))
        return updated_organization

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

    def update_availability(self, organizations_data, website_organizations):
        availability_changed_list = [organization_brain for organization_brain in website_organizations if self.is_availability_changed(organization_brain, self.get_organization_data_from_list_by_id(organization_brain, organizations_data))]
        updated_availability = [self.update_availability_field(organization_brain, organizations_data[organization_brain.organization_id]) for organization_brain in availability_changed_list]
        return updated_availability
        
    def create_new_organizations(self, organizations_data, website_data):
        new_organizations = [api_id for api_id in organizations_data.keys() if api_id not in website_data.keys()]
        created_organizations = [self.create_organization(organization_id) for organization_id in new_organizations]
        return new_organizations

    def update_organization_list(self, organization_list):
        for organization in organization_list:
            organization_id = organization.get('id', '')
            try:
                organization_data = self.update_organization_by_id(organization_id)
            except Exception as err:
                logger("[Error] Error while requesting the sync for the organization ID: %s" %(organization_id), err)
        
        return organization_list

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

    def unpublish_organization(self, organization):
        plone.api.content.transition(obj=organization, to_state="private")
        logger("[Status] Unpublished organization with ID: '%s'" %(getattr(organization, 'organization_id', '')))
        return organization

    def publish_organization(self, organization):
        plone.api.content.transition(obj=organization, to_state="published")

    def delete_organization(self, organization):
        plone.api.content.delete(obj=organization)

    def unpublish_organization_by_id(self, organization_id):
        obj = self.find_organization(organization_id=organization_id)
        self.unpublish_organization(obj)

    def delete_organization_by_id(self, organization_id):
        obj = self.find_organization(organization_id=organization_id)
        self.delete_organization(obj)    

    def get_all_upcoming_organizations(self):
        today = datetime.today()
        results = self.get_all_organizations(date_from=today)
        return results

    def get_all_organizations(self, date_from=None):
        if date_from:
            if isinstance(date_from, str):
                date_from = datetime.strptime(date_from, DATE_FORMAT)
            results = plone.api.content.find(portal_type=self.DEFAULT_CONTENT_TYPE, start={'query': date_from, 'range': 'min'})
            return results
        else:
            results = plone.api.content.find(portal_type=self.DEFAULT_CONTENT_TYPE)
            return results

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


    def update_availability_field(self, organization_brain, organization_data):
        organization = organization_brain.getObject()
        for field in self.AVAILABILITY_FIELDS:
            try:
                setattr(organization, field, organization_data[field])
            except Exception as err:
                logger("[Error] Availability field '%s' cannot be updated for organization ID '%s'."%(field, organization_data.get('id', 'Unknown')), err)

        try:
            organization = self.generate_organization_availability_field(organization, organization_data)
        except Exception as err:
            logger("[Error] Organization availability field value cannot be updated for organization ID '%s'." %(organization_data.get('id', 'Unknown')), err)

        organization.reindexObject()
        transaction.get().commit()
        logger("[Status] Organization availability is now updated for ID: %s" %(organization_brain.organization_id))
        return organization_brain

    def is_availability_changed(self, organization_brain, organization_data):
        ## TODO needs refactoring 
        current_onsale_value = str2bool(organization_brain.onsale)
        if organization_data and 'onsale' in organization_data:
            organization_data_onsale_value = organization_data['onsale']
            if organization_data_onsale_value != current_onsale_value:
                logger('[Status] Availability field is changed for the organization ID: %s.' %(organization_brain.organization_id))
                return True
            else:
                logger('[Status] Availability field is NOT changed for the organization ID: %s.' %(organization_brain.organization_id))
                return False
        elif not organization_data:
            return False
        else:
            if getattr(organization_brain, 'organization_id', None):
                logger("[Error] Organization 'onsale' field is not available for the ID '%s'." %(organization_brain.organization_id), 'requestHandlingError')
            else:
                return False

    def find_organization(self, organization_id):
        organization_id = self.safe_value(organization_id)
        result = plone.api.content.find(organization_id=organization_id)
        if result:
            return result[0].getObject()
        else:
            raise_error("organizationNotFoundError", "Organization with ID '%s' is not found in Plone" %(organization_id))

    def find_product_details_by_id(self, product_id, scale="mini"):
        product_id = self.safe_value(product_id)
        result = plone.api.content.find(product_id=product_id)
        if result:
            product_description = result[0].Description
            lead_image_scale_url = ""
            leadMedia = getattr(result[0], 'leadMedia', None)
            if leadMedia:
                images = plone.api.content.find(UID=leadMedia)
                if images:
                    lead_image = images[0]
                    lead_image_url = lead_image.getURL()
                    lead_image_scale_url = "%s/@@images/image/%s" %(lead_image_url, scale)
            return lead_image_scale_url, product_description

        else:
            return "", ""

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

    def update_all_fields(self, organization, organization_data, arrangement_list=None):
        self.clean_all_fields(organization)
        updated_fields = [(self.update_field(organization, field, organization_data[field]), field) for field in organization_data.keys()]
        organization = self.generate_organization_availability_field(organization, organization_data)
        organization = self.generate_arrangement_list_field(organization, arrangement_list)

        organization = self.validate_organization_data(organization, organization_data)
        return organization

    def get_container(self):
        container = plone.api.content.get(path=self.DEFAULT_FOLDER)
        return container

    #
    # Sanitising/validation methods
    #

    def generate_organization_availability_field(self, organization, organization_data):
        fieldvalue = self.generate_availability_html(organization_data)
        setattr(organization, 'organization_availability', fieldvalue)
        return organization

    def generate_arrangement_list_field(self, organization, arrangement_list):
        fieldvalue = self.generate_arrangement_list_html(arrangement_list)
        setattr(organization, 'arrangements', fieldvalue)
        return organization

    def generate_availability_html(self, organization_data):
        organizationStatus = organization_data.get('organizationStatus', )
        onsale = organization_data.get('onsale', '')
        if organizationStatus:
            if organizationStatus != "ONSALE":
                availability_value = self.get_availability_html(organizationStatus, organization_data)
                final_value = RichTextValue(availability_value, 'text/html', 'text/html')
                return final_value
            else:
                if onsale == True:
                    availability_value = self.get_availability_html(organizationStatus, organization_data)
                    final_value = RichTextValue(availability_value, 'text/html', 'text/html')
                    return final_value
                elif onsale == False:
                    availability_value = self.get_availability_html("SOLDOUT", organization_data)
                    final_value = RichTextValue(availability_value, 'text/html', 'text/html')
                    return final_value
                else:
                    final_value = RichTextValue("", 'text/html', 'text/html')
                    return final_value
        else:
            logger('[Error] Organization status is not available. Cannot update the availability field.', 'requestHandingError')
            final_value = RichTextValue("", 'text/html', 'text/html')
            return final_value


    def generate_arrangement_list_html(self, arrangement_list):
        if arrangement_list:
            arrangements_html = [self.get_arrangement_html(arrangement) for arrangement in arrangement_list]
            final_arrangements_list = "<h3>Arrangementen</h3>"
            final_arrangements_list += "".join(arrangements_html)
            final_value = RichTextValue(final_arrangements_list, 'text/html', 'text/html')
            return final_value
        else:
            final_value = RichTextValue("", 'text/html', 'text/html')
            return final_value

    def get_arrangement_html(self, arrangement):
        title = arrangement.get('shortTitle', '')
        arrangement_id = arrangement.get('id', '')
        product_id = arrangement.get('product_id', '')
        image_url = ""
        if product_id:
            image_url, description = self.find_product_details_by_id(product_id)

        if image_url:
            arrangement_html = "<div class='arrangement-wrapper'><div class='arrangement-image'><a href='%s/%s'><img src='%s'/></a></div><div class='arrangement-details'><h4><a href='%s/%s'>%s</a><h4><p class='arrangement-description'>%s</p></div></div>" %(self.REDIRECT_URL, arrangement_id, image_url, self.REDIRECT_URL, arrangement_id, title, description)
        else:
            arrangement_html = "<div class='arrangement-wrapper'><div class='arrangement-details'><h4><a href='%s/%s'>%s</a></h4><p class='arrangement-description'>%s</p></div></div>" %(self.REDIRECT_URL, arrangement_id, title, description)
        
        return arrangement_html

    def get_availability_html(self, organizationStatus, organization_data):
        field_text = self.get_availability_status_text(organizationStatus)
        disabled_state = ""
        if organizationStatus != "ONSALE":
            disabled_state = "disabled"

        if field_text:
            if organizationStatus == "NOSALE":
                availability_html = "<a class='btn btn-default' %s>%s</a>" %(disabled_state, field_text)
                return availability_html
            elif organizationStatus != "ONSALE":
                availability_html = "<a class='btn btn-default' %s>%s</a>" %(disabled_state, field_text)
                return availability_html
            else:
                availability_html = "<a href='%s' class='btn btn-default' %s>%s</a>" %(self.get_redirect_url(organization_data), disabled_state, field_text)
                return availability_html
        else:
            return ""

    def get_availability_status_text(self, organizationStatus):
        if organizationStatus in self.PERFORMANCE_STATUSES_TEXT:
            return self.PERFORMANCE_STATUSES_TEXT[organizationStatus]
        else:
            return None

    def get_redirect_url(self, organization_data):
        endpoint_mode = "stlt=sbhp"
        organization_id = organization_data['id']
        redirect_url = "%s/%s?%s" %(self.REDIRECT_URL, str(organization_id), endpoint_mode)
        return redirect_url

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
            if fieldname not in ['organization_id', 'waiting_list']:
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

    def validate_dates(self, organization, organization_data):
        startDateTime = organization_data.get('startDateTime', '')
        endDateTime = organization_data.get('endDateTime', '')

        if startDateTime and not endDateTime:
            organization_date_fields = IEventAccessor(organization)
            organization_date_fields.end = organization_date_fields.start
            return True
        
        if not startDateTime and not endDateTime:
            logger("[Error] There are no dates for the organization. ", "Organization dates cannot be found.")
            return False

        return True

    def validate_organization_data(self, organization, organization_data):
        validated = self.validate_dates(organization, organization_data)
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
            "organizationGenre": self._transform_organization_genre,
            "startDateTime": self._transform_start_date,
            "endDateTime": self._transform_end_date,
            "tags": self._transform_tags,
            "ranks": self._transform_ranks
        }

        if fieldname in SPECIAL_FIELDS_HANDLERS:
            return SPECIAL_FIELDS_HANDLERS[fieldname]
        else:
            return None

    def _transform_organization_title(self, organization, fieldname, fieldvalue):
        setattr(organization, 'organization_title', fieldvalue)
        return fieldvalue

    def _transform_organization_genre(self, organization, fieldname, fieldvalue):
        current_subjects = organization.Subject()
        if 'frontpage-slideshow' in current_subjects:
            subjects = ['frontpage-slideshow', fieldvalue]
            organization.setSubject(subjects)
        else:
            organization.setSubject([fieldvalue])
            
        return [fieldvalue]

    def _transform_start_date(self, organization, fieldname, fieldvalue):
        organization_date_fields = IEventAccessor(organization)
        date_datetime = datetime.strptime(fieldvalue, '%Y-%m-%d %H:%M')
        organization_date_fields.start = date_datetime
        return fieldvalue

    def _transform_end_date(self, organization, fieldname, fieldvalue):
        organization_date_fields = IEventAccessor(organization)
        date_datetime = datetime.strptime(fieldvalue, '%Y-%m-%d %H:%M')
        organization_date_fields.end = date_datetime
        return fieldvalue

    def _transform_tags(self, organization, fieldname, fieldvalue):
        return fieldvalue

    def _transform_currency(self, currency):
        currencies = {
            "EUR": u'€'
        }

        if currency in currencies:
            return currencies[currency]
        else:
            return currency

    def _transform_ranks_generate_prices(self, rank, multiple_ranks=False):
        prices = rank.get('prices', '')
        final_value = ""

        if prices:
            if len(prices) > 1:
                if not multiple_ranks:
                    final_value = "<strong>Prijzen</strong>"

                default_prices = []
                available_prices = []

                for price in prices:
                    priceTypeDescription = price.get('priceTypeDescription', '')
                    price_value = price.get('price', '')
                    is_default_price = price.get('isDefault', '')
                    currency = self._transform_currency(price.get('currency', u'€'))
                    new_price = "<span><span class='price-type'>%s</span> %s%s</span>" %(priceTypeDescription, currency, price_value)

                    if is_default_price:
                        default_prices.append(new_price)
                    else:
                        available_prices.append(new_price)
                        
                generated_prices = default_prices+available_prices
                final_value += "<div class='list-prices'>"+"".join(generated_prices)+"</div>"
                return final_value
            elif len(prices) == 1:
                if not multiple_ranks:
                    final_value = "<strong>Prijs</strong>"

                price = prices[0]
                price_value = price.get('price', '')
                currency = self._transform_currency(price.get('currency', u'€'))
                final_value += "<span>%s%s</span>" %(currency, price_value)
                return final_value
            else:
                return ""
        else:
            return ""

    def _transform_ranks(self, organization, fieldname, fieldvalue):

        html_value = ""
        if len(fieldvalue) > 1:
            html_value = "<strong>Prijzen</strong>"
            for rank in fieldvalue:
                rankDescription = rank.get('rankDescription')
                prices = self._transform_ranks_generate_prices(rank, True)
                html_value += "<h6>%s</h6><div>%s</div>" %(rankDescription, prices)
            final_value = RichTextValue(html_value, 'text/html', 'text/html')
            setattr(organization, 'price', final_value)

        elif len(fieldvalue) == 1:
            rank = fieldvalue[0]
            prices = self._transform_ranks_generate_prices(rank)
            html_value += "<div>%s</div>" %(prices)
            final_value = RichTextValue(html_value, 'text/html', 'text/html')
            setattr(organization, 'price', final_value)
        else:
            html_value = ""
            final_value = RichTextValue(html_value, 'text/html', 'text/html')
            return final_value
        final_value = RichTextValue(html_value, 'text/html', 'text/html')   
        return final_value
