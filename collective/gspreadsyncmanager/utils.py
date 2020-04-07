#!/usr/bin/env python
# -*- coding: utf-8 -*-

from plone.registry.interfaces import IRegistry
from zope.component import getUtility
from collective.gspreadsyncmanager.controlpanel.controlpanel import IGSheetsControlPanel
from datetime import datetime, timedelta

import plone.api
import transaction

from zope.component import getUtility
from plone.i18n.normalizer.interfaces import IIDNormalizer
from plone.i18n.normalizer import idnormalizer

import gspread
from oauth2client.service_account import ServiceAccountCredentials

#
# Common definitions
#
ONE_YEAR = 365
DATE_FORMAT = "%Y-%m-%d"


def reverse_onsale_value(organization_ids):
    ids = [str(_id) for _id in organization_ids]

    brains = plone.api.content.find(organization_id=ids)

    for brain in brains:
        if brain.onsale:
            brain.getObject().onsale = False
        else:
            brain.getObject().onsale = True
        brain.getObject().reindexObject()

    transaction.get().commit()
    return True

def get_api_settings():
    registry = getUtility(IRegistry)
    settings = registry.forInterface(IGSheetsControlPanel)
        
    api_settings = {
        'scope': getattr(settings, 'api_scope', None),
        'json_key': getattr(settings, 'api_json_key', None),
        'spreadsheet_url': getattr(settings, 'api_spreadsheet_url', None),
        'worksheet_name': getattr(settings, 'api_worksheet_name', None),
    }   

    api_settings['scope'] = api_settings['scope'].split(',')

    return api_settings

def get_api_settings_persons():
    registry = getUtility(IRegistry)
    settings = registry.forInterface(IGSheetsControlPanel)
        
    api_settings = {
        'scope': getattr(settings, 'api_scope', None),
        'json_key': getattr(settings, 'api_json_key', None),
        'spreadsheet_url': getattr(settings, 'api_persons_spreadsheet_url', None),
        'worksheet_name': getattr(settings, 'api_persons_worksheet_name', None),
    }   

    api_settings['scope'] = api_settings['scope'].split(',')

    return api_settings


def get_datetime_today(as_string=False):
    ## format = YYYY-MM-DD
    today = datetime.today()
    if as_string:
        return today.strftime(DATE_FORMAT)
    else:
        return today

def get_datetime_future(as_string=False, years=20):
    ## format = YYYY-MM-DD
    today = datetime.today()
    time_leap = years*ONE_YEAR
    future = today + timedelta(days=time_leap)
    if as_string:
        date_future = future.strftime(DATE_FORMAT)
        return date_future
    else:
        return future

def str2bool(value):
    return str(value).lower() in ("yes", "true", "t", "1")


def normalize_id(value):
    new_value = idnormalizer.normalize(value, max_length=len(value))
    return new_value

