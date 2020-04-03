#!/usr/bin/env python
# -*- coding: utf-8 -*-

from plone.registry.interfaces import IRegistry
from zope.component import getUtility
from collective.gspreadsyncmanager.controlpanel import IGSheetsControlPanel
from datetime import datetime, timedelta
import plone.api
import transaction
from zope.component import getUtility
from plone.i18n.normalizer.interfaces import IIDNormalizer
from plone.i18n.normalizer import idnormalizer

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
        'test': {
            'url': getattr(settings, 'api_url_test', None),
            'api_key': getattr(settings, 'api_key_test', None)
        },
        'prod': {
            'url': getattr(settings, 'api_url_prod', None),
            'api_key': getattr(settings, 'api_key_prod', None)
        },
        'api_mode': getattr(settings, 'api_prod_mode', None)
    }

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

