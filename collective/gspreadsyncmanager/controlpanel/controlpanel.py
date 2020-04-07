# -*- coding: utf-8 -*-
from datetime import date
from plone.app.registry.browser.controlpanel import ControlPanelFormWrapper
from plone.app.registry.browser.controlpanel import RegistryEditForm
from plone.z3cform import layout
from zope import schema
from zope.interface import Interface

class IGSheetsControlPanel(Interface):

    api_scope = schema.TextLine(
        title=u'API scopes (separated by comma)',
        required=False
    )

    api_json_key =  schema.Text(
        title=u'API key (JSON)',
        required=False
    )

    api_spreadsheet_url = schema.TextLine(
        title=u'Spreadsheet url (organizations)',
        required=False
    )

    api_worksheet_name =  schema.TextLine(
        title=u'Spreadsheet worksheet name (organizations)',
        required=False
    )

    api_persons_spreadsheet_url = schema.TextLine(
        title=u'Spreadsheet url (persons)',
        required=False
    )

    api_persons_worksheet_name =  schema.TextLine(
        title=u'Spreadsheet worksheet name (persons)',
        required=False
    )


class OrganizationControlPanelForm(RegistryEditForm):
    schema = IGSheetsControlPanel
    label = u'GSheets api control panel'

class OrganizationControlPanelView(ControlPanelFormWrapper):
    form = OrganizationControlPanelForm



