Introduction
============

Provides methods to sync content from GoogleSheets API

Features
============
- Synchronization between GoogleSheets API and Plone content.
- Connection with the GoogleSheets API separated from the content sync. 
- Other API connections can be plugged into the sync. The main sources of data for this project are Google Sheets. 

Installation
===================
If you are using zc.buildout and the plone.recipe.zope2instance recipe to manage your project, you can do this:
Add collective.gspreadsyncmanager to the list of eggs to install, e.g.::

	[buildout]
		…
		eggs =
			…
			collective.gspreadsyncmanager

How to use method as a cron job?
=======================================================
Add to your buildout.cfg::

	zope-conf-additional = 
	<clock-server> 
		method /SiteName/gsheets_sync 
		period 60 
		user username-to-invoke-method-with
		password password-for-user 
		host localhost 
	</clock-server>

Dependencies
===============
- gspread
- oauth2client

The following dependencies are not required unless the creation of pictures and translations is requested.

- plone.namedfile
- plone.app.multilingual 
