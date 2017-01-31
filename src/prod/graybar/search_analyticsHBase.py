'''
Created on Oct 20, 2016

@author: obryhbx

This program builds a Google service account credential from file 
'client_secret.json', located in the home directory of the Python script. It 
then requests a daily report using the DIMENSIONS and DEVICE filters in global 
variables. This report is then loaded into the HBase table TABLE_NAME located 
on the server, SERVER_NAME.

'''
#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from apiclient.discovery import build
from datetime import timedelta, date
from hashlib import md5
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
import happybase

LOOKBACK = timedelta(days=-14)
END_DATE = date.today()
START_DATE = (date.today() + LOOKBACK)

DIMENSIONS = ['country','device','query','page','date']
DEVICES = ['DESKTOP','MOBILE','TABLET']

SCOPES = 'https://www.googleapis.com/auth/webmasters.readonly'
DISCOVERY_URI = 'https://www.googleapis.com/discovery/v1/apis/webmasters/v3/rest'
PROPERTY_URI= {'A:':'http://www.graybar.com','B:':'http://www.graybar.com/store/en/gb/'}            
TABLE_NAME = 'gsc_prod' #HBase table name
SERVER_NAME = 'myserver.com' #Example server name

KEYFILE_LOCATION = 'client_secret.json'

def main():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEYFILE_LOCATION, SCOPES)
    http_auth = credentials.authorize(Http())
    searchanalytics = build('webmasters', 'v3', http=http_auth, 
                            discoveryServiceUrl=DISCOVERY_URI)
    for single_date in daterange(START_DATE, END_DATE):
        reportDate = single_date.strftime("%Y-%m-%d")
        for device in DEVICES:
            request = {
                    'startDate': reportDate,
                    'endDate': reportDate,
                    'dimensions': DIMENSIONS,
                    'dimensionFilterGroups': [{
                        'filters': [{
                            'dimension': 'device',
                            'expression': device
                            }]
                         }],
                    'aggregationType':'byPage',
                    'rowlimit':5000,
                    }
            for cf, uri in PROPERTY_URI.iteritems():
                response = execute_request(searchanalytics, uri, request)
                print device + "_" + reportDate + " report downloaded"
                load_hbase(cf, response)
                print device + "_" + reportDate + " data loaded to Hbase"
    print "Data ingest complete"
def daterange(start_date, end_date):
    """ Iterates over a date range.
    
    Args:
        start_date: The datetime.date start of the date range.
        end_date: The datetime.date end of the date range.
    
    Returns:
        The next datetime.date object in the series.
    """
    
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def execute_request(service, property_uri, request):
    """Executes a searchAnalytics.query request.

    Args:
        service: The webmasters service to use when executing the query.
        property_uri: The site or app URI to request data for.
        request: The request to be executed.

    Returns:
        An array of response rows.
    """
    return service.searchanalytics().query(
            siteUrl=property_uri, body=request).execute()


def load_hbase(cf, response):
    """Opens connection to Hbase database & table, loads a batch of dict data 
       into an HBase table using happybase. Closes connection when done.
    
    Args:
        cf: Column family to store data
        response: Google Search Console array list
    
    """
    connection = happybase.Connection(SERVER_NAME)
    connection.open()
    table = connection.table(TABLE_NAME)
    dataBatch = table.batch()
        
    rowList = response.get('rows', [])
    for listItem in rowList:
        dataToLoad = {}
        rowKey = []
        for key, value in listItem.iteritems():
            if key == 'keys':
                dimensionValues = listItem['keys']
                for header, dimension in zip(DIMENSIONS, dimensionValues):
                    rowKey.append(dimension.encode('ascii','replace'))
                    dataToLoad[cf + header] = str(dimension.encode('ascii','replace'))
            else:
                dataToLoad[cf + key] = str(value)
        rowKeyStr = str(rowKey[0]) + str(rowKey[1]) + '_' + str(rowKey[2]) + '_' + md5(rowKey[3]).hexdigest() + '_' + str(rowKey[4])
        dataBatch.put(rowKeyStr, dataToLoad)
    dataBatch.send()
    connection.close()
    
if __name__ == '__main__':
    main()