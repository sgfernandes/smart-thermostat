# -*- coding: utf-8 -*-
"""

Created on Wed May  31 11:30:52 2023

# Name: func_schedule_fault_detection_correction.py
# Version: 1.0


@author: Yimin Chen

# contact: YiminChen@lbl.gov

# Copyright Reserved @ Lawrence Berkeley National Laboratory 
# Funded by the U.S. Department of Energy


# This is for the smart thermostat automated fault correction
# The purpose is to detect and correct software setting related faults in the thermalstat.
# The thermostat provides the OpenAPI to enable the two-way communication between \\
# the FDD and correction (FDD-C) algorithm and the thermostat

# The two-way OpenAPI used is from the Pelican thermostat product
# Reference: https://www.pelicanwireless.com/help-center/thermostat-api-overview/ 

# The proof-of-concept (PoC) FDD-C algorithm was tested at the LBNL FLEXLab testbed
# The PoC FDD-C algorithm can be run on the users' desktop/laptop which is installed \\
# Python 3.7 and above environment
# The FDD-C algorithm calls thermostat data which are collected by the Pelican thermostat \\
# and are stored on the Pelican cloud-base server.
# Two-say communication OpenAPI interfaces with the Pelican cloud-base server to query data \\
# and write back data (i.e., setpoints) through the Pelican gateway installed locally.



"""

import requests
import xmltodict
import collections
import sys
import uuid
import time
import csv
from datetime import datetime
import os

import pandas as pd
from datetime import datetime

from func_pelican_learn_schedule import schedule_detection

class pelican():
    """
    The pelican client is a collection of functions that allows the user to
    access the PelicanWireless thermostats via the PelicanWireless web api.
    The API allows for the reading and writing of thermostat setttings.
    """
    def __init__(self,
                 site='lbnlselfcorrection',
                 username="lbnlemistest2023@gmail.com",
                 password='thermostat_test_2023'):   # 'correctiontest_2023'

        """
        Parameters
        ----------

        site - str
            The site is a level in the grouping hierarchy (usually a defined
            physical space). The site is lbnlselfcorrection
        username - str 
            A dummy username we created to access the webapi
        password - str
            the password for lbnlemistest2023@gmail.com is correctiontest_2023

        the generic api structure is shown below

        https://demo.officeclimatecontrol.net/api.cgi?
        username=myname@gmail.com&password=mypassword
        &request=get
        &object=Thermostat
        &selection=name:TestThermostat;
        &value=heatSetting;coolSetting;temperature;
        """

        self.username = username
        self.password = password
        self.site = site

    # =========================================================================
    # --- URL Utilities
    # =========================================================================

    def _join_items(self, items, joiner=';'):
        """ Join iterable with 'joiner' delimiter. This is useful for combining
        all of the 'selection' and 'variable' items in the URL construction

        Parameters
        ----------
        items - str | list | dictionary
            if type(str) return input value -- nothing to join
            if type(list) join list items with "joiner" variable
            if type(dict) join each key:value pair to "joiner" with colon

        Returns
        -------
        joined - str

        """

        if items is None:
            return None

        if isinstance(items, str):
            return items

        elif isinstance(items, list):
            return joiner.join(items)

        elif isinstance(items, dict):
            return joiner.join([f'{key}:{val}'
                                for key, val in items.items()])
        else:
            raise ValueError("_join_items: items must be a str, list, dict")

    def _make_login(self, site=None, username=None, password=None):
        """ Combine the site (variable) with the username and password
        (probably not variable) to create the base url for the API calls"""

        if site is None:
            site = self.site
        if username is None:
            username = self.username
        if password is None:
            password = self.password

        return ('https://{}.officeclimatecontrol.net/api.cgi?'
                'username={}&password={}').format(site, username, password)

    def _make_URL(self,
                  object_type,
                  request,
                  login=None,
                  selection=None,
                  variables=None,
                  _verbose=False):
        """ Stiches together the login from _make_login with the object type,
        selection, vars and request type


        Parameters
        ----------
        object_type : str
            'Thermostat' or 'ThermostatHistory' or 'ThermostatSchedule' or 'Site'
        request : str
            'set' - Pelican API parameter to make tstat changes
            'get' - Pelican API parameter to retrieve data
            'drEvent' - Demand Response Event
        login : str
            The login information provided by the _make_login function
        selection : dictionary
            
            key:value pairs are joined into the appropriate str representation
            according to the documentation below
            dates - ISO 8601 Formatted Date Time [2018-08-06T15:53]
            
        From the API Doc:
            
        This is a set of attribute/value pairs which should be used as a query 
        match for the “set” or “get” request.  In a “get” request, items 
        matching the selection will be returned in the XML reply.  In a “set” 
        request, items matching the selection will be modified in the Pelican 
        system.  Pairs are separated by semicolons (;) and attributes are 
        separated from their values by colons (:) (See example below). 
        Attribute names are not case sensitive.  Attribute values are case
        sensitive.    
            
            
        variables : str or list of str
            if str, it is transformed into a list and joined according to the
            documentation copied below
        
        From the API Doc:
             
        For “get” requests, this is a semicolon (;) separated list of 
        attributes which are being requested.  For a “set” request, this is a
        semicolon (;) separated list of attribute/value pairs to be modified. 
        The attribute names are separated from their values by a colon (:). 
        Semicolons and colons are invalid characters for either the attribute 
        orthe value. Attribute names are not case sensitive.  Attribute values
        are case sensitive.
        
        When using HTTP GET, the 6 required elements must be
        formatted using standard http notation with the element name as shown 
        above followed by an equal sign and then that elements value.  The
        first element is preceded by a question mark (?) and the elements are 
        separated by the ampersand (&) character.  Standard HTTPcharacter 
        escaping is supported.  When using HTTP POST, standard encoding is 
        supported.
        
        _verrbose : bool
            True - print the URL that is made each time
            False - do not print
        
        Returns
        -------
        url : str
            The HTTP request ready url
        
        URL example:

        https://<demo>.officeclimatecontrol.net/api.cgi?
        username=myname@gmail.com&password=mypassword
        &request=get
        &object=Thermostat
        &selection=name:TestThermostat;
        &value=heatSetting;coolSetting;temperature;
        
        """

        if login is None:
            login = self._make_login()

        selection = self._join_items(selection)
        variables = self._join_items(variables)

        URLbase = (f'{login}&request={request}&object={object_type}')

        # allows for the selection or variables or both to be blank
        if selection:
            if variables:  # Selection and variables
                URLend = f'&selection={selection}&value={variables}'
            else:  # Selection
                URLend = f'&selection={selection}'
        else:
            if variables:  # variables
                URLend = f'&value={variables}'
            else:  # None
                URLend = ''
                
        if _verbose:
            print(URLbase + URLend)

        return URLbase + URLend

    def get_data(self, selection={'name':'Yimin1'},
                 variables=['name', 'serialNo', 'system', 'temperature', 'heatSetting',
                            'coolSetting', 'fan', 'status', 'humidity', 'humidifySetting',
                            'dehumidifySetting', 'co2Setting',
                            'outsideVentilation', 'minHeatSetting', 'maxHeatSetting',
                            'minCoolSetting', 'maxCoolSetting', 'groupName', 'timestamp'],
                            start='2023-05-28',end='2023-05-29',
                            object_type='Thermostat'):
        

        """ Pull data from API for Thermostat or ThermostatHistory objects.
        
        Parameters
        ----------
        selection : dict
            See selection explanation in _make_URL
        variables : str or list of str
            See variables explanation in _make_URL
            passed in variable descriptions:
            https://www.pelicanwireless.com/help-center/thermostat-attributes/
        object_type : str
            Thermostat or ThermostatHistory
        start : str
            "YYYY-MM-DD" formatted date for the start of the ThermostatHistory
        end : str
            "YYYY-MM-DD" formatted date for the end of the ThermostatHistory
            
        Returns
        -------
        XML parsed into ordered dict
        
        """

        # Some pre-building work for the seletion field
        if object_type == 'ThermostatHistory':
            selection['startDateTime'] = start
            selection['endDateTime'] = end
            url = self._make_URL(object_type=object_type,
                             request='get',
                             selection=selection,
                             variables=variables)
            # Send URL
            resp = requests.get(url)
            out = xmltodict.parse(resp.content)
            # print(out['result']['Thermostat']['heatSetting'])

            # Parse response XML into ordered dictionaries
            try:
                return xmltodict.parse(resp.content)
            except xmltodict.expat.ExpatError:
                return resp
            

        # construct the url
        url = self._make_URL(object_type=object_type,
                             request='get',
                             selection=selection,
                             variables=variables)
        # Send URL
        resp = requests.get(url)
        out = xmltodict.parse(resp.content)
        print("here you are: ", object_type)
        out = out['result']['Thermostat']

        now = datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        name = out['name']
        serialNo = out['serialNo']
        groupName = out['groupName']
        print('SerialNo: ', serialNo)
        print('groupName: ', groupName)
        system = out['system']
        temperature = out['temperature']
        heatSetting = out['heatSetting']
        coolSetting = out['coolSetting']
        fan = out['fan']
        status = out['status']
        humidity = out['humidity']
        humidifySetting = out['humidifySetting']
        dehumidifySetting = out['dehumidifySetting']
        co2Setting = out['co2Setting']
        outsideVentilation = out['outsideVentilation']
        minHeatSetting = out['minHeatSetting']
        maxHeatSetting = out['maxHeatSetting']
        minCoolSetting = out['minCoolSetting']
        maxCoolSetting = out['maxCoolSetting']
        row = [time, name, serialNo, system,
              temperature, heatSetting, coolSetting,
              fan, status, humidity, humidifySetting,
              dehumidifySetting, co2Setting, outsideVentilation,
              minHeatSetting, maxHeatSetting, minCoolSetting,
              maxCoolSetting]
        self.append_data_to_csv(row)


    def set_heat_temp(self, heat_temp,
                   object_type='Thermostat',
                   selection={'name':'Yimin1'},
                   start='2023-05-28', end='2023-05-29'):

        """ Send an API 'set' request to set the key:value pairs provided
        to variables as a dict. 
        
        Parameters
        ----------
        heat_temp: int
            temperature to be set to
        object_type : str
            Thermostat, ThermostatSchedule (not used, since we cant get data)
        selection : dict
            See selection explanation in _make_URL
            
        Returns
        -------
        resp : HTTP response
        
        """
        variables = {'heatSetting': heat_temp}
        
        url = self._make_URL(object_type=object_type,
                             request='set',
                             selection=selection,
                             variables=variables)

        resp = requests.get(url)

        return resp


    def set_cool_temp(self, cool_temp,
                   object_type='Thermostat',
                   selection={'name':'Yimin1'}):

        """ Send an API 'set' request to set the key:value pairs provided
        to variables as a dict. 
        
        Parameters
        ----------
        cool_temp: int
            temperature to be set to
        object_type : str
            Thermostat, ThermostatSchedule (not used, since we cant get data)
        selection : dict
            See selection explanation in _make_URL
            
        Returns
        -------
        resp : HTTP response
        
        """
        variables = {'coolSetting': cool_temp}
        
        url = self._make_URL(object_type=object_type,
                             request='set',
                             selection=selection,
                             variables=variables)

        resp = requests.get(url)

        return resp

    def set_mode(self, mode, object_type='Thermostat',
                  selection={'name':'Yimin1'}):

        """ Send an API 'set' request to set the key:value pairs provided
        to variables as a dict. 
        
        Parameters
        ----------
        mode: string
            Off, Auto, Heat, Cool
            Auto mode activates heating and cooling setpoints
        object_type : str
            Thermostat, ThermostatSchedule (not used, since we cant get data)
        selection : dict
            See selection explanation in _make_URL
            
        Returns
        -------
        resp : HTTP response
        
        """
        variables = {'system': mode}
        
        url = self._make_URL(object_type=object_type,
                             request='set',
                             selection=selection,
                             variables=variables)

        resp = requests.get(url)

        return resp

    def parse_historical_data(self, data_dict, filename='historical_data_2023-06-07.csv'):
        """ returns the thermostat attributes from a certain date in the past 

        data_dict - dict
            The massive OrderedDict structure that is generated from the
            return xml API response
        filename - string
            Name of the file to log the historic data on

        Returns
        -------
        thermostat attributes from a certain date in the past logged into a csv file

        """

        try:
            stat_list = data_dict['result']['ThermostatHistory']
        except KeyError:
            raise KeyError(f'{data_dict["result"]["message"]}')

        # Work around for a single result. The parsing expects a list
        # so turn this single object in to a list of 1
        if not isinstance(stat_list, list):
            stat_list = [stat_list]
        
        name1 = stat_list[0]['name']
        serial = stat_list[0]['serialNo']
        group = stat_list[0]['groupName']


        # print(stat_list)
        try:
             # Grab history data OrderedDict
            history = stat_list[0]['History']

        except KeyError:
            print(f'{name} contains no "History"')
            history = None
        
        if history is not None:
             print(f'Parsing {name1}')

        try:  # Sometimes the first element is none ¯\_(ツ)_/¯
              history.remove(None)
        except ValueError:
              pass

        for item in history:
              time = item['timestamp'][0:4]
              time += '-' + item['timestamp'][5:7]
              time += '-' + item['timestamp'][8:10]
              time += ' ' + item['timestamp'][11:13]
              time += ':' + item['timestamp'][14:17]
              time += ':00'
              name = name1
              serialNo = serial
              groupName = group
              system = item['system']
              temperature = item['temperature']
              heatSetting = item['heatSetting']
              coolSetting = item['coolSetting']
              fan = item['fan']
              status = item['status']
              humidity = item['humidity']
              humidifySetting = item['humidifySetting']
              dehumidifySetting = item['dehumidifySetting']
              co2Setting = item['co2Setting']
              outsideVentilation = item['outsideVentilation']
              # minHeatSetting is not readable from historic data
              # maxHeatSetting is not readable from historic data
              # minCoolSetting is not readable from historic data
              # maxCoolSetting is not readable from historic data
              try:
                    minHeatSetting = item['minHeatSetting']
                    maxHeatSetting = item['maxHeatSetting']
                    minCoolSetting = item['minCoolSetting']
                    maxCoolSetting = item['maxCoolSetting']
              except:
                    minHeatSetting = None
                    maxHeatSetting = None
                    minCoolSetting = None
                    maxCoolSetting = None
              row = [time, name, serialNo, system,
              temperature, heatSetting, coolSetting,
              fan, status, humidity, humidifySetting,
              dehumidifySetting, co2Setting, outsideVentilation,
              minHeatSetting, maxHeatSetting, minCoolSetting,
              maxCoolSetting]
              self.append_data_to_csv(row, filename)
                    
                

    
    def create_csv(self, columns, filename='more_dev_pelican.csv'):
        """
        create a CSV file and specify the column names

        Parameters
        ----------
        filename - string
                name of the file to be created appended by .csv
        columns - list of strings
                name of the columns of the csv


        Returns
        -------
        none

        """
        
        df = pd.DataFrame(columns=columns)
        df.to_csv(filename, index=False)


    def append_data_to_csv(self, data, filename='more_dev_pelican.csv'):
        """
        append data to the CSV file

        Parameters
        ----------
        filename - string
                name of the file
        data - list of strings
                values of the columns


        Returns
        -------
        none

        """
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(data)

    def set_schedule(self, day, start_time, duration, mode,
                   heat_temp, cool_temp, tName, delete, object_type,
                   keypad='Off', fan_mode='Off', scheduleRepeat='Weekly'):

        """ Sets a schedule on the specific day and time repeatedly.
        or deletes the specified schedule
        
        Parameters
        ----------
        day: string
            can only be seven days of the week
        start_time: string
            the time of day (24 hr format) for this schedule entry
        duration: integer
            the indexed set time for this schedule entry
            this is the schedule number
            you should increase it by one of you want to add
            more schedules at the same day
        mode: string
            the system mode, can only be Auto, Heat, Cool, Off
        heat_temp: int
            heating temperature to be set to
        cool_temp: int
            cooling temperature to be set to
        tName: string
            name of the thermostat
        delete: boolean
            if True, it will delete the specified schedule
        object_type: string
            Thermostat object such as SharedSchedule, ThermostatSchedule
            In ThermostatSchedule, Weekly is the default.
        keypad: string
            the keypad setting
        fan_mode: string
            the fan setting, can only be On, Off
        scheduleRepeat: string
            can only be Daily, Weekday, Weekly
            
        
            
            
        Returns
        -------
        resp : HTTP response
        
        """
        
        selection={'name': tName, 'dayOfWeek': day, 'setTime': duration}
        # selection={'name': tName,  'setTime': duration}
        if delete == True:
            if object_type == 'ThermostatSchedule':
                variables = {'startTime': start_time,
                     'system': mode, 'heatSetting': heat_temp, 'coolSetting': cool_temp,
                     'fan': fan_mode, 'keypad': keypad, 'delete': None}
            
            elif object_type == 'SharedSchedule':
                variables = {'startTime': start_time,
                     'system': mode, 'heatSetting': heat_temp, 'coolSetting': cool_temp,
                     'fan': fan_mode, 'keypad': keypad, 'scheduleRepeat': scheduleRepeat,
                     'delete': None}
        else:
            variables = {'startTime': start_time,
                     'system': mode, 'heatSetting': heat_temp, 'coolSetting': cool_temp,
                     'fan': fan_mode, 'keypad': keypad, 'scheduleRepeat': scheduleRepeat}
        
        url = self._make_URL(object_type=object_type,
                             request='set',
                             selection=selection,
                             variables=variables)
        

        resp = requests.get(url)
        out = xmltodict.parse(resp.content)
        return out
            
column_names = ['Date', 'Name', 'Serial_Number', 'Thermostat_Mode', 'Current_Temperatue',
                'Heating_Setpoint', 'Cooling_Setpoint', 'Fan_Mode', 'Thermostat_Status',
                'Current_Humidity', 'Min_humidity', 'Max_Humidity', 'Demand_Ventilation',
                'Outside_Ventilation', 'Lowest_Allowable_Heating_SetPoint',
                'Highest_Allowable_Heating_SetPoint', 'Lowest_Allowable_Cooling_SetPoint',
                'Highest_Allowable_Cooling_SetPoint']



if __name__ == "__main__":
    
    pc = pelican()
    
    # %% load data
    
    # Initiate data file path and name
    data_file_path = 'C:\\Users\\yimin\Dropbox\\Project_smart thermostat'
    data_file_name = 'Yimin1_hist_data_2023-06-15_2023-07-12.csv'
    
    fl_read = os.path.join(data_file_path, data_file_name)
    
    df_data_in = pd.read_csv(fl_read)
    
    
    pref_occ_start_time_weekday = '06:00:00'
    pref_occ_start_time_weekday_min = pref_occ_start_time_weekday.rsplit(':', 1)[0]
    pref_occ_end_time_weekday = '18:00:00'
    pref_occ_end_time_weekday_min = pref_occ_end_time_weekday.rsplit(':', 1)[0]
    
    
    occupied_start_time_flag, occupied_end_time_flag = schedule_detection(df_data_in, pref_occ_start_time_weekday, pref_occ_end_time_weekday)
    
    
    if occupied_start_time_flag == 1: 
    
        # Set occupied start time
        pc.set_schedule(day = 'Monday', start_time = pref_occ_start_time_weekday_min, duration = 1, mode='Auto',
                        heat_temp = 68, cool_temp = 74, tName='B90TstatnoHP', delete=False,
                        object_type='ThermostatSchedule', scheduleRepeat='Weekly')
    
    print ('The new occupied start time has been set!')
    
    if occupied_end_time_flag == 1: 
    
        # Set occupied end time
        pc.set_schedule(day = 'Monday', start_time = pref_occ_end_time_weekday_min, duration = 2, mode='Auto',
                        heat_temp = 62, cool_temp = 85, tName='B90TstatnoHP', delete=False,
                        object_type='ThermostatSchedule', scheduleRepeat='Weekly')
        
        print ('The new occupied end time has been set!')
  
    
