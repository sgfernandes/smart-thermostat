# -*- coding: utf-8 -*-
"""
Created on Thu May 25 12:21:04 2023

# Name: pelican_openapi_io_driver.py

@author: Yimin Chen
# Contact: YiminChen@lbl.gov

# Copyright Reserved 
# @ Lawrence Berkeley National Laboratory 
# Funded by the U.S. Department of Energy


# Pelican gateway OpenAPI driver
# Acknowledgement: UC Davis team. 

"""
import os
import pandas as pd
import requests
import xmltodict
import collections
import sys
import uuid
import time
import csv
from datetime import datetime

# FDD and correction functions
from func_pelican_learn_schedule import pelican_learn_schedule # Sechedule setting FDD algorithm
from func_setpoint_setting_fdd_correction import setpoint_setting_fdd_correction


class pelican():
    """
    The pelican client is a collection of functions that allows the user to
    access the PelicanWireless thermostats via the PelicanWireless web api.
    The API allows for the reading and writing of thermostat setttings.
    """
    def __init__(self,
                 site='INPUT THE SITE NAME',
                 username="INPUT THE USER NAME",
                 password='INPUT THE PASSWORD'

                 # site='lbl-building-90c',
                 # username=" dr.pelican.xbos@gmail.com",
                 # password='xbospelican'                 
                 
                 ):

        self.username = username
        self.password = password
        self.site = site

    # =========================================================================
    # --- URL Utilities
    # =========================================================================

    def _join_items(self, items, joiner=';'):

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

    # Define the thermostat name
    # Need to input thermorstat name. 
    
    def get_data(self, selection={'name':'Name of the thermostats'},                       
                 variables=['name', 'serialNo', 'system','temperature', 'heatSetting',
                            'coolSetting', 'fan', 'status', 'humidity', 'humidifySetting',
                            'dehumidifySetting', 'co2Setting',
                            'outsideVentilation', 'minHeatSetting', 'maxHeatSetting',
                            'minCoolSetting', 'maxCoolSetting'],
                 object_type= 'Thermostat',
                 ):
    # Get real time data    

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

        # construct the url
        url = self._make_URL(object_type=object_type,
                             request='get',
                             selection=selection,
                             variables=variables)
        # Send URL
        resp = requests.get(url)
        collection_data_out = xmltodict.parse(resp.content)     
        
        data_out = collection_data_out['result']['Thermostat']
     
        
        now = datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        name = data_out['name']
        serialNo = data_out['serialNo']
        system = data_out['system']
        temperature = data_out['temperature']
        heatSetting = data_out['heatSetting']
        coolSetting = data_out['coolSetting']
        fan = data_out['fan']
        status = data_out['status']
        humidity = data_out['humidity']
        humidifySetting = data_out['humidifySetting']
        dehumidifySetting = data_out['dehumidifySetting']
        co2Setting = data_out['co2Setting']
        outsideVentilation = data_out['outsideVentilation']
        minHeatSetting = data_out['minHeatSetting']
        maxHeatSetting = data_out['maxHeatSetting']
        minCoolSetting = data_out['minCoolSetting']
        maxCoolSetting = data_out['maxCoolSetting']
        row = [time, name, serialNo, system,
              temperature, heatSetting, coolSetting,
              fan, status, humidity, humidifySetting,
              dehumidifySetting, co2Setting, outsideVentilation,
              minHeatSetting, maxHeatSetting, minCoolSetting,
              maxCoolSetting]
        self.append_data_to_csv(row)


        # Parse response XML into ordered dictionaries
        # try:
        #    return xmltodict.parse(resp.content)
        # except xmltodict.expat.ExpatError:
        #    return resp


    def get_historical_data (self, thermostat_name, start, end, variables, object_type  #'Thermostat',
                            ):
    
        """
        def get_historical_data(self, selection={'name': thermostat_name}, start, end ,                     
                     variables=['name', 'serialNo', 'system', 'timestamp','temperature', 'heatSetting',
                                'coolSetting', 'fan', 'status', 'humidity', 'humidifySetting',
                                'dehumidifySetting', 'co2Setting',
                                'outsideVentilation', 'minHeatSetting', 'maxHeatSetting',
                                'minCoolSetting', 'maxCoolSetting'],
                     object_type= 'ThermostatHistory', #'Thermostat',
                     ):
        """
    
    # Get historian data  
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
        selection={'name': thermostat_name}        
        print ('You are now querying the historical data from the thermostat ' + thermostat_name)

        # Some pre-building work for the seletion field
        if object_type == 'ThermostatHistory':
            selection['startDateTime'] = start
            selection['endDateTime'] = end

        # construct the url
        url = self._make_URL(object_type=object_type,
                             request='get',
                             selection=selection,
                             variables=variables)
        # Send URL
        resp = requests.get(url)
        dic_collection_data_out = xmltodict.parse(resp.content)
       
        print ('Historical data query from the thermostat completed!')
        
        # Parse historian data
        # data_out = collection_data_out['result']['ThermostatHistory']
        # df_data_out = pd.DataFrame(data_out, columns=data_out.keys())
        
        df_data_his = self.parse_historical_data(dic_collection_data_out)  # Return dataframe
        
        return df_data_his 
        
        # Parse response XML into ordered dictionaries
        # try:
        #    return xmltodict.parse(resp.content)
        # except xmltodict.expat.ExpatError:
        #    return resp 

    def parse_historical_data(self, data_dict, parse_slaves=False):
        """ Parses data_dict for "ThermostatHistory" API request

        data_dict - dict
            The massive OrderedDict structure that is generated from the
            return xml API response
        parse_slaves - bool
            True - Parse the slave data and insert it into the main df
            False - ignore the slave data column

        Returns
        -------
        his_collector - dict
            A dictionary of group keys and other metadata but also attached
            is his_collector['Name']['data'] = pd.DataFrame

        API response structure

        result
        |- success
            |- <binary result>
        |- message
            |- <string>
        L- ThermostatHistory
            |- stat1
            |- stat2
            :   |- vars
                |- History  # May be empty is no History data
                    |- pt1  # inidivudal timepoints
                    |- pt2
                    :   |- vars
                        |- slave (if present)
                            |- slave1
                            |- slave2
                            :   |- Name
                                |- type
                                |- value
                                |- weight
                                :
        """
        try:
            stat_list = data_dict['result']['ThermostatHistory']
        except KeyError:
            raise KeyError(f'{data_dict["result"]["message"]}')

        # Work around for a single result. The parsing expects a list
        # so turn this single object in to a list of 1
        if not isinstance(stat_list, list):
            stat_list = [stat_list]

        his_collector = {}

        for stat in stat_list:

            name = stat['name']
            # group = stat['groupName']
            serial = stat['serialNo']

            # Collects data from single t-stat and converts to DF
            df_dict = {}

            try:
                # Grab history data OrderedDict
                history = stat['History']

            except KeyError:
                print(f'{name} contains no "History"')
                history = None
                continue

            if history is not None:
                print(f'Parsing {name}')

                try:  # Sometimes the first element is none ¯\_(ツ)_/¯
                    history.remove(None)
                except ValueError:
                    pass

                history_dict = {x['timestamp']: x for x in history}

                df = pd.DataFrame.from_dict(history_dict, orient='index')
                
                df = df.set_index(['timestamp'])
                # df = df.drop('timestamp', axis=1)
  
            else:  # if history is None
                print(f'{name} History object is None')
                continue

            if parse_slaves:
                if 'slaves' not in df.columns:
                    pass

                else:  # if ignore_slaves == False and slaves in df.columns
                    print(f'Parsing slave df["slave"] of {name}')

                    slaveDf = self.parse_slaves(df['slaves'].to_dict(), name)

                    df = df.drop('slaves', axis=1)

                    # Concat slave info to parent df
                    df = pd.concat([df, slaveDf], ignore_index=False, axis=1,
                                   sort=False)

            else:  # if parse_slaves == False
                if 'slaves' in df.columns:
                    df = df.drop('slaves', axis=1)

            df.index = pd.to_datetime(df.index, infer_datetime_format = True)
            # df['co2Level'] = pd.to_numeric(df['co2Level'], errors='coerce')
            # Attach metadata to history data
            df_dict['Name'] = name
            df_dict['serial'] = serial
            # df_dict['group'] = group  # We don't have group definition in the Pelican self-correction
            df_dict['data'] = df

            # store all Tstat history dictionary in single collector
            his_collector[name] = df_dict
            
            df_his_data_out = df.copy()
            
            print ('Historical data parse is completed')
        return df_his_data_out


    def set_heat_temp_setpoint(self, heat_temp,
                      object_type,
                      thermostat_name,
                      ):

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
        selection = {'name': thermostat_name}
        variables = {'heatSetting': heat_temp}
        
        url = self._make_URL(object_type=object_type,
                             request='set',
                             selection=selection,
                             variables=variables)

        resp = requests.get(url)

        return resp
 
    
    def set_cool_temp_setpoint(self, cool_temp,
                      object_type,
                      thermostat_name
                      ):

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
        selection = {'name': thermostat_name}
        variables = {'coolSetting': cool_temp}
        
        url = self._make_URL(object_type=object_type,
                             request='set',
                             selection=selection,
                             variables=variables)

        resp = requests.get(url)

        return resp
    

    def set_mode(self, mode, object_type='Thermostat',
                  thermostat_name):

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
        selection = {'name': thermostat_name}
        variables = {'system': mode}
        
        url = self._make_URL(object_type=object_type,
                             request='set',
                             selection=selection,
                             variables=variables)

        resp = requests.get(url)

        return resp
    
    def create_csv(self, columns, filename='pelican_reads_test.csv'):
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


    def append_data_to_csv(self, data, filename='pelican_reads_test.csv'):
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


if __name__ == "__main__":
    
    pc = pelican()
    
    # %% Parameter settings (Some settings are in the function scripts)
    # ---------------------------------------------------------------------------------------------
    pref_occ_start_time_weekday = '06:00:00'
    pref_occ_end_time_weekday = '17:59:00'
    
    pref_unocc_start_time_weekday = '18:00:00'
    pref_unocc_end_time_weekday = '05:59:00'
    
    pref_occ_clg_spt = 74  
    pref_occ_htg_spt = 68
    
    pref_unocc_clg_spt = 85  
    pref_unocc_htg_spt = 62
    
    # =============================================================================================
    
    # Set: thermostat name
    object_type = 'ThermostatHistory'
    thermostat_name = 'INPUT THERMOSTAT NAME 1'
    
    # Input the historian data scope
    # Date format: yyyy-mm-dd
    his_query_start_date = '2023-07-01'
    his_query_end_date = '2023-07-20'
    
    # %% Get historical data from the cloud-based server   
    # ----------------------------------------------------------------------------------------------
    data_point_names =['name', 'serialNo', 'system', 'timestamp','temperature', 'heatSetting',
                    'coolSetting', 'fan', 'status', 'humidity', 'humidifySetting',
                    'dehumidifySetting', 'co2Setting', 'outsideVentilation', 'minHeatSetting', 
                    'maxHeatSetting', 'minCoolSetting', 'maxCoolSetting', 'schedule', 
                    'scheduleRepeat', 'runStatus']

    df_his_data = pc.get_historical_data(thermostat_name, his_query_start_date, his_query_end_date, data_point_names, object_type)
    
    # =============================================================================================
    
    df_data_in = df_his_data.copy()
    
    # %% Setpoint setting FDD and correction
    # ----------------------------------------------------------------------------------------------
    setpoint_setting_fdd_correction(df_data_in)    
    
    # ==============================================================================================

    # %% Schedule setting FDD and correction 
    # ----------------------------------------------------------------------------------------------
    occupied_start_time_flag, occupied_end_time_flag = pelican_learn_schedule(df_data_in, pref_occ_start_time_weekday, pref_occ_end_time_weekday,\
                                                                              pref_unocc_clg_spt, pref_unocc_htg_spt, pref_occ_clg_spt, pref_occ_htg_spt)

    # %% Schedule correction
    # Attention: the thermostata mode should be set to 'Auto' to enable the change of the schedule
    # We use Friday as an instance
    if occupied_start_time_flag == 1:
        pc.set_schedule(day='Friday', start_time = pref_occ_start_time_weekday, duration=2, mode='Auto',
                        heat_temp = pref_occ_htg_spt, cool_temp = pref_occ_clg_spt, thermostat_name, delete=False,
                        object_type='ThermostatSchedule', scheduleRepeat='Weekly')

    
    if occupied_end_time_flag == 1:
        pc.set_schedule(day='Friday', start_time = pref_unocc_start_time_weekday, duration=2, mode='Auto',
                heat_temp = pref_unocc_htg_spt, cool_temp = pref_unocc_clg_spt, thermostat_name, delete=False,
                object_type='ThermostatSchedule', scheduleRepeat='Weekly')
    # ==============================================================================================
    