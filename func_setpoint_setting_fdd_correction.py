# -*- coding: utf-8 -*-
"""
Created on Thu May 04 09：15:23 2023
# Name: func_setpoint_setting_fdd_correction.py
# Version: 1.0

@author: Yimin Chen
# contact: YiminChen@lbl.gov

# Copyright Reserved 
# @ Lawrence Berkeley National Laboratory 
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


# Variable declaration:
# Input varialbes from the thermostat: 
        
         cooling_spt (Cooling setpoint): Entry: Values got from the main function
         
         heating_spt (Heating setpoint): Entry: Values got the main function

# Input varialbes for user input parameters
         
         setpoint_fdd_deviation_thresh (setpoint fdd deviation threshold): 
             Entry: 1 (1 degF), User input
         
         deadband_fdd_deviation_thresh (deadband fdd deviation threshold):
             Entry: 0.5 (0.5 degF), User input
         
         fdd_setpoint_setting_flag_duration (Detection mode): Entry: User selects the detection mode. Currently there are two entries:
             Entry 1: 48 (48-hours baseline for detection)
             Entry 2: 4  (4-hour baseline for detection)
             
         fault_correction_enable (fault correction enabler): Entry: user selects.
             Entry 1: ON  (enabled)
             Entry 2: OFF (disabled)
         
         fault_correction_way (fault correcton mode):  Entry: user selects.
             Entry 1: Manual (manual mode)
             Entry 2: Auto (auto mode)
             Entry 3: Schedule (scheudle mode)
             
         fault_correction_scheduled_time (fault correction action scheduled time): Entry: user selects
             Entry 1: 6:00 (correction takes action at 6:00)
             Entry 2: 8:00 (correction takes action at 6:00)
             Entry 3: 15:00 (correction takes action at 15:00)


# Output: detection_flag (detection flag): 
             Output 1: occ_clg_spt_low_flag (cooling setpoint too low fault) 
             Output 2: occ_htg_spt_high_flag (heating setpoint too high fault)
             Output 3: occ_deadband_narrow_flag (narrow deadband ault)
 
# Implementation requirements:
1) Make sure that all packages needed are placed in the same folder
2) Make sure the io driver can query data from the database

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
import datetime
import calendar
from datetime import date, timedelta
import pandas as pd
from datetime import datetime

from pelican_openapi_io_driver import pelican


data_point_names =['name', 'serialNo', 'system', 'timestamp','temperature', 'heatSetting',
                    'coolSetting', 'fan', 'status', 'humidity', 'humidifySetting',
                    'dehumidifySetting', 'co2Setting', 'outsideVentilation', 'minHeatSetting', 
                    'maxHeatSetting', 'minCoolSetting', 'maxCoolSetting', 'schedule', 
                    'scheduleRepeat', 'runStatus']

def func_prev_weekday(adate):
    # Ref: https://stackoverflow.com/questions/12053633/previous-weekday-in-python
    adate -= timedelta(days=1)
    while adate.weekday() > 4: # Mon-Fri are 0-4
        adate -= timedelta(days=1)
    return adate            


def setpoint_setting_fdd_correction(df_data_in):
    # %% Pelican thermostat setting
    object_type = 'Thermostat'
    thermostat_name = {'name': 'TstatnoHP'}
    
    
    # %% FDD parameters setting (threshold setting)
    # -----------------------------------------------------------------------------
        
    fault_correction_enable = 'ON'
    fault_correction_way = 'User'
    fault_correction_scheduled_time = ['6:00', '8:00', '15:00']
    
    # ============================================================================= 
    
    # Detection threshold settings
    # -----------------------------------------------------------------------------
    # detection historical data time scope
        # FDD setpoint setting flag duration settings: 
        # 48: using average setpoint values during occupied hours in two days (i.e., 48 hours during)
        # 2: using average setpoint values during occupied hours in two hours (i.e., 2 hours during)
    fdd_setpoint_setting_flag_duration = 48   
    
    # Thresholds that are provided by customers
    setpoint_fdd_deviation_thresh = 0.5    # Setpoint deviation threshold: 1 degF
    deadband_fdd_deviation_thresh = 0.5    # Deadband deviation threshold: 1 degF
    
    # =============================================================================
    
    
    # %% Preferred correction settings
    # -----------------------------------------------------------------------------
    # Set: preferred cooling setpoint, heating setpoint, dead band, schedule start time, schedule end time
    pref_occ_start_time_weekday = '6:00'
    pref_occ_end_time_weekday = '17:59'
    
    pref_occ_clg_spt = 74  
    pref_occ_htg_spt = 68
    
    pref_unocc_clg_spt = 85  
    pref_unocc_htg_spt = 62
    
    pref_occ_setpoint_deadband = 6
    
    # ============================================================================
    
    
    # %% Get current timestamp
    current_time = datetime.now() 
    current_year = '{:02d}'.format(current_time.year)
    current_month = '{:02d}'.format(current_time.month)
    current_day = '{:02d}'.format(current_time.day)
    # current_day = '22'
    
    current_hour_min = current_time.strftime('%H:%M')
    
    current_date = current_year + '-' + current_month + '-' + current_day
    
    current_weekday = current_time.isoweekday()  # Monday is 1; Sunday is 7
    current_calendar_day = calendar.day_name[current_time.weekday()]
    
    print ('Today is ' + current_date + ', ' + current_calendar_day)
    
    
    # %% Convert data format
    df_his_data_time_handled_new = df_data_in.copy()
    
    df_his_data_time_handled_new['coolSetting'] = pd.to_numeric(df_his_data_time_handled_new['coolSetting'], errors='coerce')
    df_his_data_time_handled_new['heatSetting'] = pd.to_numeric(df_his_data_time_handled_new['heatSetting'], errors='coerce')
    df_his_data_time_handled_new['temperature'] = pd.to_numeric(df_his_data_time_handled_new['temperature'], errors='coerce')
    
    
    # %% FAULT DETECTION PROCESS
    # ------------------------------------------------------------------------------------------------------------
    
    # %% FDD algorithm description
    '''
    FDD approach: using the historical data (i.e., cooling setpoint, heating setpoint, and runstatus)
    # Algorithm reference: 
    
    Step 1: 
        Get the occupied time period through the weekdays 
    Step 2:
        Extract cooling operation and heating operation
    Stpe 3:
        Calculate the mean cooling setpoint and heating setpoint through the extract cooling operation and heating operation
    Step 4:
        Compare the incoming mean setpoints with the preferred ones, get the deviations.
    Step 5:
        Flag the faults if the deviation is higher than the threshold
     
    '''
    
    # %% BASELINE DATA GENERATEION
    # -----------------------------------------------------------------------------------------------------------
    
    # %% Determine how long historical data should be used
    if fdd_setpoint_setting_flag_duration == 48:
        no_his_data_days = 2
        fdd_setpoint_setting_algo_selection_flag = "two_day_average"
        
        prev_weekday = func_prev_weekday(date(int(current_year), int(current_month), int(current_day))) 
        prev_two_weekday = func_prev_weekday(prev_weekday)
        
            
    if fdd_setpoint_setting_flag_duration == 2:
        no_his_data_days = 0
        fdd_setpoint_setting_algo_selection_flag = "two_hour_average"
        
    
    # %% Extract operation in the occupied time 
    # Select data from the occupied hours 
    
    df_oper_data_his_occ = df_his_data_time_handled_new.between_time(pref_occ_start_time_weekday, 
                                                                     pref_occ_end_time_weekday)
    
    # Extract cooling operation
    df_oper_data_his_occ_clg = df_oper_data_his_occ.loc[(df_oper_data_his_occ['runStatus'] == 'Cool-Stage1') | (df_oper_data_his_occ['runStatus'] == 'Cool-Stage2')]
    
    # Extract heating operation
    df_oper_data_his_occ_htg = df_oper_data_his_occ.loc[(df_oper_data_his_occ['runStatus'] == 'Heat-Stage1') | (df_oper_data_his_occ['runStatus'] == 'Heat-Stage2')]
    
    # %% Calucation duration, average spt values
    # Calcuate the cooling/heating operation duration
    # clg_duration_his_data = len(df_oper_data_his_oper_clg)
    # htg_duration_his_data = len(df_oper_data_his_oper_htg)
    
    # %% Calcuate the average cooling setpoint and heating setpoint during cooling operation and heating operation, respectively 
    ave_clg_spt_clg_operation = round(df_oper_data_his_occ_clg.loc[:, 'coolSetting'].mean(), 2)   
    ave_htg_spt_htg_operation = round(df_oper_data_his_occ_htg.loc[:, 'heatSetting'].mean(), 2)
    
    # Calcuate the average cooling setpoint and heating setpoint during occupied operaiton period, respectively
    ave_clg_spt_occ_operation = round(df_oper_data_his_occ.loc[:, 'coolSetting'].mean(), 2)
    ave_htg_spt_occ_operation = round(df_oper_data_his_occ.loc[:, 'heatSetting'].mean(), 2)
    
    
    # %% Calculate the cooling setponit and the heating setpoint from incoming data 
    # Extract operation in the occupied time 
    
    # %% Calucation duration, average spt values
    # Calcuate the cooling/heating operation duration
    clg_duration_hist_data = len(df_oper_data_his_occ_clg)
    htg_duration_hist_data = len(df_oper_data_his_occ_htg)
    
    occ_duration_hist_data = len(df_oper_data_his_occ)
    
    # ===============================================================================================
    
    
    # %% FAULT DETECTION AND CORRECTION
    # Overcooling and overheating detection and correction
    # -----------------------------------------------------------------------------------------------
    
    '''
    Detection algorithm 1
    Algorithm description: 
        Collect system operation data from two days, and calculate the averaged setpoints 
        Compare the averaged setpoints with the prefered setpoint
    
    '''
    # TODO: package the method
    if ave_clg_spt_clg_operation < pref_occ_clg_spt - setpoint_fdd_deviation_thresh:
        print ('There is an overcooling fault. The averaged cooling setpoint in the occupied hours is lower than the prefered value')
        
        if fault_correction_enable == 'ON':
        # Correct the fault
            print ('Implement fault correction by overridding the cooling setpoint to a prefered value ' + str(pref_occ_clg_spt) + ' degF')
            if fault_correction_way == 'Immediate':
                pelican.set_cool_temp_setpoint(pref_occ_clg_spt, object_type, thermostat_name)
                print ('The fault is corrected')
            if fault_correction_way == 'Scheduled':
                if current_hour_min in fault_correction_scheduled_time:
                    print ('The fault will be corrected at the scheduled time')
                    pelican.set_cool_temp_setpoint(pref_occ_clg_spt, object_type, thermostat_name)
            if fault_correction_way == 'User':
                print ('Please correct the fault through the interface')
                y_or_n = input("\nDo you want to reset the cooling setpoint to the corrected value? (y/n) ")
                if y_or_n == 'y':
                    pelican.set_cool_temp_setpoint(pref_occ_clg_spt, object_type, thermostat_name)
                    print ('The fault is corrected')
                if y_or_n == 'n':
                    print ('No correction is taken, the fault may exist')
        
        if fault_correction_enable == 'OFF':
            print ('Fault self correction is not enabled, please enable the self correction')
    else:
        print ('There is no overcooling fault. The averaged cooling setpoint in the occupied hours equals or is higher than the prefered value')
        
        
    if ave_htg_spt_htg_operation > pref_occ_htg_spt + setpoint_fdd_deviation_thresh:
        print ('There is an overheating fault. The averaged heating setpoint in the occupied hours is lower than the prefered value')
        
        if fault_correction_enable == 'ON':
        # Correct the fault
            print ('Implement fault correction by overridding the heating setpoint to a prefered value ' + str(pref_occ_htg_spt) + ' degF')
            if fault_correction_way == 'Immediate':
                pelican.set_heat_temp_setpoint(pref_occ_htg_spt, object_type, thermostat_name)
                print ('The fault is corrected')
            if fault_correction_way == 'Scheduled':
                if current_hour_min in fault_correction_scheduled_time:
                    print ('The fault will be corrected at the scheduled time')
                    pelican.set_heat_temp_setpoint(pref_occ_htg_spt, object_type, thermostat_name)
            if fault_correction_way == 'User':
                print ('Please correct the fault through the interface')
                y_or_n = input("\nDo you want to reset the heating setpoint to the corrected value? (y/n) ")
                if y_or_n == 'y':
                    pelican.set_heat_temp_setpoint(pref_occ_htg_spt, object_type, thermostat_name)
                    print ('The fault is corrected')
                if y_or_n == 'n':
                    print ('No correction is taken, the fault may exist')
        
        if fault_correction_enable == 'OFF':
            print ('Fault self correction is not enabled, please enable the self correction')
    else:
        print ('There is no overheating fault. The averaged heating setpoint in the occupied hours equals or is lower than the prefered value')
    
    # ============================================================================
    
    # %% Narrow dead band fault detection and correction
    # -----------------------------------------------------------------------------------------------
    '''
    Detection algorithm 1
    Algorithm description: 
        Collect system operation data from two days, and calculate the averaged setpoints 
        Compare the averaged setpoints with the prefered setpoint
    
    '''
    
    # TODO: package the method
    # Calcuate the average cooling setpoint and heating setpoint during cooling operation and heating operation, respectively
    # In the operation period (including the occupancy period and unoccupancy period)
    
    ave_spt_dead_band_operation = ave_clg_spt_clg_operation - ave_htg_spt_htg_operation
    
    # In the occupancy period
    ave_spt_dead_band_occ_operation = ave_clg_spt_occ_operation - ave_htg_spt_occ_operation
    
    if ave_spt_dead_band_occ_operation < 0:
        print ('Cooling setpoint is lower than the heating setpoint, setpoint wrong setting')
    if ave_spt_dead_band_occ_operation < deadband_fdd_deviation_thresh:
        if fault_correction_way == 'Immediate':
            # Method 1: Set preferred cooling setpoint and heating setpoint
            pelican.set_heat_temp_setpoint(pref_occ_htg_spt, object_type, thermostat_name)
            pelican.set_cool_temp_setpoint(pref_occ_clg_spt, object_type, thermostat_name)
            print ('The fault is corrected')
            
            '''
            # Method 2: Set preferred heating setpoint, and the cooling setpoint will be calculated by 
            #           using the heating setpoint value plus preferred deadband
            pelican.set_heat_temp_setpoint(pref_occ_htg_spt, object_type, thermostat_name)
            calc_clg_spt = pref_occ_htg_spt + pref_occ_setpoint_deadband
            pelican.set_cool_temp_setpoint(calc_clg_spt, object_type, thermostat_name)
            print ('The fault is corrected')
            '''
        
        if fault_correction_way == 'Scheduled':
            if current_hour_min in fault_correction_scheduled_time:
                print ('The fault will be corrected at the scheduled time')
                pelican.set_heat_temp_setpoint(pref_occ_htg_spt, object_type, thermostat_name)
                pelican.set_cool_temp_setpoint(pref_occ_clg_spt, object_type, thermostat_name)
        
        
        if fault_correction_way == 'User':
            print ('Please correct the fault through the interface')
            y_or_n = input("\nDo you want to reset the cooling setpoint to the corrected value? (y/n) ")
            if y_or_n == 'y':
                pelican.set_heat_temp_setpoint(pref_occ_htg_spt, object_type, thermostat_name)
                pelican.set_cool_temp_setpoint(pref_occ_clg_spt, object_type, thermostat_name)
                print ('The fault is corrected')
            if y_or_n == 'n':
                print ('No correction is taken, the fault may exist')
        
    
'''
# For test
if __name__ == "__main__":
   
    # --------------------------------------------------------------------------------
    # %% load data
    # Initiate data file path and name
    data_file_path = 'INPUT DATA PATH'
    data_file_name = 'INPUT DATA FILE NAME'
    
    fl_read = os.path.join(data_file_path, data_file_name)
    
    df_data_in = pd.read_csv(fl_read)
    
'''








