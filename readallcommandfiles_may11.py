# -*- coding: utf-8 -*-
"""
Created on Thu May 11 14:44:29 2017

@author: TZhao
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 16:07:11 2017

@author: tzhao
"""

import pandas as pd, numpy as np
from bokeh.plotting import *
from bokeh.models import ColumnDataSource, HoverTool
import os
#import datetime
import os, time, datetime, pickle, json
#%%
# read in flow data, command data, pod select data 
dirName = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\WestEclipseParsedOutput01\\'
dirNameNew = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\WestEclipseParsedOutput02\\'
# this is for all subsea commands total 70 with MVC included
listOfCommands = os.listdir(dirName)
listOfCommands = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands if (not ('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))
listOfCommands2 = os.listdir(dirName)
listOfCommandstopside = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands2 if (('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))

#%%
def clean_data(tocleandata):
#    tocleandata=commderrorbluea
    tocleandata.reset_index(inplace=True, drop = True)  # reset the indexes from 1
    
    removeind=[]
    for ind in range(len(tocleandata)-1):
#        ind=0
        if (abs(tocleandata['DateFormatted'].iloc[ind]-tocleandata['DateFormatted'].iloc[ind+1]).total_seconds()<10.0 and \
              ( tocleandata['FromState'].iloc[ind]==tocleandata['ToState'].iloc[ind+1] or \
               (tocleandata['ToState'].iloc[ind+1] in tocleandata['FromState'].iloc[ind]) or \
              (tocleandata['FromState'].iloc[ind] in tocleandata['ToState'].iloc[ind+1])) ):
            removeind.append(ind)
            removeind.append(ind+1)
            if ind<=len(tocleandata)-3:
                ind=ind+2
        
    tocleandata=tocleandata.drop(removeind)  
    tocleandata.reset_index(inplace=True, drop = True)  # reset the indexes from 1
     
    removeind=[]
    for ind in range(len(tocleandata)-1):
#        ind=0
        if (abs(tocleandata['DateFormatted'].iloc[ind]-tocleandata['DateFormatted'].iloc[ind+1]).total_seconds()<10.0 and \
             ( tocleandata['ToState'].iloc[ind]==tocleandata['FromState'].iloc[ind+1] or  \
            ( ("ERROR" in tocleandata['ToState'].iloc[ind]) and ("ERROR" in tocleandata['FromState'].iloc[ind+1]) ) ) and \
              tocleandata['FromState'].iloc[ind]!=tocleandata['ToState'].iloc[ind+1] ):
            removeind.append(ind)
            tocleandata['FromState'].iloc[ind+1]= tocleandata['FromState'].iloc[ind]
            if ind<=len(tocleandata)-3:
                ind=ind+2
                                
    tocleandata=tocleandata.drop(removeind)  
#    tocleandata['Action']= tocleandata['FromState']+" to "+tocleandata['ToState']
    return tocleandata

def process_data(commData):
    commData['error1']=[1  if 'ERROR' in b  else  0   for b in commData['FromState'] ]
    commdataerror1=commData[commData['error1']==1 ]
    commData['error2']=[1  if 'ERROR' in b  else  0   for b in commData['ToState'] ] 
    commdataerror2=commData[commData['error2']==1 ]
        
    commdataerror=pd.concat([commdataerror1,commdataerror2])
    commdataerror.sort_values(by='DateTime', inplace=True, ascending=True)  # sort by time
    
    commData=commData.drop(commdataerror.index)  # drop those rows with errors
    
    commdataerror.reset_index(inplace=True, drop = True)  # reset the indexes from 1
    
    
    commderrorbluea= commdataerror[commdataerror['SEMName']=='Blue A']
    commderrorblueb= commdataerror[commdataerror['SEMName']=='Blue B']
    commderroryellowa= commdataerror[commdataerror['SEMName']=='Yellow A']
    commderroryellowb= commdataerror[commdataerror['SEMName']=='Yellow B']

    commdbluea = clean_data(commderrorbluea) 
    commdblueb = clean_data(commderrorblueb)
    commdyellowa = clean_data(commderroryellowa)
    commdyellowb = clean_data(commderroryellowb)
    
    commData=pd.concat([commData,commdbluea,commdblueb,commdyellowa,commdyellowb])
    commData.sort_values(by='DateTime', inplace=True, ascending=True)
    commData.reset_index(inplace=True, drop = True)
    del commData['error1']
    del commData['error2']
    return commData
#%%
def getAllComm(listOfCommands):
    allData = pd.DataFrame()
    for command in listOfCommands:
        commData = pd.read_table(os.path.join(dirName,command+'.txt'), parse_dates=True)
        commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]    
        commData['Command'] = command        
        allData = pd.concat((allData, commData), axis=0)
    allData = allData.sort('DateFormatted')
    return allData

def getAllComm_V2(listOfCommands):
    allData = pd.DataFrame()
    for command in listOfCommands:
        commData = pd.read_table(os.path.join(dirName,command+'.txt'), parse_dates=True)
        commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]    
        commData['Command'] = command   
        if command !='MVC':
            commData=process_data(commData)
        allData = pd.concat((allData, commData), axis=0)
    allData = allData.sort('DateFormatted')
    return allData

# get all command for subsea
allcomm = getAllComm(listOfCommands)
#if 'MVC' in listOfCommands:
#    listOfCommands.remove('MVC')
allcomm.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\subseaoldallcommand.csv'))
allcomm2 = getAllComm_V2(listOfCommands)
del allcomm2['Action']
allcomm2.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\subseanewallcommand.csv'))


# get all command for top
allcomm = getAllComm(listOfCommandstopside)
#if 'MVC' in listOfCommands:
#    listOfCommands.remove('MVC')
allcomm.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\topsideoldallcommand.csv'))
allcomm2 = getAllComm_V2(listOfCommandstopside)
del allcomm2['Action']
allcomm2.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\topsidenewallcommand.csv'))