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
#dirName = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\WestEclipseParsedOutput01\\'
#dirNameNew = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\WestEclipseParsedOutput02\\'

dirName = 'G:\\cameron_BOP\\Seadrill\\WestEclipseParsedOutput01\\'
dirNameNew ='G:\\cameron_BOP\\Seadrill\\WestEclipseParsedOutput02\\'
# this is for all subsea commands total 70 with MVC included
listOfCommands = os.listdir(dirName)
#listOfCommands = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands if (not ('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))
# this is for all subsea commands total 70 with MVC included, 18 HPU commands for topside?
#listOfCommands = os.listdir(dirName)
#listOfCommands = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands if (not ('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))
listOfCommands = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands if (('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))



pretime_thresh = 10
posttime_thresh = 10
commtime_thresh = 3*60

# read in flow meter data
flowReading = ['FlowStartStop.txt']
flowdata = pd.read_table(os.path.join(dirName,flowReading[0]), parse_dates=True)  
flowdata['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in flowdata['DateTime'].values]
flowdata = {'Blue':flowdata.iloc[(flowdata['POD']=='Blue').values, :], 'Yellow':flowdata.iloc[(flowdata['POD']=='Yellow').values, :]}

# read in pod select to select which pod and sems is active 
BluePodSelect=['BLUE POD SELECT.txt']
YellowPodSelect=['YELLOW POD SELECT.txt']

BlueActiveData = pd.read_table(os.path.join(dirName,BluePodSelect[0]),  parse_dates=True)
YellowActiveData = pd.read_table(os.path.join(dirName,YellowPodSelect[0]),  parse_dates=True) 
#
BlueActiveData['Status'] = ['Active' if b=='OPEN' else 'InActive' for b in BlueActiveData['ToState']]
YellowActiveData['Status'] = ['Active' if d=='OPEN' else 'InActive' for d in YellowActiveData['ToState']]
BlueActiveData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in BlueActiveData['DateTime'].values]
YellowActiveData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in YellowActiveData['DateTime'].values]
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
#allcomm = getAllComm(listOfCommands)
##if 'MVC' in listOfCommands:
##    listOfCommands.remove('MVC')
#allcomm.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\subseaoldallcommand.csv'))
#allcomm2 = getAllComm_V2(listOfCommands)
#del allcomm2['Action']
#allcomm2.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\subseanewallcommand.csv'))

listOfCommands = os.listdir(dirName)
listOfCommandstopside = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands if (('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))
# get all command for top
allcommtopside = getAllComm(listOfCommandstopside)
#if 'MVC' in listOfCommands:
#    listOfCommands.remove('MVC')
allcommtopside.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\topsideoldallcommand.csv'))
#allcomm2 = getAllComm_V2(listOfCommandstopside)
#del allcomm2['Action']
#allcomm2.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\topsidenewallcommand.csv'))




#if 'MVC' in listOfCommands:
#    listOfCommands.remove('MVC')    
#%%

def filterCommands(df, singcomm, timeForSameCommand=3):  # check if df contains singcomm ?
# check if it's the same command, and same action
    df = df[np.logical_not((df['Command']==singcomm['Command']).values & \
                           (df['FromState']==singcomm['FromState']).values & (df['ToState']==singcomm['ToState']).values) ]
    if len(df)>0:      # if not, further check 
        df = df.iloc[(df['Command']!='MVC').values]  # check only non multi valve commands
        # if have the same command, and from state
        df_filtered = df.iloc[(df['Command']==singcomm['Command']).values & (df['FromState']==singcomm['FromState']).values].copy()
        for time_begin in df_filtered['DateFormatted'].values:  # all the time in different command
            time_end = time_begin + np.timedelta64(timeForSameCommand, 's')     # check within 2 second frame
            # same command, same semname, time within 2 seconds
#            df_temp = df.iloc[(df['Command']==singcomm['Command']).values & (df['SEMName']==singcomm['SEMName']).values &\
#                (df['DateFormatted']>=time_begin).values & (df['DateFormatted']<=time_end).values]
            df_temp = df.iloc[(df['Command']==singcomm['Command']).values &\
                (df['DateFormatted']>=time_begin).values & (df['DateFormatted']<=time_end).values]
            # if there are such command, and first of it is the same as singcomm and last of it is the same state as in singcomm
            if len(df_temp)>0 and df_temp['FromState'].iloc[0] == singcomm['FromState'] and df_temp['ToState'].iloc[-1] == singcomm['ToState']:
                df.drop(df_temp.index)      # it's the same command as singcomm, drop it 
    return df
#%%    getFlowDetails(sing_comm, allcomm, wbpres,pretime_thresh, commtime_thresh)
def getFlowDetails(sing_comm, allcomm, wbpresData, pretime_thresh=10, commtime_thresh=3*60):
#    fl = np.nan
#    tm = np.nan
    df_mean = np.nan
    df_lenchange = np.nan
    df_change = np.nan
    get_flag = False
    precedDiscard = True   # first assign both precede and post discard as true
    postDiscard = True
#    pod = sing_comm['SEMName'][:-2]
#    sem = sing_comm['SEMName'][-1]
#    flowStart = []
#    flowEnd = []
#    nanflag=100
    
##    #temporary
#    flowData=fd
#    wbpresData=wbpres
#    activeData=activedata
    
    timeNow = sing_comm['DateFormatted']   # current command time  t0    
    timePreced = timeNow - datetime.timedelta(seconds = pretime_thresh)   #  10 seconds before current command  t-1
    timeEnd = timeNow+datetime.timedelta(seconds = commtime_thresh)  #  3 minutes after current command t1
    # first check the post current time flow information within 3 minutes
#    if timeNow<wbpresData['DateFormatted'].iloc[-1]:   # if t0 is within the flowdata time range t0 < the last time recorded in flowdata
#        wbpresStart = wbpresData.iloc[(wbpresData['DateFormatted']>= timeNow).values & (wbpresData['State']=='Started').values,:].iloc[0,:]  # trying to capture the first flow data recorded
#        wbpresEnd = wbpresData.iloc[(wbpresData['DateFormatted']>= timeNow).values & (wbpresData['State']=='Stopped').values,:]
#        if len(wbpresEnd)!=0:   # if found stopped flow
#            wbpresEnd = wbpresEnd.iloc[0,:]   # the first one is the flow stop
#        elif (len(wbpresEnd)==0) & (len(wbpresStart)!=0):    # find start, but cannot find end within 3 minutes
#            nanflag= 3    # flow is over 3 minutes
#        elif len(wbpresStart)==0 & len(wbpresEnd)==0:    # cannot find either start or end
#            nanflag= 1    # there is no flow
#            
#        if wbpresStart['DateFormatted']<=timeEnd and len(wbpresEnd)>0 and wbpresEnd['DateFormatted']<=timeEnd:  # if first flow start and end within 3 minutes
#            timeEnd = wbpresEnd['DateFormatted']     # take the timeend as the flow end(the first one)
#            postDiscard = False   # do not discard, use it in the feature
#         # add output to indicate if it's no flow or overlap command if it's 1, it's no flow, if it's 0 it's overlap command
#    else:
#         nanflag= 10    # t0 is out of flow time range
##          
#  post commands within 180 seconds time frame  
    df_postcom = allcomm[allcomm['DateFormatted'].between(timeNow,timeEnd)]
    df_postcom = filterCommands(df_postcom, sing_comm)
#    df_precedcom = df_precedcom[np.logical_not((df_precedcom['Command']==command).values & \
#                (df_precedcom['FromState']==sing_comm['FromState']).values & (df_precedcom['ToState']==sing_comm['ToState']).values) ]
    
    if len(df_postcom)==0:    # if there is no preceding command, should have no command
        postDiscard = False   # do not discard
    else: 
        postDiscard = True        
    #  preceding commands within 10 seconds time frame
    df_precedcom = allcomm[allcomm['DateFormatted'].between(timePreced, timeNow)]
    df_precedcom = filterCommands(df_precedcom, sing_comm)
#    df_precedcom = df_precedcom[np.logical_not((df_precedcom['Command']==command).values & \
#                (df_precedcom['FromState']==sing_comm['FromState']).values & (df_precedcom['ToState']==sing_comm['ToState']).values) ]
    
    if len(df_precedcom)==0:    # if there is no preceding command, should have no command
        precedDiscard = False   # do not discard
    else: 
        precedDiscard = True   
        
    if not precedDiscard and not postDiscard:    # if both preceed and post say do not discard, then compute the flow and timeofflow
#        df_followcom = allcomm[allcomm['DateFormatted'].between(timeNow, timeEnd)]  # check all commands from now to 3 minutes
#        df_followcom = filterCommands(df_followcom, sing_comm)
#        
##        df_followcom = df_followcom[np.logical_not((df_followcom['Command']==command).values & \
##                (df_followcom['FromState']==sing_comm['FromState']).values & (df_followcom['ToState']==sing_comm['ToState']).values) ]
#        if len(df_followcom)==0:   # if there is no other commands
            get_flag = True    # compute the time flow feature
#    else:
#         nanflag = 0  # there is overlap command
        
#    # add output is taken during active pod select or not, output it no matter if get_flag is active or not
#    activeflag=0   # 0 means inactive, 1 means active
#    df_before = activeData.iloc[(activeData['DateFormatted']<= timeNow).values]
#    if len(df_before)!=0:   # if found stopped flow
#        activeEnd = df_before.iloc[-1]   # the first one is the flow stop
#        activeflag= activeEnd['Status'] 
#    else:
#        activeflag='out of range'
    
    
    if get_flag:    # if this command is valid for get flow
#        fl = flowEnd['TotalFlow']
#        tm = flowEnd['TimeSecs']
        
# get the pressure data
        timeEnd = timeNow+datetime.timedelta(seconds = commtime_thresh) 
        df_presfea=wbpresData[wbpresData['DateFormatted'].between(timeNow, timeEnd)] 
    
        if len(df_presfea)==0:
            try:
                wbpres_last=wbpresData[wbpresData['DateFormatted']<timeNow]['To'].values[-1]
            except:
                wbpres_last=0
            df_mean=wbpres_last
            df_lenchange=0
            df_change=0
            
        else:
            try:
                wbpres_last=wbpresData[wbpresData['DateFormatted']<timeNow]['To'].values[-1]
            except:
                wbpres_last=0
            feat=list(df_presfea['To'].values)
            feat.append(wbpres_last)
            df_mean=sum(feat)/float(len(feat))
            df_lenchange=len(df_presfea)
            df_change=max(feat)-min(feat)
       
    return df_mean,df_lenchange,df_change
#    else:   # if get_flag=0
#        return fl, tm, df_mean,df_lenchange,df_wellbore, activeflag, nanflag

#%%    getFlowList(df_wbpres,commData, allcomm,pretime_thresh, commtime_thresh)
def getFlowList(df_wbpres,commData, allcomm, pretime_thresh = 10, commtime_thresh = 180):
    presmean=[]
    preslen=[]
    presrange=[]
#    active=[]
#    nanT=[]
    for ind in range(len(commData)):   # FOR EACH LINE in the command, compute the flow vs timeofflow
#        ind=1
        sing_comm = commData.iloc[ind,:]
        wbpres=df_wbpres
            
#        fl,tm,pres_mean,pres_len,pres_range,activeind,nanindex = getFlowDetails(commData.iloc[ind,:], allcomm, fd,wbpres, activedata,pretime_thresh, commtime_thresh) # check one command each time
        pres_mean,pres_len,pres_range = getFlowDetails(sing_comm, allcomm, wbpres,pretime_thresh, commtime_thresh) # check one command each time
      
#        flow.append(fl)
#        timeOfFlow.append(tm)
        presmean.append(pres_mean)
        preslen.append(pres_len)
        presrange.append(pres_range)
#        active.append(activeind)
#        nanT.append(nanindex)
        
    return presmean, preslen, presrange


#%%  this is for all command and do it line by line so it's slow
# get all command
#allcomm = getAllComm(listOfCommands)
#if 'MVC' in listOfCommands:
#    listOfCommands.remove('MVC')
    
#    command=listOfCommands[8]  # had 5, then 6:10,11:15
    

#% for each function, find the corresponding flow and pressure data
#for command in listOfCommands:
#    commData = pd.read_table(os.path.join(dirName,command+'.txt'), parse_dates=True)
#    commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]
#    commData['Command'] = command    
#    commData['Flow'], commData['TimeOfFlow'],commData['Active'], commData['NanType'] = getFlowList(commData, allcomm, flowdata, pretime_thresh, commtime_thresh)
##    commData['Flow'], commData['TimeOfFlow'],commData['Active'] = getFlowList(commData, allcomm, flowdata, pretime_thresh, commtime_thresh)
#    
#    commData.to_csv(os.path.join(dirName,'WestEclipseFlowData\\%s.csv' %command))

#_________________________________________        

#%%  use function_tracking list to extract features
#%%
def utc2date(UTC):
    date1 = time.strftime('%Y:%m:%d %H:%M:%S', time.localtime(UTC))
    date = datetime.datetime.strptime(date1, '%Y:%m:%d %H:%M:%S')
    return date
    
def date2utc(date):    
    convert = datetime.datetime.strptime(date, '\n%Y\%m\%d %H:%M:%S').timetuple()   
    utc = time.mktime(convert)    
    return utc


#%% read information from the function_tracking list

# read in function tracking list   
funcTracking = pd.read_excel('C:/Users/tzhao/Documents/cameron_bop/Seadrill/function_tracking/Functions_Tracking_Rev5_RB.xlsx',sheetname = 'West_Eclipse',index_col = 0)
funcTracking = funcTracking.dropna(subset=['EventLogger_functions'] )
funcTracking['Functions'] = funcTracking['Functions'].ffill()
funcTracking['Analogs'] = funcTracking['Analogs'].fillna(value='')
funcTracking['Wellbore_Pressure'] = funcTracking['Wellbore_Pressure'].fillna(value='')
date2use = datetime.datetime(2010, 01, 04, 13, 20, 11)  # starting time from allcomm, but not sure
acqtime2use = date2utc(date2use.strftime('\n%Y\%m\%d %H:%M:%S'))
#%%
# pick one function as one example
#for func_ind in range(len(funcTracking)-6)  # for all the functions from the tracking list
#for func_ind in range(len(funcTracking)):
for func_ind in range(31)[27:31]:   # process the subsea first
#    func_ind=31
    
    func = funcTracking['EventLogger_functions'].iloc[func_ind]
    commData = pd.read_table(os.path.join(dirName,func+'.txt'), parse_dates=True)
#    func=func.split(';')
    flow_pick = funcTracking['Topside'].iloc[func_ind]  # 0 is subsea, 1 is topside
    if flow_pick==0:
        allcomm=allcomm2
    else:
        allcomm=allcommtopside
#    funci=listOfCommands[54]
#    commData = pd.read_table(os.path.join(dirName,funci+'.txt'), parse_dates=True)
    commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]
    commData['Command'] = func
    commData['Action']= commData['FromState']+" to "+commData['ToState']  # maybe not needed      
     
# do some data cleaning, this may not be needed, since the filter command considered this
#_____not needed for topside

#    commData=process_data(commData)
#____      
      
#  pressure files for the feature extraction
#  
    if funcTracking['Analogs'].iloc[func_ind]=='' and funcTracking['Wellbore_Pressure'].iloc[func_ind]=='':
        wellbore_files=[]
    elif funcTracking['Analogs'].iloc[func_ind]=='':
        wellbore_files = funcTracking['Wellbore_Pressure'].iloc[func_ind].split('; ')
    elif funcTracking['Wellbore_Pressure'].iloc[func_ind]=='':
        wellbore_files = funcTracking['Analogs'].iloc[func_ind].split('; ')
    else: 
        wellbore_files = funcTracking['Analogs'].iloc[func_ind].split('; ') + funcTracking['Wellbore_Pressure'].iloc[func_ind].split('; ')
    wellbore_files = [ff+'.txt' for ff in wellbore_files]
#______    
    allsensors = []  # contains all pressure sensor information
    allunits = []
    df_wbpres = pd.DataFrame()
    for wellbore_file in wellbore_files:
        df_wbpres = pd.read_csv(os.path.join(dirName, wellbore_file),sep='\t', lineterminator='\r')
        df_wbpres= df_wbpres.rename(columns={'FromValue':'From'}) 
        df_wbpres= df_wbpres.rename(columns={'ToValue':'To'})
        df_wbpres= df_wbpres.rename(columns={'DateTime':'DT'})
        df_wbpres=df_wbpres.dropna() 
        df_wbpres['wellbore']=wellbore_file[:-4]
        df_wbpres['AcqTime'] = [int(time.mktime(time.strptime(str(k), '\n%Y\%m\%d %H:%M:%S'))) for k in df_wbpres['DT']]
        df_wbpres['DateFormatted'] = [datetime.datetime.strptime(a, '\n%Y\%m\%d %H:%M:%S') for a in df_wbpres['DT'].values]
        units_wbpres = df_wbpres.Units.values[0]
        allsensors.append(df_wbpres)
        allunits.append(units_wbpres)  # why have it? 
#____There is no flow meter file for topside in this rig__
#    #  first step, get the flow and time of flow information for each command in the commData
#    Flow=[]  # initialize for each function, the return results are empty 
#    TimeOfFlow=[]
#    Active=[]
#    NanType =[] 
#    Flow,TimeOfFlow,presmean,preslen,presrange,Active,NanType = getFlowList(df_wbpres,commData, allcomm, flowdata, pretime_thresh, commtime_thresh)
#    commData['Flow']=Flow
#    commData['TimeOfFlow']=TimeOfFlow
#    commData['Active']=Active
#    commData['NanType']=NanType  
#_____                
    # second step, get the pressure features (3) for each wellbore files
    for ind in range(len(allsensors)):
#        ind=0
        df_wbpres = allsensors[ind]
        name = wellbore_files[ind][:-4]
                
        presmean = []  # initialize the result first
        preslen = []
        presrange = []
                
        presmean,preslen,presrange = getFlowList(df_wbpres,commData, allcomm,pretime_thresh, commtime_thresh)
              
            
        commData[name+'_mean'] = presmean
        commData[name+'_num_change'] = preslen
        commData[name+'_max_change'] = presrange
                                             
#___________                                             
         
        
    commData.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\%s_preprocess.csv' %func))



