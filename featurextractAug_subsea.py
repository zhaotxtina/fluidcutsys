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
#
#%%
def utc2date(UTC):
    date1 = time.strftime('%Y:%m:%d %H:%M:%S', time.localtime(UTC))
    date = datetime.datetime.strptime(date1, '%Y:%m:%d %H:%M:%S')
    return date
    
def date2utc(date):    
    convert = datetime.datetime.strptime(date, '\n%Y\%m\%d %H:%M:%S').timetuple()   
    utc = time.mktime(convert)    
    return utc
    
#%%
# read in command data
#dirNameIn = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\Event Logger Files\\parsedclean\\subsea\\'
#dirNameOut = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\featurecsv\\subsea\\'
#dirName = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\Event Logger Files\\parsedtxtnew\\'

dirName = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\Event Logger Files\\parsedclean\\subsea\\'
fileallcomm=dirName+'cleansubseaallcommand.csv'
allcommd =pd.read_csv(fileallcomm)

date2use = datetime.datetime(2009, 10, 01, 14, 26, 00)
acqtime2use = date2utc(date2use.strftime('\n%Y\%m\%d %H:%M:%S'))

#%% read in flow reading file

dir_data = r'C:\Users\tzhao\Documents\cameron_bop\Seadrill\westphoenix\Event Logger Files\parsedtxtnew'
flowReading = ['Flowmeter.txt']
flowdata = pd.read_table(os.path.join(dir_data,flowReading[0]), parse_dates=True)  
flowdata['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in flowdata['DateTime'].values]
flowdata['AcqTime'] = [int(time.mktime(time.strptime(str(k), '%Y\\%m\\%d %H:%M:%S'))) for k in flowdata['DateTime']]
# I don't know the reason for 
flowdata = {'Blue A':flowdata[flowdata['SEMName']=='Blue A'], 'Blue B':flowdata[flowdata['SEMName']=='Blue B' ],  \
'Yellow A':flowdata[flowdata['SEMName']=='Yellow A'], 'Yellow B':flowdata[flowdata['SEMName']=='Yellow B']}


#%%

pretime_thresh = 10
posttime_thresh = 10
commtime_thresh = 3*60

#%%
#def new_clean_data(tocleandata):
#
##    tocleandata=commderrorbluea  # for debugging
#    tocleandata.reset_index(inplace=True, drop = True)  # reset the indexes from 1
#    
#
#    newrecord=pd.DataFrame() # to save the newly generated record after combining
#    newind=0
#    for ind in range(len(tocleandata)-1):
##        ind=16  # for debugging
#        if (tocleandata['DateFormatted'].iloc[ind+1]-tocleandata['DateFormatted'].iloc[ind]).total_seconds()<8.0:
#            
#            if (tocleandata['ToState'].iloc[ind] == tocleandata['FromState'].iloc[ind+1]):
#                
##                if (tocleandata['ToState'].iloc[ind+1]=='Vent' and tocleandata['FromState'].iloc[ind]=='Vent'):
#                if (tocleandata['ToState'].iloc[ind+1]==tocleandata['FromState'].iloc[ind]): # if there is no state change 
#                    if ind<=len(tocleandata)-3:
#                        ind=ind+2
#                else:
##                    
#                    newrecord=newrecord.append(tocleandata.iloc[ind+1,:])
#                    newrecord['FromState'].iloc[newind]=tocleandata['FromState'].iloc[ind]
##                   
#                    newind=newind+1
#                    if ind<=len(tocleandata)-3:
#                        ind=ind+2
#            else:
#                 if ind<=len(tocleandata)-3:
#                        ind=ind+2
#         
#        else:                           # if the time difference between it and the next one is more than 10 seconds, something is wrong
#
#            if ind<=len(tocleandata)-2:
#                ind=ind+1                # jump one more index
#                
#                
##    newrecord['Action']= newrecord['FromState']+" to "+newrecord['ToState']       
#                
#    newrecord.reset_index(inplace=True, drop = True)  # reset the indexes from 1           
#        
#    return newrecord
#
#def new_process_data(commData):
#    
#    commData['error1']=[1  if len(b.split(' '))>1  else  0   for b in commData['FromState']]    
#    commData['error2']=[1  if len(b.split(' '))>1  else  0   for b in commData['ToState' ]]
#    commData['error3']=[1  if b=='fault'  else  0   for b in commData['FromState' ]]
#    commData['error4']=[1  if b=='fault'  else  0   for b in commData['ToState' ]]
#    commData['error']= commData['error1']+commData['error2']+commData['error3']+commData['error4']
#    
#    
#
#    commderrorbluea= commData.loc[(commData['SEMName']=='Blue A') & (commData['error']==1),:]
#    commderrorblueb= commData.loc[(commData['SEMName']=='Blue B') & (commData['error']==1),:]
#    commderroryellowa= commData.loc[(commData['SEMName']=='Yellow A') & (commData['error']==1),:]
#    commderroryellowb= commData.loc[(commData['SEMName']=='Yellow B') & (commData['error']==1),:]
#
#
#    commdbluea = new_clean_data(commderrorbluea) 
#    commdblueb = new_clean_data(commderrorblueb)
#    commdyellowa = new_clean_data(commderroryellowa)
#    commdyellowb = new_clean_data(commderroryellowb)
#    
#    commData=commData[commData['error']==0]
#    
#    commData=pd.concat([commData,commdbluea,commdblueb,commdyellowa,commdyellowb])
#    commData.sort_values(by='DateTime', inplace=True, ascending=True)
#    commData.reset_index(inplace=True, drop = True)
#    del commData['error1']
#    del commData['error2']
#    del commData['error3']
#    del commData['error4']
#    del commData['error']
#    return commData
#%%


 
#%%

def filterCommands(df, singcomm, timeForSameCommand=4):  # check if df contains singcomm ?
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
            df_temp = df.iloc[(df['Command']==singcomm['Command']).values & (df['SEMName']==singcomm['SEMName']).values &\
                (df['DateFormatted']>=time_begin).values & (df['DateFormatted']<=time_end).values]
            # if there are such command, and first of it is the same as singcomm and last of it is the same state as in singcomm
            if len(df_temp)>0 and df_temp['FromState'].iloc[0] == singcomm['FromState'] and df_temp['ToState'].iloc[-1] == singcomm['ToState']:
                df.drop(df_temp.index)      # it's the same command as singcomm, drop it 
    return df
#%%    
def getFlowDetails(sing_comm, allcomm, flowData,wbpresData,activeData, pretime_thresh=10, commtime_thresh=3*60):
    fl = np.nan
    tm = np.nan
    df_mean = np.nan
    df_lenchange = np.nan
    df_change = np.nan
    get_flag = False
    precedDiscard = True   # first assign both precede and post discard as true
    postDiscard = True
#    pod = sing_comm['SEMName'][:-2]
#    sem = sing_comm['SEMName'][-1]
    flowStart = []
    flowEnd = []
    nanflag=100
    
##    #temporary
#    flowData=fd
#    wbpresData=wbpres
#    activeData=activedata
    
    timeNow = sing_comm['DateFormatted']   # current command time  t0    
    timePreced = timeNow - datetime.timedelta(seconds = pretime_thresh)   #  10 seconds before current command  t-1
    timeEnd = timeNow+datetime.timedelta(seconds = commtime_thresh)  #  3 minutes after current command t1
    # first check the post current time flow information within 3 minutes
    if timeNow<flowData['DateFormatted'].iloc[-1]:   # if t0 is within the flowdata time range t0 < the last time recorded in flowdata
        flowStart = flowData.iloc[(flowData['DateFormatted']>= timeNow).values & (flowData['State']=='Started').values,:].iloc[0,:]  # trying to capture the first flow data recorded
        flowEnd = flowData.iloc[(flowData['DateFormatted']>= timeNow).values & (flowData['State']=='Stopped').values,:]
        if len(flowEnd)!=0:   # if found stopped flow
            flowEnd = flowEnd.iloc[0,:]   # the first one is the flow stop
        elif (len(flowEnd)==0) & (len(flowStart)!=0):    # find start, but cannot find end within 3 minutes
            nanflag= 3    # flow is over 3 minutes
        elif len(flowStart)==0 & len(flowEnd)==0:    # cannot find either start or end
            nanflag= 1    # there is no flow
            
        if flowStart['DateFormatted']<=timeEnd and len(flowEnd)>0 and flowEnd['DateFormatted']<=timeEnd:  # if first flow start and end within 3 minutes
            timeEnd = flowEnd['DateFormatted']     # take the timeend as the flow end(the first one)
            postDiscard = False   # do not discard, use it in the feature
         # add output to indicate if it's no flow or overlap command if it's 1, it's no flow, if it's 0 it's overlap command
    else:
         nanflag= 10    # t0 is out of flow time range
#            
           
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
        df_followcom = allcomm[allcomm['DateFormatted'].between(timeNow, timeEnd)]  # check all commands from now to 3 minutes
        df_followcom = filterCommands(df_followcom, sing_comm)
        
#        df_followcom = df_followcom[np.logical_not((df_followcom['Command']==command).values & \
#                (df_followcom['FromState']==sing_comm['FromState']).values & (df_followcom['ToState']==sing_comm['ToState']).values) ]
        if len(df_followcom)==0:   # if there is no other commands
            get_flag = True    # compute the time flow feature
    else:
         nanflag = 0  # there is overlap command
        
    # add output is taken during active pod select or not, output it no matter if get_flag is active or not
    activeflag=0   # 0 means inactive, 1 means active
    df_before = activeData.iloc[(activeData['DateFormatted']<= timeNow).values]
    if len(df_before)!=0:   # if found stopped flow
        activeEnd = df_before.iloc[-1]   # the first one is the flow stop
        activeflag= activeEnd['Status'] 
    else:
        activeflag='out of range'
    
    
    if get_flag:    # if this command is valid for get flow
        fl = flowEnd['TotalFlow']
        tm = flowEnd['TimeSecs']
        
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
       
    return fl, tm, df_mean,df_lenchange,df_change, activeflag, nanflag
#    else:   # if get_flag=0
#        return fl, tm, df_mean,df_lenchange,df_wellbore, activeflag, nanflag

#%%    
def getFlowListeclipse(df_wbpres,commData, allcomm, flowData, pretime_thresh = 10, commtime_thresh = 180):
    flow = []
    timeOfFlow = []
    presmean=[]
    preslen=[]
    presrange=[]
#    active=[]
#    nanT=[]

    for ind in range(len(commData)):   # FOR EACH LINE in the command, compute the flow vs timeofflow
#        ind=0
        sing_comm = commData.iloc[ind,:]
#        sing_comm = commData.iloc[0,:] # for debug only
        if 'Blue A' in sing_comm['SEMName']:  # if it's in blue SEM, check the flowdata in BLUE
            fd = flowdata['Blue A']
            wbpres= df_wbpres[df_wbpres['SEMName']=='Blue A']

            wbpres.sort_values(by='AcqTime', inplace=True, ascending=True)
#            wbpres.reset_index(inplace=True, drop = True)
        elif 'Blue B' in sing_comm['SEMName']:   
            fd = flowdata['Blue B']
            wbpres= df_wbpres[df_wbpres['SEMName']=='Blue B']
            wbpres.sort_values(by='AcqTime', inplace=True, ascending=True)
        elif 'Yellow A' in sing_comm['SEMName']: 
            fd = flowdata['Yellow A']
            wbpres= df_wbpres[df_wbpres['SEMName']=='Yellow A']
#            
            wbpres.sort_values(by='AcqTime', inplace=True, ascending=True)
        elif 'Yellow B' in sing_comm['SEMName']:   #yellow B
            fd = flowdata['Yellow B']
            wbpres= df_wbpres[df_wbpres['SEMName']=='Yellow B']

            wbpres.sort_values(by='AcqTime', inplace=True, ascending=True)
#            wbpres.reset_index(inplace=True, drop = True)
        else:
            print "not in multisem?"
                        
#            activedata = YellowActiveData
            
#        fl,tm,pres_mean,pres_len,pres_range,activeind,nanindex = getFlowDetails(commData.iloc[ind,:], allcomm, fd,wbpres, activedata,pretime_thresh, commtime_thresh) # check one command each time
#        fl,tm,pres_mean,pres_len,pres_range,activeind,nanindex = getFlowDetails(sing_comm, allcomm, fd,wbpres, activedata,pretime_thresh, commtime_thresh) # check one command each time
        fl,tm,pres_mean,pres_len,pres_range = getFlowDetails(sing_comm, allcomm, fd,wbpres, activedata,pretime_thresh, commtime_thresh) # check one command each time
 
        flow.append(fl)
        timeOfFlow.append(tm)
        presmean.append(pres_mean)
        preslen.append(pres_len)
        presrange.append(pres_range)
#        active.append(activeind)
#        nanT.append(nanindex)
        
    return flow,timeOfFlow, presmean, preslen, presrange  #, active, nanT

#%%  extract features
#%% fl,tm,pres_mean,pres_len,pres_range,activeind,nanindex = get_feature(sing_comm,allcomm,fd,wbpres,pretime_thresh,commtime_thresh)
#def get_feature(df_sel, df_flow, df_allcom, com_sel = 62, dt_thresh = 180, t_1_thresh = 10):
#    get_feature(sing_comm, fd, allcommd)
def get_feature(sing_comm, df_flow, df_allcom,df_wbpres,dt_thresh = 180, t_1_thresh = 10):
    # this is for debugging, copy the same variables here
#    df_allcom1=df_allcom # save the other one first
#    df_flow1=df_flow
#    df_sel=sing_comm
    
#    df_allcom=allcommd  # or df_allcom in older version
#    df_flow=fd
#    dt_thresh = 180
#    t_1_thresh = 10
#    
    get_flag = False
    
#    utc_now = df_sel['AcqTime'].loc[com_sel]
#    timeNow = sing_comm['DateFormatted']
    utc_now = sing_comm['AcqTime']
    utc_end = utc_now+dt_thresh
    
    preced_discard = True
    utc_preced = utc_now - t_1_thresh
    #preceding commands
    df_precedcom = df_allcom[df_allcom['AcqTime'].between(utc_preced, utc_now)]
    #drop the command of interest
    valve=sing_comm["Command"]
    states=sing_comm["Action"]
    df_precedcom['Action']= df_precedcom['FromState']+" to "+df_precedcom['ToState']
    df_precedcom = df_precedcom[(df_precedcom['Command']!=valve) | (df_precedcom['Action']!=states)]
#    df_precedcom = filterCommands(df_precedcom, sing_comm)  # add this to filter
    #if there are preceding command...
    if len(df_precedcom)>0:
        #...check the last state and time
        preced_states = df_precedcom['Action'].values[-1]  # the last one on the list
        utc_preced_com = df_precedcom['AcqTime'].values[-1]
        #if the the last state is vent...
        if preced_states.lower().endswith('vent'):
            #...get flowmeter data and check if there is flow
            dft = df_flow[df_flow['AcqTime'].between(utc_preced_com, utc_now)]
#            print "preceding VENT"
            #if there is flow...
            if len(dft)>0:
                #...DISCARD
#                print "preceding VENT AND flow"
                dummy=1
            else:
                preced_discard = False
        else:
        #if the last stata is NOT vent... DISCARD
#            print "preceding com"
            dummy=1
    else:
        preced_discard = False
    

#==============================================================================
    if not preced_discard:
        #check following command
        df_followcom = df_allcom[df_allcom['AcqTime'].between(utc_now, utc_end)]
        df_followcom['Action']= df_followcom['FromState']+" to "+df_followcom['ToState']
        #drop the command of interest
        df_followcom = df_followcom[(df_followcom['Command']!=valve) | (df_followcom['Action']!=states)]
#        df_followcom = filterCommands(df_followcom, sing_comm)   # add this filter
#==============================================================================
    
        #flag for following event
        flag_follow = 1
        #if there are following commands... take the first command
        if len(df_followcom)>0:
            time_cut = df_followcom['AcqTime'].values[0]
        #if there are no following commands... horizon ends when the window ends
        else:
            time_cut = utc_end
            #and turn flag to 0
            flag_follow = 0
        
        #No. of following flowmeter readings
        no_followflow = len(df_flow[df_flow['AcqTime'].between(utc_now, time_cut)])
        
        #If there are no flowmeter readings...
        if no_followflow == 0:
            #...DISCARD
#            print 'No flow occurred at all'
            pass
        else:
        #If there are flowmeter readings... get them first
            
            dft = df_flow[df_flow['AcqTime'].between(utc_now, time_cut)]
#            print dft
            #Get the timestamp of the last flowmeter reading
            last_flow_utc = dft['AcqTime'].max()
            
            #check the time between last flowmeter reading last_flow_utc and the first following event  time_cut
            #If it's < 10
            if (time_cut-last_flow_utc<10) & flag_follow:  
                #Get the first flowmeter reading after the first event
                first_flow_followcom_utc = df_flow[df_flow['AcqTime']>time_cut]['AcqTime'].min()
                #if that flow meter happens to fast (10 seconds after the event)
                if first_flow_followcom_utc - last_flow_utc < 10:
                    #DISCARD
                    get_flag = False
#                    print 'Succeeding com AND flow'
                    pass
                #if it does not, KEEP!
                else:
                    get_flag = True
            else:
            #if the the time is > 10, or the horizon ends at 180
            #KEEP!
                get_flag = True
#    print get_flag
    if get_flag:
        
        #truncate flowmeter data and prepare for return        
        dft.loc[:, 'delta'] = [(dft.loc[k, 'ToValue']-dft.loc[k, 'FromValue']) if (dft.loc[k, 'ToValue']-dft.loc[k, 'FromValue'])>0 else dft.loc[k, 'ToValue'] for k in dft.index]
        total_flow= dft['delta'].sum()
        timeflow=dft['AcqTime'].max()-dft['AcqTime'].min()
        
        # get the pressure data
#        timeEnd = timeNow+datetime.timedelta(seconds = dt_thresh) 
#        df_presfea=wbpresData[wbpresData['DateFormatted'].between(timeNow, timeEnd)] 
#    
#        if len(df_presfea)==0:
#            try:
#                wbpres_last=wbpresData[wbpresData['DateFormatted']<timeNow]['To'].values[-1]
#            except:
#                wbpres_last=0
#            df_mean=wbpres_last
#            df_lenchange=0
#            df_change=0
#            
#        else:
#            try:
#                wbpres_last=wbpresData[wbpresData['DateFormatted']<timeNow]['To'].values[-1]
#            except:
#                wbpres_last=0
#            feat=list(df_presfea['To'].values)
#            feat.append(wbpres_last)
#            df_mean=sum(feat)/float(len(feat))
#            df_lenchange=len(df_presfea)
#            df_change=max(feat)-min(feat)
       
#    return total_flow,timeflow, df_mean,df_lenchange,df_change
        
        #truncate analog data and prepare for return        
        dfw = df_wbpres[df_wbpres['AcqTime'].between(utc_now, time_cut)]
        #if analog length is 0...
        if len(dfw) == 0:
            #... get the last available value as feature
            try: 
                wbpres_last = df_wbpres[df_wbpres['AcqTime']<utc_now]['To'].values[-1]
            except:
                wbpres_last = 0
            df_mean = wbpres_last
            df_lenchange = 0
            df_change = 0
        else:
        #if analog length is non-zero... extract features
            try:
                wbpres_last = df_wbpres[df_wbpres['AcqTime']<utc_now]['To'].values[-1]
            except:
                wbpres_last = 0
            feat = list(dfw['To'].values)
            feat.append(wbpres_last)  ### WZ: do we need to append?
            df_mean = sum(feat)/float(len(feat))
            df_lenchange = len(dfw)
            df_change = max(feat) - min(feat)
            
#        return df_mean, df_lenchange, df_change, dft
#        total_flow= dft['delta'].sum()
#        timeflow=dft['AcqTime'].max()-dft['AcqTime'].min()
#        nc=1
#        return  nc, dft
        return total_flow,timeflow, df_mean,df_lenchange,df_change
    else:
#        nc=-1
        return  0, 0,0,0,0
#%% get flow and pressure

  #%%  getFlowList(df_wbpres,commData, allcomm, flowdata, pretime_thresh, commtime_thresh) 
    # df_wbpres,commData, allcommd, flowdata
def getFlowList(df_wbpres,commData, allcommd, flowdata, pretime_thresh = 10, commtime_thresh = 180):
    flow = []
    timeOfFlow = []
    presmean=[]
    preslen=[]
    presrange=[]
#    active=[]
#    nanT=[]
    for ind in range(len(commData)):   # FOR EACH LINE in the command, compute the flow vs timeofflow
#        ind=0
        sing_comm = commData.iloc[ind,:]
#        sing_comm = commData.iloc[0,:] # for debug only
        if 'Blue A' in sing_comm['SEMName']:  # if it's in blue SEM, check the flowdata in BLUE
            fd = flowdata['Blue A']
            wbpres= df_wbpres[df_wbpres['SEMName']=='Blue A']

            wbpres.sort_values(by='AcqTime', inplace=True, ascending=True)
#            wbpres.reset_index(inplace=True, drop = True)
        elif 'Blue B' in sing_comm['SEMName']:   
            fd = flowdata['Blue B']
            wbpres= df_wbpres[df_wbpres['SEMName']=='Blue B']
            wbpres.sort_values(by='AcqTime', inplace=True, ascending=True)
        elif 'Yellow A' in sing_comm['SEMName']: 
            fd = flowdata['Yellow A']
            wbpres= df_wbpres[df_wbpres['SEMName']=='Yellow A']
#            
            wbpres.sort_values(by='AcqTime', inplace=True, ascending=True)
        elif 'Yellow B' in sing_comm['SEMName']:   #yellow B
            fd = flowdata['Yellow B']
            wbpres= df_wbpres[df_wbpres['SEMName']=='Yellow B']

            wbpres.sort_values(by='AcqTime', inplace=True, ascending=True)
#            wbpres.reset_index(inplace=True, drop = True)
        else:
            print "not in multisem?"
#            activedata = YellowActiveData
#        fl,tm,pres_mean,pres_len,pres_range,activeind,nanindex = get_feature(sing_comm,allcomm,fd,wbpres,pretime_thresh,commtime_thresh)
#        fl,tm,pres_mean,pres_len,pres_range,activeind,nanindex = getFlowDetails(sing_comm,allcomm,fd,wbpres,activedata,pretime_thresh, commtime_thresh) # check one command each time
#        total_flow= 'nan'
#        timeflow='nan'
#        nc,fl = get_feature(sing_comm, fd, allcommd,wbpres)
#        if nc != -1:
#            total_flow= fl['delta'].sum()
#            timeflow=fl['AcqTime'].max()-fl['AcqTime'].min()
        fd.reset_index(inplace=True, drop = True)
        wbpres.reset_index(inplace=True, drop = True)
        total_flow,timeflow, pres_mean,pres_lenchange,pres_change = get_feature(sing_comm, fd, allcommd,wbpres)

            
        flow.append(total_flow)
        timeOfFlow.append(timeflow)
        presmean.append(pres_mean)
        preslen.append(pres_lenchange)
        presrange.append(pres_change)
#        active.append(activeind)
#        nanT.append(nanindex)
        
#    return flow,timeOfFlow, presmean, preslen, presrange, active, nanT
#    return flow,timeOfFlow
    return flow,timeOfFlow, presmean, preslen, presrange

#%% read information from the function_tracking list
dirName = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\Event Logger Files\\parsedclean\\subsea\\'
# read in function tracking list 
 
funcTracking = pd.read_excel('C://Users/tzhao/Documents/cameron_bop/Seadrill/function_tracking/Functions_Tracking.xlsx',sheetname = 'West Phoenix',index_col = 0)
funcTracking = funcTracking.dropna(subset=['EventLogger_functions'] )
funcTracking['Functions'] = funcTracking['Functions'].ffill()
funcTracking['Analogs'] = funcTracking['Analogs'].fillna(value='')
funcTracking['Wellbore_Pressure'] = funcTracking['Wellbore_Pressure'].fillna(value='')
#
#date2use = datetime.datetime(2009, 10, 01, 14, 26, 00)
#acqtime2use = date2utc(date2use.strftime('\n%Y\%m\%d %H:%M:%S'))
# pick one function as one example
#for func_ind in range(len(funcTracking)-6)  # for all the functions from the tracking list
for func_ind in range(19,len(funcTracking)):
#for func_ind in range(1):   # process the subsea first
    func_ind=19
    
    func = funcTracking['EventLogger_functions'].iloc[func_ind]
    fileallcomm=dirName+func+'_afterclean.csv'
    commData = pd.read_csv(fileallcomm)
#    commData = pd.read_table(os.path.join(dirName,func+'.txt'), parse_dates=True)
#    func=func.split(';')
    flow_pick = funcTracking['Topside'].iloc[func_ind]  # 0 is subsea, 1 is topside
    if flow_pick==0:
        allcomm=allcommd
        
#    funci=listOfCommands[54]
#    commData = pd.read_table(os.path.join(dirName,funci+'.txt'), parse_dates=True)
    commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]
    commData['Command'] = func
    commData['Action']= commData['FromState']+" to "+commData['ToState']  # maybe not needed      
     
# do some data cleaning, this may not be needed, since the filter command considered this
#_____

#    commData=process_data(commData)
#____      
      
#  pressure files for the feature extraction
#    units = df_flowbluea.Units.values[0]
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
    dir_data = r'C:\Users\tzhao\Documents\cameron_bop\Seadrill\westphoenix\Event Logger Files\parsedtxtnew'
    for wellbore_file in wellbore_files:
        df_wbpres = pd.read_csv(os.path.join(dir_data, wellbore_file),sep='\t', lineterminator='\r')
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
#______
    #  first step, get the flow and time of flow information for each command in the commData
    Flow=[]  # initialize for each function, the return results are empty 
    TimeOfFlow=[]
#    Active=[]
#    NanType =[] 
    pretime_thresh = 10
    posttime_thresh = 10
    commtime_thresh = 3*60
#    Flow,TimeOfFlow,presmean,preslen,presrange,Active,NanType = getFlowList(df_wbpres,commData, allcomm, flowdata, pretime_thresh, commtime_thresh)
    Flow,TimeOfFlow,presmean,preslen,presrange = getFlowList(df_wbpres,commData, allcommd, flowdata, pretime_thresh, commtime_thresh)

    
    commData['Flow']=Flow
    commData['TimeOfFlow']=TimeOfFlow
#    commData['Active']=Active
#    commData['NanType']=NanType  
#_____                
    # second step, get the pressure features (3) for each wellbore files
    for ind in range(len(allsensors)):
        df_wbpres = allsensors[ind]
        name = wellbore_files[ind][:-4]
                
        presmean = []  # initialize the result first
        preslen = []
        presrange = []
                
#        Flow,TimeOfFlow,presmean,preslen,presrange,Active,NanType = getFlowList(df_wbpres,commData, allcomm, flowdata, pretime_thresh, commtime_thresh)
        Flow,TimeOfFlow,presmean,preslen,presrange = getFlowList(df_wbpres,commData, allcommd, flowdata, pretime_thresh, commtime_thresh)
             
            
        commData[name+'_mean'] = presmean
        commData[name+'_num_change'] = preslen
        commData[name+'_max_change'] = presrange
                                             
#___________                                             
         
        
    commData.to_csv(os.path.join(dirNameOut,'%s_featureV1.csv' %func))



