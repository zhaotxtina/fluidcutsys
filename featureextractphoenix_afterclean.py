import pandas as pd
import os, time, datetime, pickle, json
import numpy as np
from matplotlib import pyplot as plt
import matplotlib.dates as mdates

from bokeh.plotting import *
from bokeh.models import ColumnDataSource
from bokeh.models import HoverTool
dir_data = r'G:\cameron_BOP\BP\Thunderhorse\fromsean'

#%%
def utc2date(UTC):
    date1 = time.strftime('%Y:%m:%d %H:%M:%S', time.localtime(UTC))
    date = datetime.datetime.strptime(date1, '%Y:%m:%d %H:%M:%S')
    return date
    
def date2utc(date):    
    convert = datetime.datetime.strptime(date, '\n%Y\%m\%d %H:%M:%S').timetuple()   
    utc = time.mktime(convert)    
    return utc
    
#%% Functions
def df_clean(df):
    
#    df.rename(columns = {'Unnamed: 0':'TS'}, inplace = True)
#    df['DT'] = [datetime.datetime.strptime(k, '%Y-%m-%d %H:%M:%S') for k in df['TS']]
    
#    cols_drop = ['Count', 'DataDescription', 'SemType', 'TimeStamp', 'TS']
    cols_drop = ['FromState', 'ToState']
    df.drop(cols_drop, axis=1, inplace=True)
    
    df['AcqTime'] = [int(time.mktime(time.strptime(str(k), '\n%Y\%m\%d %H:%M:%S'))) for k in df['DT']]
    df.sort_values(by='AcqTime', inplace=True, ascending=True)
    df.reset_index(inplace=True, drop = True)
    return df

def df_clean2(df):
    
    df['AcqTime'] = [int(time.mktime(time.strptime(str(k), '\n%Y\\%m\\%d %H:%M:%S'))) for k in df['DT']]
    df.sort_values(by='AcqTime', inplace=True, ascending=True)
    df.reset_index(inplace=True, drop = True)
    return df



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

    
#%% read in the command or valve txt files and merge them to a super df_allcom  dataframe

#%%
# get all command for subsea
#allcomm = getAllComm(listOfCommands)
##if 'MVC' in listOfCommands:
##    listOfCommands.remove('MVC')
#allcomm.to_csv(os.path.join(dirNameNew,'WestEclipsetotalData\\subseaoldallcommand.csv'))
dirName = 'G:\\cameron_BOP\\BP\\Thunderhorse\\fromsean\\aftercleaning\\'
fileallcomm=dirName+'subseaallcommandV2.csv'
allcommd =pd.read_csv(fileallcomm)




date2use = datetime.datetime(2013, 03, 29, 12, 00, 35)
acqtime2use = date2utc(date2use.strftime('\n%Y\%m\%d %H:%M:%S'))
#df_allcom = df_allcom.iloc[(df_allcom['AcqTime']>=acqtime2use).values,:]  # only use after certain acquisition time 2016/02/01

#%% 
# read in flow meter data, new version
flowReading = ['POD Flow meter.txt']
flowdata = pd.read_table(os.path.join(dir_data,flowReading[0]), parse_dates=True)  
flowdata['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in flowdata['DateTime'].values]
flowdata['AcqTime'] = [int(time.mktime(time.strptime(str(k), '%Y\\%m\\%d %H:%M:%S'))) for k in flowdata['DateTime']]
# I don't know the reason for 
flowdata = {'Blue A':flowdata[flowdata['SEMName']=='Blue A'], 'Blue B':flowdata[flowdata['SEMName']=='Blue B' ],  \
'Yellow A':flowdata[flowdata['SEMName']=='Yellow A'], 'Yellow B':flowdata[flowdata['SEMName']=='Yellow B']}



#%% read information from the function_tracking list

#funcTracking = pd.read_excel('G://cameron_BOP/BP/Thunderhorse/from Rajesh/Functions_Tracking_Rev5_RB_new.xlsx',sheetname = 'Thunderhorse',index_col = 0)
funcTracking = pd.read_excel('C://Users/tzhao/Documents/cameron_bop/BP/Thunderhorse/from Rajesh/Functions_Tracking_Rev5_RB_new.xlsx',sheetname = 'Thunderhorse',index_col = 0)

#funcTracking = pd.read_excel('G://cameron_BOP/BP/Thunderhorse/from Rajesh/Functions_Tracking_Rev4.xlsx',sheetname = 'Thunderhorse',index_col = 0)
funcTracking = funcTracking.dropna(subset=['EventLogger_functions'] )
funcTracking['Functions'] = funcTracking['Functions'].ffill()
funcTracking['Analogs'] = funcTracking['Analogs'].fillna(value='')
funcTracking['Wellbore_Pressure'] = funcTracking['Wellbore_Pressure'].fillna(value='')
funcTracking.reset_index(inplace=True, drop = True)

#%% for each function, find the corresponding flow and pressure data
dirNameOri= 'G:\\cameron_BOP\\BP\\Thunderhorse\\fromsean\\'
#for func_ind in range(len(funcTracking)):
for func_ind in range(len(funcTracking)):
    # GET READY INITIALIZE VARIABLES
#____________________________________________________________________________
#    func_ind=19  # for debugging
    func = funcTracking['EventLogger_functions'].iloc[func_ind]
#    flow_pick = funcTracking['Topside'].iloc[func_ind]
#    df_allcom = pd.read_excel(os.path.join(dir_com, com_files[flow_pick]))
#    df_allcom = df_allcom.iloc[(df_allcom['AcqTime']>=acqtime2use).values,:]  # only use after certain acquisition time 2016/02/01
#    df_flow = pd.read_csv(os.path.join(dir_data, flow_meter_files[flow_pick]))
#    df_flow = df_clean(df_flow)

# read the command data for this particular valve and process one by one to obtain the flow/pressure information
    fileallcomm=dirName+func+'_afterclean.csv'
    commData = pd.read_csv(fileallcomm)
#    func=func.split(';')
#    flow_pick = funcTracking['Topside'].iloc[func_ind]  # 0 is subsea, 1 is topside
#    if flow_pick==0:
#        allcomm=allcomm2
#    funci=listOfCommands[54]
#    commData = pd.read_table(os.path.join(dirName,funci+'.txt'), parse_dates=True)
    commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]
    commData['Command'] = func
    commData['Action']= commData['FromState']+" to "+commData['ToState']  # maybe not needed      
    commData['AcqTime'] = [int(time.mktime(time.strptime(str(k), '%Y\\%m\\%d %H:%M:%S'))) for k in commData['DateTime']]
 
# do some data cleaning, this may not be needed, since the filter command considered this
#_____

#    commData=process_data(commData)
#____         



#    units = df_flow.Units.values[0]
    if funcTracking['Analogs'].iloc[func_ind]=='' and funcTracking['Wellbore_Pressure'].iloc[func_ind]=='':
        wellbore_files=[]
    elif funcTracking['Analogs'].iloc[func_ind]=='':
        wellbore_files = funcTracking['Wellbore_Pressure'].iloc[func_ind].split('; ')
    elif funcTracking['Wellbore_Pressure'].iloc[func_ind]=='':
        wellbore_files = funcTracking['Analogs'].iloc[func_ind].split('; ')
    else: 
        wellbore_files = funcTracking['Analogs'].iloc[func_ind].split('; ') + funcTracking['Wellbore_Pressure'].iloc[func_ind].split('; ')
    wellbore_files = [ff+'.txt' for ff in wellbore_files]
    
    allsensors = []  # contains all pressure sensor information
    allunits = []
    df_wbpres = pd.DataFrame()
    
    for wellbore_file in wellbore_files:
        df_wbpres = pd.read_csv(os.path.join(dirNameOri, wellbore_file),sep='\t', lineterminator='\r')
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
   
      
#%
#  first step, get the flow and time of flow information for each command in the commData
    pretime_thresh = 10
    posttime_thresh = 10
    commtime_thresh = 3*60
    Flow=[]  # initialize for each function, the return results are empty 
    TimeOfFlow=[]
#    Active=[]
#    NanType =[] 
#    Flow,TimeOfFlow,presmean,preslen,presrange,Active,NanType = getFlowList(df_wbpres,commData, allcomm, flowdata, pretime_thresh, commtime_thresh)
    Flow,TimeOfFlow,presmean,preslen,presrange = getFlowList(df_wbpres,commData, allcommd, flowdata, pretime_thresh, commtime_thresh)

    commData['Flow']=Flow
    commData['TimeOfFlow']=TimeOfFlow
#    commData['Active']=Active
#    commData['NanType']=NanType  

#    commData.to_csv(os.path.join(dirNameOri,'OutputData\\%s_julyafterclean.csv' %func))
#flow,timeOfFlow, presmean, preslen, presrange

   # second step, get the pressure features (3) for each wellbore files
    for ind in range(len(allsensors)):
        df_wbpres = allsensors[ind]
        name = wellbore_files[ind][:-4]
                
        presmean = []  # initialize the result first
        preslen = []
        presrange = []
                
        Flow,TimeOfFlow,presmean,preslen,presrange = getFlowList(df_wbpres,commData, allcommd, flowdata, pretime_thresh, commtime_thresh)
              
            
        commData[name+'_mean'] = presmean
        commData[name+'_num_change'] = preslen
        commData[name+'_max_change'] = presrange
                                             
#___________                                             
         
        
    commData.to_csv(os.path.join(dirNameOri,'OutputData\\%s_julyafterclean3.csv' %func))