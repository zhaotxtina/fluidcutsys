import pandas as pd
import os, time, datetime, pickle, json
import numpy as np
from matplotlib import pyplot as plt
import matplotlib.dates as mdates

from bokeh.plotting import *
from bokeh.models import ColumnDataSource
from bokeh.models import HoverTool
#dir_data = r'G:\cameron_BOP\BP\Thunderhorse\fromsean'
dir_data = r'C:\Users\tzhao\Documents\cameron_bop\Seadrill\westphoenix\Event Logger Files\parsedtxtnew'
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


#%%
def new_clean_data(tocleandata):

    tocleandata=commderrorbluea  # for debugging
    tocleandata.reset_index(inplace=True, drop = True)  # reset the indexes from 1
    

    newrecord=pd.DataFrame() # to save the newly generated record after combining
    newind=0
#    for ind in range(len(tocleandata)-1):
    for ind in range(650):
#        ind=647  # for debugging
        if (tocleandata['DateFormatted'].iloc[ind+1]-tocleandata['DateFormatted'].iloc[ind]).total_seconds()<8.0:
            
            if ((tocleandata['ToState'].iloc[ind] == tocleandata['FromState'].iloc[ind+1]) and len(tocleandata['ToState'].iloc[ind].split(' '))>1 ):
                
#                if (tocleandata['ToState'].iloc[ind+1]=='Vent' and tocleandata['FromState'].iloc[ind]=='Vent'):
                if ((tocleandata['ToState'].iloc[ind+1]==tocleandata['FromState'].iloc[ind]) ): # if there is no state change 
                    if ind<=len(tocleandata)-3:
                        ind=ind+2
#                elif (tocleandata['ToState'].iloc[ind+1]!=tocleandata['FromState'].iloc[ind]):
                else:
#                    
                    newrecord=newrecord.append(tocleandata.iloc[ind+1,:])
                    newrecord['FromState'].iloc[newind]=tocleandata['FromState'].iloc[ind]
#                   
                    newind=newind+1
                    if ind<=len(tocleandata)-3:
                        ind=ind+2
            else:
                 if ind<=len(tocleandata)-3:
                        ind=ind+2
         
        else:                           # if the time difference between it and the next one is more than 10 seconds, something is wrong

            if ind<=len(tocleandata)-2:
                ind=ind+1                # jump one more index
                
                
    newrecord['Action']= newrecord['FromState']+" to "+newrecord['ToState']       
                
    newrecord.reset_index(inplace=True, drop = True)  # reset the indexes from 1           
        
    return newrecord

def new_process_data(commData):
    
    commData['error1']=[1  if len(b.split(' '))>1  else  0   for b in commData['FromState']]    
    commData['error2']=[1  if len(b.split(' '))>1  else  0   for b in commData['ToState' ]]
    commData['error3']=[1  if b=='fault'  else  0   for b in commData['FromState' ]]
    commData['error4']=[1  if b=='fault'  else  0   for b in commData['ToState' ]]
#    commData['error5']=[1  if 'Both Diags On' in b  else  0   for b in commData['FromState']]  
#    commData['error6']=[1  if 'Both Diags On' in b  else  0   for b in commData['ToState']]
#    commData['error7']=[1  if 'ERROR' in b  else  0   for b in commData['FromState']]  
#    commData['error8']=[1  if 'ERROR' in b  else  0   for b in commData['ToState']]
    commData['error']= commData['error1']+commData['error2']+commData['error3']+commData['error4']
#    commData['errora']= commData['error1']+commData['error2']+commData['error3']+commData['error4'] \
#                       +commData['error5']+commData['error6']+commData['error7']+commData['error8']
    

    commderrorbluea= commData.loc[(commData['SEMName']=='Blue A') & (commData['error']>=1),:]
    commderrorblueb= commData.loc[(commData['SEMName']=='Blue B') & (commData['error']>=1),:]
    commderroryellowa= commData.loc[(commData['SEMName']=='Yellow A') & (commData['error']>=1),:]
    commderroryellowb= commData.loc[(commData['SEMName']=='Yellow B') & (commData['error']>=1),:]


    commdbluea = new_clean_data(commderrorbluea) 
    commdblueb = new_clean_data(commderrorblueb)
    commdyellowa = new_clean_data(commderroryellowa)
    commdyellowb = new_clean_data(commderroryellowb)
    
    commData=commData[commData['error']==0]
    
    commData=pd.concat([commData,commdbluea,commdblueb,commdyellowa,commdyellowb])
    commData.sort_values(by='DateTime', inplace=True, ascending=True)
    commData.reset_index(inplace=True, drop = True)
    del commData['error1']
    del commData['error2']
    del commData['error3']
    del commData['error4']
    del commData['error']
    return commData

#%%  The list for all command files in the folder, can be simplified later
# read in flow data, command data, pod select data 
#dir_data = r'C:\Users\tzhao\Documents\cameron_bop\Seadrill\westphoenix\Event Logger Files\parsedtxtnew'
dirName = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\Event Logger Files\\parsedtxtnew\\'
dirNameNew = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\Event Logger Files\\parsedclean\\'
# this is for all subsea commands total 70 with MVC included
listOfCommands = os.listdir(dirName)
listOfCommandssubsea = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands if (not ('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))

listOfCommands2 = os.listdir(dirName)
listOfCommandstopside = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands2 if (('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))



    
#%% read in the command or valve txt files and merge them to a super df_allcom  dataframe

#dirName = 'G:\\cameron_BOP\\BP\\Thunderhorse\\fromsean\\'
def getAllComm(listOfCommands):
    allData = pd.DataFrame()
#    listOfCommands=command_files # for debugging
    for ind in range(len(listOfCommands)):
#        command=listOfCommands[4]
#    for ind in range(0,65):
        command=listOfCommands[ind]
        commData = pd.read_table(os.path.join(dirName,command+'.txt'), parse_dates=True)
        commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]    
#        func=command.split('.txt')[0]
        commData['Command'] = command
        commData['AcqTime'] = [int(time.mktime(time.strptime(str(k), '%Y\\%m\\%d %H:%M:%S'))) for k in commData['DateTime']]
#        commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]
#        commData['Command'] = func
        commData['Action']= commData['FromState']+" to "+commData['ToState']  # maybe not needed      
#        commData['AcqTime'] = [int(time.mktime(time.strptime(str(k), '%Y\\%m\\%d %H:%M:%S'))) for k in commData['DateTime']]
 
#    df.sort_values(by='AcqTime', inplace=True, ascending=True)
        commData=new_process_data(commData)
        commData.to_csv(os.path.join(dirNameNew,'%s_afterclean.csv' %command))
        allData = pd.concat((allData, commData), axis=0)
    allData = allData.sort('DateFormatted')
    return allData

#def getAllComm_V2(listOfCommands):
#    allData = pd.DataFrame()
#    for command in listOfCommands:
#        commData = pd.read_table(os.path.join(dirName,command+'.txt'), parse_dates=True)
#        commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]    
#        commData['Command'] = command   
#        if command !='MVC':
#            commData=process_data(commData)
#        allData = pd.concat((allData, commData), axis=0)
#    allData = allData.sort('DateFormatted')
#    return allData
#%%
# get all command for subsea functions
#allcomm = getAllComm(listOfCommands)
if 'MVC' in listOfCommandssubsea:
    listOfCommandssubsea.remove('MVC')
listOfCommandssubsea.remove('MVC_EDS')

allcommd = getAllComm(listOfCommandssubsea)
allcommd.reset_index(inplace=True, drop = True)
allcommd.to_csv(os.path.join(dirNameNew,'subsea\\cleansubseaallcommand.csv'))


# clean all commands for topside functions

allcommdtp = getAllComm(listOfCommandstopside)
allcommdtp.reset_index(inplace=True, drop = True)
allcommdtp.to_csv(os.path.join(dirNameNew,'topside\\cleantopsideallcommand.csv'))

#%% just three individual case study

#ind=195
#command=listOfCommands[ind]
#
#commData = pd.read_table(os.path.join(dirName,command), parse_dates=True)
#commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]    
##        func=command.split('.txt')[0]
#commData['Command'] = command
#commData['AcqTime'] = [int(time.mktime(time.strptime(str(k), '%Y\\%m\\%d %H:%M:%S'))) for k in commData['DateTime']]
##        commData['DateFormatted'] = [datetime.datetime.strptime(a, '%Y\\%m\\%d %H:%M:%S') for a in commData['DateTime'].values]
##        commData['Command'] = func
#commData['Action']= commData['FromState']+" to "+commData['ToState']  # maybe not needed      
##        commData['AcqTime'] = [int(time.mktime(time.strptime(str(k), '%Y\\%m\\%d %H:%M:%S'))) for k in commData['DateTime']]
# 
##    df.sort_values(by='AcqTime', inplace=True, ascending=True)
#commData=new_process_data(commData)
#commData.to_csv(os.path.join(dirNameNew,'%s_afterclean2.csv' %command))
#       
