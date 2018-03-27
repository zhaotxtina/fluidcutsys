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
dir_data = r'C:\Users\tzhao\Documents\cameron_bop\Seadrill\westphoenix\Event Logger Files\parsedtxtnew'

dirName = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\Event Logger Files\\parsedtxtnew\\'
dirNameNew = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\Event Logger Files\\parsedclean\\'
# this is for all subsea commands total 70 with MVC included
listOfCommands = os.listdir(dirName)
listOfCommandssubsea = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands if (not ('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))

listOfCommands2 = os.listdir(dirName)
listOfCommandstopside = list(set([a.replace('.txt','').replace('_CMD','') for a in listOfCommands2 if (('HPU' in a)) and a.replace('.txt','').replace('_CMD','').isupper()]))

listOfCommandstopside.remove('HPU_4_ RISER FILL-UP')  # this one doesn't exist
#%%
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
#        commData=new_process_data(commData)
        commData.to_csv(os.path.join(dirNameNew,'%s_afterclean.csv' %command))
        allData = pd.concat((allData, commData), axis=0)
    allData = allData.sort('DateFormatted')
    return allData

#%%
# clean all commands for topside functions

allcommdtp = getAllComm(listOfCommandstopside)
allcommdtp.reset_index(inplace=True, drop = True)
allcommdtp.to_csv(os.path.join(dirNameNew,'topside\\cleantopsideallcommand.csv'))



#%%
