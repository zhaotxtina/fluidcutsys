# -*- coding: utf-8 -*-
"""
Created on Mon Sept 22 2017

@author: tzhao
"""

import pandas as pd
import numpy as np

from sklearn.cluster import KMeans
from bokeh.plotting import *
from bokeh.models import ColumnDataSource
from bokeh.models import HoverTool

import os, time, datetime, pickle, json
#%%
def ComupteKmeans(datain,features):
#    datain=data
    numOfCluster = len(datain['Action'].unique()) +1
    
    npData = datain.loc[:,features].values
    for col in range(npData.shape[1]):     #normalize values
        if npData[:,col].max(axis=0) == npData[:,col].min(axis=0):
            pass
        else:
            npData[:,col] = (npData[:,col] - npData[:,col].min(axis=0))/(npData[:,col].max(axis=0)-npData[:,col].min(axis=0))
    
    kmeans = KMeans(n_clusters = numOfCluster)
    kmeans = kmeans.fit(npData)
    
    cluster = kmeans.cluster_centers_
    labels = kmeans.labels_
    
    datain['Anomaly'] = [np.mean((npData[ind,:].reshape(-1) - cluster[labels[ind],:])**2) for ind in range(len(labels))]
    return datain

def plot_data(data,valve):
#    data=datain
    reset_output()
    TOOLS = "pan,wheel_zoom,box_zoom,reset,save,box_select,lasso_select,resize,hover"
    
#    output_file(os.path.join(r'C:\Users\tzhao\Documents\cameron_bop\Seadrill\WestEclipseParsedOutput02\WestEclipsetotalData\topsidecolor',
#                             '%s_kmeansAnomaly.html'%valve))
    output_file(os.path.join(r'C:\Users\tzhao\Documents\cameron_bop\Seadrill\westphoenix\featurecsv\topsidekmean',
                             '%s_kmeansAnomaly.html'%valve))
    
    data['stringDate'] = [datetime.datetime.strftime(pd.to_datetime(dd), '%Y/%m/%d %H:%M:%S') for dd in data['DateTime'].values]
    
    source = ColumnDataSource(data = {'Index': data.index,
                                     'Action': data['Action'],
#                                     'flow': data['Flow'],
                                     'stringDate':data['stringDate'],
                                     'DT':data['DT'],
#                                     'timeflow':data['TimeOfFlow'],
                                     'Anomaly': data['Anomaly']})
    p1 = figure(plot_width=800, plot_height=400, tools=TOOLS,
               title="KMeans Anomaly for "+valve, x_axis_type = "datetime")
    
    colorlist=['black','blue','yellow','red','green','cyan','magenta']
#    sortby=data['ActionNum'].value_counts()
#    for i in range(len(sortby)):
#        p1.circle(data[data['ActionNum']==i]['DT'], data[data['ActionNum']==i]['Anomaly'], legend=data[data['ActionNum']==i]['Action'].iloc[0],size=5, color=colorlist[i])
    sortby=data['Action'].value_counts()
#    sortby.reset_index(inplace=True,drop=True)
    for i in range(len(sortby)):
#        legendcap=sortby.index[i]
        p1.circle(data[data['Action']==sortby.index[i]]['DT'], data[data['Action']==sortby.index[i]]['Anomaly'],\
                   legend=sortby.index[i],size=5, color=colorlist[i])


#    p1 = figure(plot_width=800, plot_height=400, tools=TOOLS,
#               title="KMeans Anomaly for "+valve, x_axis_type = "datetime")
#    bp = p1.circle('DT', 'Anomaly', size=5, source=source)  
#    
#    hover = p1.select(dict(type=HoverTool))
#    hover[0].renderers = [bp]
#    hover[0].tooltips = [
#                        ("Action", "@Action"),
#                        ("Time", "@stringDate"),
##                        ("Flow", "@flow"),
##                        ("Time of Flow", "@timeflow"),
#                        ]
#    hover[0].mode = 'mouse'         
    #p1.circle(data.DT,data.Anomaly, size=5)
    save(p1)
    

##%%
#inpPath2 = r'C:\Users\tzhao\Documents\cameron_bop\Seadrill\WestEclipseParsedOutput02\WestEclipsetotalData\topside'
#inpPath = r'C:\Users\tzhao\Documents\cameron_bop\Seadrill\WestEclipseParsedOutput02\WestEclipsetotalData\subsea'
#listOfFiles = os.listdir(inpPath)
#listOfFiles = [ff for ff in listOfFiles if ff[-4:]=='.csv']
#%%
#valve=listOfFiles[6]   # 3,7,8 has problem
def utc2date(UTC):
    date1 = time.strftime('%Y:%m:%d %H:%M:%S', time.localtime(UTC))
    date = datetime.datetime.strptime(date1, '%Y:%m:%d %H:%M:%S')
    return date
    
def date2utc(date):    
    convert = datetime.datetime.strptime(date, '\n%Y\%m\%d %H:%M:%S').timetuple()   
    utc = time.mktime(convert)    
    return utc
#%%
#funcTracking = pd.read_excel('C://Users/tzhao/Documents/cameron_bop/Seadrill/function_tracking/Functions_Tracking_Rev5_RB_older.xlsx',sheetname = 'West_Eclipse',index_col = 0)
funcTracking = pd.read_excel('C://Users/tzhao/Documents/cameron_bop/Seadrill/function_tracking/Functions_Tracking.xlsx',sheetname = 'West Phoenix',index_col = 0)
funcTracking = funcTracking.dropna(subset=['EventLogger_functions'] )
funcTracking['Functions'] = funcTracking['Functions'].ffill()
funcTracking['Analogs'] = funcTracking['Analogs'].fillna(value='')
funcTracking['Wellbore_Pressure'] = funcTracking['Wellbore_Pressure'].fillna(value='')
funcTracking.reset_index(inplace=True, drop = True)  # reset the indexes from 1
date2use = datetime.datetime(2010, 01, 04, 13, 20, 11)  # starting time from allcomm, but not sure
acqtime2use = date2utc(date2use.strftime('\n%Y\%m\%d %H:%M:%S'))                  
 
#%%
#dirNameNew = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\WestEclipseParsedOutput02\\WestEclipsetotalData\\subsea'
#for func_ind in range(32)[0:27]:   # process the subsea functions
#dirNameNew = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\WestEclipseParsedOutput02\\WestEclipsetotalData\\topside'
dirNameNew = 'C:\\Users\\tzhao\\Documents\\cameron_bop\\Seadrill\\westphoenix\\featurecsv\\topside' 
for func_ind in range(28,31):   # process the topside functions
    func_ind=30
  
    func = funcTracking['EventLogger_functions'].iloc[func_ind]
    valve=str(func)
    data = pd.read_csv(os.path.join(dirNameNew,func+'_featureV1.csv'))
    data=data.dropna()   # drop the nan values
#    data=data[data.Active=='Active']  # only select the active ones

    data['Action']=data['FromState']+"--->"+data['ToState'] # action decide how many clusters        
    data['DT'] = pd.to_datetime(data['DateTime'])
    
    # obtain how many pressure files in the function
    wellbore_files = funcTracking['Analogs'].iloc[func_ind].split('; ')
#    if funcTracking['Analogs'].iloc[func_ind]=='' and funcTracking['Wellbore_Pressure'].iloc[func_ind]=='':
#        wellbore_files=[]
#    elif funcTracking['Analogs'].iloc[func_ind]=='':
#        wellbore_files = funcTracking['Wellbore_Pressure'].iloc[func_ind].split('; ')
#    elif funcTracking['Wellbore_Pressure'].iloc[func_ind]=='':
#        wellbore_files = funcTracking['Analogs'].iloc[func_ind].split('; ')
#    else: 
#        wellbore_files = funcTracking['Analogs'].iloc[func_ind].split('; ') + funcTracking['Wellbore_Pressure'].iloc[func_ind].split('; ')
#    wellbore_files = [ff+'.txt' for ff in wellbore_files]
    
    
    
    features = []
#    to_remove = ['Unnamed: 0', 'Valve', 'DT', 'AcqTime', 'Diverter_Accum_Flowmeter_Flag', 'Action', 'valve', 'stringDate']
#    features = [ff for ff in list(data.columns.values) if ff not in to_remove]
#    
#    features=['Flow','TimeOfFlow'];
             
    data['Action1']=data['Action'].astype('category') 
    data['ActionNum'] = list(pd.Categorical.from_array(data['Action1']).codes)
    features.append('ActionNum')
    
    data['valve1']=data['Command'].astype('category')
    data['valveNum'] = list(pd.Categorical.from_array(data['valve1']).codes)
    features.append('valveNum')
    for ind in range(len(wellbore_files)):
        str1=str(wellbore_files[ind])+'_mean'
        features.append(str1)
        str1=str(wellbore_files[ind])+'_num_change'
        features.append(str1)
        str1=str(wellbore_files[ind])+'_max_change'
        features.append(str1)
#    str1=str(str(funcTracking['Analogs'].iloc[func_ind]))+'_mean'
#    features.append(str1)
#    str1=str(str(funcTracking['Analogs'].iloc[func_ind]))+'_num_change'
#    features.append(str1)
#    str1=str(str(funcTracking['Analogs'].iloc[func_ind]))+'_max_change'
#    features.append(str1)
    
#    
#    
    if len(data) > 4:
        data=ComupteKmeans(data,features)
        if len(data) > 0:
            plot_data(data,valve)
#  
#
        
    
        
    
    
    
    