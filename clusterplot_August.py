# -*- coding: utf-8 -*-
"""
Created on Mon May 1 2017

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
#def ComupteKmeans(datain,features):
#    numOfCluster = len(datain['Action'].unique()) +1
#    
#    npData = datain.loc[:,features].values
#    for col in range(npData.shape[1]):     #normalize values
#        if npData[:,col].max(axis=0) == npData[:,col].min(axis=0):
#            pass
#        else:
#            npData[:,col] = (npData[:,col] - npData[:,col].min(axis=0))/(npData[:,col].max(axis=0)-npData[:,col].min(axis=0))
#    
#    kmeans = KMeans(n_clusters = numOfCluster)
#    kmeans = kmeans.fit(npData)
#    
#    cluster = kmeans.cluster_centers_
#    labels = kmeans.labels_
#    
#    datain['Anomaly'] = [np.mean((npData[ind,:].reshape(-1) - cluster[labels[ind],:])**2) for ind in range(len(labels))]
#    return datain
    #%%
def plot_csvdata(data,valve,semname,features):
#    data=data_bluea
#    semname="Blue A"
    reset_output()
    TOOLS = "pan,box_zoom,reset,save,box_select,resize,hover"
        
    filename = func.replace('*','').replace('(','').replace(')','').replace('/','').replace(';','_')+'_'+valve
#    output_file(os.path.join(r'G:\cameron_BOP\Seadrill\WestEclipseParsedOutput02\WestEclipsetotalData\subseacolor',
#                             '%s_clusterplot_%s.html'%(valve,semname)))
    output_file(os.path.join(r'G:\cameron_BOP\BP\Thunderhorse\fromsean\plots\clusterplots',
                                 '%s_clusterplot3_%s.html'%(valve,semname)))
#        output_file(os.path.join(r'D:\\MeenakshiData\\BOP\\Songa Equinox\\SensorPlots\\',
#                                 '%s.html'%(filename)))
    data['stringDate'] = [datetime.datetime.strftime(pd.to_datetime(dd), '%Y/%m/%d %H:%M:%S') for dd in data['DT'].values]
#    features = []
    source = ColumnDataSource(data = {'Index': data.index,
                                     'Action': data['Action'],
                                     'flow': data['Flow'],
                                     'stringDate':data['stringDate'],
                                     'DT':data['DT'],
                                     'timeflow':data['TimeOfFlow'],
                                     'PressureMean':data[features[4]],
                                     'PressureNumchange':data[features[5]],
                                     'PressureMaxchange':data[features[6]]})
#    for name in features:
#        features.extend([name[:-4]+'_mean', name[:-4]+'_num_change', name[:-4]+'_max_change'])                         
#        source = ColumnDataSource(data = data.to_dict('list'))
#        
#        hover = HoverTool(
#                tooltips=[
#                    ("Action", "@Action"),
#                    ("Time", "@stringDate"),
#                    ("Flow", "@flow"),
#                    ("Time of Flow", "@timeflow"),   
#                ]
#            ) 
    p1 = figure(plot_width=1200, plot_height=400, tools=TOOLS,
                   title="Flow "+valve+" "+semname, x_axis_type = "datetime")
    p2 = figure(plot_width=1200, plot_height=400, tools=TOOLS,
                   title="Time To Flow "+valve+" "+semname, x_axis_type = "datetime", x_range = p1.x_range)
    p3 = figure(plot_width=1200, plot_height=400, tools=TOOLS,
                   title="Pressure Mean "+valve+" "+semname, x_axis_type = "datetime")
    p4 = figure(plot_width=1200, plot_height=400, tools=TOOLS,
                   title="PressureNumchange "+valve+" "+semname, x_axis_type = "datetime", x_range = p1.x_range)
    p5 = figure(plot_width=1200, plot_height=400, tools=TOOLS,
                   title="PressureMaxchange "+valve+" "+semname, x_axis_type = "datetime", x_range = p1.x_range)
    
    colorlist=['black','blue','yellow','red','green','cyan','magenta','black','blue']
    sortby=data['Action'].value_counts()
    for i in range(len(sortby)):
   
#        p1.circle('DT', 'flow', size=5,source=source)
  
        p1.circle(data[data['Action']==sortby.index[i]]['DT'], data[data['Action']==sortby.index[i]]['Flow'],\
                   legend=sortby.index[i],size=5, color=colorlist[i])
   
       
        
#        p2.circle('DT', 'timeflow', size=5,source=source)
        p2.circle(data[data['Action']==sortby.index[i]]['DT'], data[data['Action']==sortby.index[i]]['TimeOfFlow'],\
                   legend=sortby.index[i],size=5, color=colorlist[i])
   
        
        
#        p3.circle('DT', 'PressureMean', size=5,source=source)
        p3.circle(data[data['Action']==sortby.index[i]]['DT'], data[data['Action']==sortby.index[i]][features[4]],\
                   legend=sortby.index[i],size=5, color=colorlist[i])
   
     
        
#        p4.circle('DT', 'PressureNumchange', size=5,source=source)
        p4.circle(data[data['Action']==sortby.index[i]]['DT'], data[data['Action']==sortby.index[i]][features[5]],\
                   legend=sortby.index[i],size=5, color=colorlist[i])
        
        
#        p5.circle('DT', 'PressureMaxchange', size=5,source=source)
        p5.circle(data[data['Action']==sortby.index[i]]['DT'], data[data['Action']==sortby.index[i]][features[6]],\
                   legend=sortby.index[i],size=5, color=colorlist[i])
        # put all the plots in a VBox
        p = vplot(p1, p2, p3,p4,p5)

# show the results
#        show(p)

        
        save(p)
    
    #%%

#
#    
#%%
#plot_data(data,valve,'BlueA')
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
#funcTracking = pd.read_excel('G://cameron_BOP/Seadrill/function_tracking/Functions_Tracking_Rev5_RB.xlsx',sheetname = 'West_Eclipse',index_col = 0)
funcTracking = pd.read_excel('C://Users/tzhao/Documents/cameron_bop/BP/Thunderhorse/from Rajesh/Functions_Tracking_Rev5_RB_new.xlsx',sheetname = 'Thunderhorse',index_col = 0)
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
#dirNameNew = 'G:\\cameron_BOP\\Seadrill\\WestEclipseParsedOutput02\\WestEclipsetotalData\\subsea'
dirNameNew = 'G:\\cameron_BOP\\BP\\Thunderhorse\\fromsean\\OutputData'
for func_ind in range(len(funcTracking)):   # process the subsea functions
    
#    func_ind=0
    func = funcTracking['EventLogger_functions'].iloc[func_ind]
    valve=str(func)
    data = pd.read_csv(os.path.join(dirNameNew,func+'_julyafterclean3.csv'))
#    data = pd.read_csv(os.path.join(dirNameTemp,func+'_preprocess4s.csv'))
    data=data.dropna()   # drop the nan values
#    data['Action']=data['FromState']+"--->"+data['ToState'] # action decide how many clusters  
#    data=data.drop(data[data['Action'].str.contains('Both Diags On')].index)
#    data=data.drop(data[data['Action'].str.contains('Sol 1 Lock Sol 2 Lock')].index)
#    data=data[data.Active=='Active']  # only select the active ones

          
    data['DT'] = pd.to_datetime(data['DateTime'])
    
    # obtain how many pressure files in the function
    wellbore_files = funcTracking['Analogs'].iloc[func_ind].split('; ')

    
    features = []
   
    features=['Flow','TimeOfFlow'];
             
    data['Action1']=data['Action'].astype('category') 
    data['ActionNum'] = list(pd.Categorical.from_array(data['Action1']).codes)
    features.append('ActionNum')
    
    data['valve1']=data['Command'].astype('category')
    data['valveNum'] = list(pd.Categorical.from_array(data['valve1']).codes)
    features.append('valveNum')
    str1=str(str(funcTracking['Analogs'].iloc[func_ind]))+'_mean'
    features.append(str1)
    str1=str(str(funcTracking['Analogs'].iloc[func_ind]))+'_num_change'
    features.append(str1)
    str1=str(str(funcTracking['Analogs'].iloc[func_ind]))+'_max_change'
    features.append(str1)
    
    data_bluea= data[data['SEMName']=='Blue A']
    data_blueb= data[data['SEMName']=='Blue B'] 
    data_yellowa= data[data['SEMName']=='Yellow A'] 
    data_yellowb= data[data['SEMName']=='Yellow B'] 
    
#    if len(data_combluea) > 4:
#        data_bluea=ComupteKmeans(data_combluea,features)
    if len(data_bluea) > 0:
        plot_csvdata(data_bluea,valve,'BlueA',features)
  
#    if len(data_comblueb) > 4:    
#        data_blueb=ComupteKmeans(data_comblueb,features)
    if len(data_blueb) > 0: 
        plot_csvdata(data_blueb,valve,'BlueB',features)
            
#    if len(data_comyellowa) > 4:
#        data_yellowa=ComupteKmeans(data_comyellowa,features)
    if len(data_yellowa) > 0:
        plot_csvdata(data_yellowa,valve,'YellowA',features)
        
#    if len(data_comyellowb) > 4:
#        data_yellowb=ComupteKmeans(data_comyellowb,features)
    if len(data_yellowb) > 0:
        plot_csvdata(data_yellowb,valve,'YellowB',features)
    
    
        
    
        
    
    
    
    
    
#def ComupteKmeans(datain,features):
#    numOfCluster = len(datain['Action'].unique()) +1
#    
#    npData = datain.loc[:,features].values
#    for col in range(npData.shape[1]):     #normalize values
#        if npData[:,col].max(axis=0) == npData[:,col].min(axis=0):
#            pass
#        else:
#            npData[:,col] = (npData[:,col] - npData[:,col].min(axis=0))/(npData[:,col].max(axis=0)-npData[:,col].min(axis=0))
#    
#    kmeans = KMeans(n_clusters = numOfCluster)
#    kmeans = kmeans.fit(npData)
#    
#    cluster = kmeans.cluster_centers_
#    labels = kmeans.labels_
#    
#    datain['Anomaly'] = [np.mean((npData[ind,:].reshape(-1) - cluster[labels[ind],:])**2) for ind in range(len(labels))]
#    return datain
#%
#data = data.drop('ActionNum')

#%%

    
    #%%

   
    
    
    
    #%%
#    reset_output()
#    TOOLS = "pan,wheel_zoom,box_zoom,reset,save,box_select,lasso_select,resize,hover"
#    
#    output_file(os.path.join(r'G:\cameron_BOP\Seadrill\WestEclipseParsedOutput01\WestEclipseFlowData',
#                             '%s_kmeansAnomaly.html'%(valve[:-4])))
#    
#    
#    data['stringDate'] = [datetime.datetime.strftime(pd.to_datetime(dd), '%Y/%m/%d %H:%M:%S') for dd in data['DateTime'].values]
#    
#    source = ColumnDataSource(data = {'Index': data.index,
#                                     'Action': data['Action'],
#                                       'flow': data['Flow'],
#                                     'stringDate':data['stringDate'],
#                                      'DT':data['DT'],
#                                      'timeflow':data['TimeOfFlow'],
#                                        'Anomaly': data['Anomaly']})
#    
#    
#    p1 = figure(plot_width=800, plot_height=400, tools=TOOLS,
#               title="KMeans Anomaly for "+valve[:-4], x_axis_type = "datetime")
#    bp = p1.circle('DT', 'Anomaly', size=5, source=source)  
#    
#    hover = p1.select(dict(type=HoverTool))
#    hover[0].renderers = [bp]
#    hover[0].tooltips = [
#                        ("Action", "@Action"),
#                        ("Time", "@stringDate"),
#                        ("Flow", "@flow"),
#                        ("Time of Flow", "@timeflow"),
#                        ]
#    hover[0].mode = 'mouse'         
#    #p1.circle(data.DT,data.Anomaly, size=5)
#    save(p1)
##    show(p1)

