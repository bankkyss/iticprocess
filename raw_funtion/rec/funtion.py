import json
import pandas as pd
from pandas.io import gbq
from datetime import datetime,timedelta
import time
import numpy as np
import utm
import math

with open("raw_funtion/rama4data.json", "rb") as fin:
    datarama4 = json.load(fin)
def seclec(data, timestart, timestop, condition='', con=''):
    dstrt = timestart
    dstop = timestop
    if type(timestart) == str:
        dstrt = datetime.strptime(timestart, '%m/%d/%y %H:%M:%S')  # "09/19/18 13:55:26"
        dstop = datetime.strptime(timestop, '%m/%d/%y %H:%M:%S')  # "09/19/18 13:55:26"
    # print(dstrt)
    start = time.mktime(dstrt.timetuple())
    stop = time.mktime(dstop.timetuple())
    s = data[data['pingtimestamp'] >= start]
    s = s[s['pingtimestamp'] <= stop]
    if condition != '':
        s = s[s[condition] == con]
    return (s)


nodedata1=pd.read_csv('raw_funtion/nodedata.csv') 
def distrelat1(lat,lon,node,road,dataroad=datarama4,nodedata=nodedata1):
    raods=dataroad['allroad']
    dis=0
    for i in range(raods.index(road)):
        dis=dis+float(dataroad[str(raods[i])]['len'])
    nodes=dataroad[str(road)]['node']
    for j in range(len(nodes[0:nodes.index(node)])):
        datanodestart=dataroad[str(road)]['nodedata'][str(nodes[j])]
        datanodestop=dataroad[str(road)]['nodedata'][str(nodes[j+1])]
        utmstart=utm.from_latlon(datanodestart[0],datanodestart[1])
        utmstop=utm.from_latlon(datanodestop[0],datanodestop[1])
        dis1=((utmstart[0]-utmstop[0])**2+(utmstart[1]-utmstop[1])**2)**0.5
        dis=dis+dis1
    datanode=dataroad[str(road)]['nodedata'][str(node)]
    utmdatanode=utm.from_latlon(datanode[0],datanode[1])
    utmpoint=utm.from_latlon(lat,lon)
    dis1=((utmdatanode[0]-utmpoint[0])**2+(utmdatanode[1]-utmpoint[1])**2)**0.5
    dis=dis+dis1
    return(dis)


def selecroadtime(data,timed,roads):
    i=1
    for road in roads:
        if i==1 :
            dataall=seclec(data,timed,timed+timedelta(days=1),'wayids',road)
        else :
            datame=seclec(data,timed,timed+timedelta(days=1),'wayids',road)
            dataall=dataall.append(datame)
        i=i+1
    return(dataall)


def findallbooking(data):
    bookingcodes=data.bookingcode.unique()
    return(bookingcodes)

    
def findstopposition(bookcode,selecter):
    timestop=[]
    locationstop=[]
    delslope=[]
    for i in bookcode:
        car = selecter[selecter['bookingcode'] == i].sort_values(by=['pingtimestamp'])
        dist = []
        k = 0
        for j, row in car.iterrows():
            if k == 0:
                gdis =distrelat1(row['projectedlat'],row['projectedlng'],row['segmentstartnode'],row['wayids'],datarama4)
                dist.append(gdis)
                delslope.append(row['speed'])
                
            else:
                gdis =distrelat1(row['projectedlat'],row['projectedlng'],row['segmentstartnode'],row['wayids'],datarama4)
                dis = gdis-dist[-1]
                dist.append(gdis)
                deltime=row['pingtimestamp']-timemin
                if dis >0 and deltime !=0 :
                    if (dis/deltime < 1)  :
                        locationstop.append(gdis)
                        timestop.append(datetime.fromtimestamp(row['pingtimestamp']))
            timemin=row['pingtimestamp']
            latbefor = row['projectedlat']
            lonbefor = row['projectedlng']
            timemin=row['pingtimestamp']
            k = k + 1
    datadel=pd.DataFrame({'time':timestop,'position':locationstop})
    return(datadel)

    
def selectimedatafromgbq(timestart,timestop,condition=''):  
    dstrt=timestart
    dstop=timestop

    if type(timestart) == str :
        dstrt=datetime.strptime(timestart,'%m/%d/%y %H:%M:%S')#"09/19/18 13:55:26"
        dstop=datetime.strptime(timestop,'%m/%d/%y %H:%M:%S')#"09/19/18 13:55:26"
    start=time.mktime(dstrt.timetuple())
    stop=time.mktime(dstop.timetuple())
    if dstrt.date()==dstop.date():
        sql = "SELECT * FROM `data-for-grab.grab1."+dstrt.strftime("%Y_%m_%d")+"` WHERE pingtimestamp > "+str(start)+" AND pingtimestamp < "+str(stop)+condition
        print(sql)
        #query = "SELECT * FROM [dbo].["+dstrt.strftime("%Y_%m_%d")+"] WHERE pingtimestamp > "+str(start)+" AND pingtimestamp < "+str(stop)+condition
        df=gbq.read_gbq(sql,project_id='data-for-grab')
    return df
