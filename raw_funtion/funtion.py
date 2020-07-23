import pickle
import pandas as pd
from pandas.io import gbq
from datetime import datetime,timedelta
import time
import numpy as np
import utm
import math

with open("raw_funtion/rama4data", "rb") as fin:
    datarama4 = pickle.load(fin)
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
def distrelat1(lat,lon,node,road,direction='w',dataroad=datarama4,nodedata=nodedata1):
    raods=dataroad[direction]['allroad']
    dis=0
    for i in range(raods.index(road)):
        dis=dis+float(dataroad[direction][int(raods[i])]['len'])
    nodes=dataroad[direction][int(road)]['node']
    for j in range(len(nodes[0:nodes.index(node)])):
        datanodestart=dataroad[direction][int(road)]['nodedata'][int(nodes[j])]
        datanodestop=dataroad[direction][int(road)]['nodedata'][int(nodes[j+1])]
        utmstart=utm.from_latlon(datanodestart[0],datanodestart[1])
        utmstop=utm.from_latlon(datanodestop[0],datanodestop[1])
        dis1=((utmstart[0]-utmstop[0])**2+(utmstart[1]-utmstop[1])**2)**0.5
        dis=dis+dis1
    datanode=dataroad[direction][int(road)]['nodedata'][int(node)]
    utmdatanode=utm.from_latlon(datanode[0],datanode[1])
    utmpoint=utm.from_latlon(lat,lon)
    dis1=((utmdatanode[0]-utmpoint[0])**2+(utmdatanode[1]-utmpoint[1])**2)**0.5
    dis=dis+dis1
    return(dis)
def selecroad(data,roads):
    i=1
    for road in roads:
        if i==1 :
            dataall=data[data.wayids==road]
        else :
            datame=data[data.wayids==road]
            dataall=dataall.append(datame)
        i=i+1
    return(dataall)

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
                
                gdis =distrelat1(row['projectedlat'],row['projectedlng'],row['segmentstartnode'],row['wayids'],direction='w',dataroad=datarama4,nodedata=nodedata1)
                dist.append(gdis)
                delslope.append(row['speed'])
                
            else:
                gdis =distrelat1(row['projectedlat'],row['projectedlng'],row['segmentstartnode'],row['wayids'],direction='w',dataroad=datarama4,nodedata=nodedata1)
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


def raw2classdata(datafilter,rawdata,booking_inter,Llen,Rlen,ran):
    time=[]
    location=[]
    speed=[]
    boolkingid=[]
    alllen=[]
    exitno=[]
    latbe=[]
    lonbe=[]
    #Llen=[208631109 ,482209833,319130516,9168676,459492916,156966536,156966549,24829068,154773969]
    #Rlen=[208631105,481971013,158010303,40007275]
    for booking in booking_inter:
        car = datafilter[datafilter['bookingcode'] == booking].sort_values(by=['pingtimestamp'])
        carout=rawdata[rawdata['bookingcode'] == booking].sort_values(by=['pingtimestamp'])
        dist = []
        k= 0
        for j, row in car.iterrows():
            if k == 0:
                gdis =distrelat1(row['projectedlat'],row['projectedlng'],row['segmentstartnode'],row['wayids'])
                dist.append(gdis)
                speed.append(row['speed'])
                boolkingid.append(booking)
                time.append(datetime.fromtimestamp(row['pingtimestamp']))
            else:
                boolkingid.append(booking)
                gdis =distrelat1(row['projectedlat'],row['projectedlng'],row['segmentstartnode'],row['wayids'])
                dis = gdis-dist[-1]
                dist.append(gdis)
                deltime=row['pingtimestamp']-timemin
                speed.append(dis/deltime)
                time.append(datetime.fromtimestamp(row['pingtimestamp']))
            timemin=row['pingtimestamp']
            latbe.append(row['projectedlat'])
            lonbe.append(row['projectedlng'])
            k = k + 1
        location.extend(dist)
        wayidss=carout.wayids.unique()
        for wayid in wayidss :
            try:
                wa=int(wayid)
                if wa in Llen:
                    lens=['L']*len(dist)
                    break
                elif wa in Rlen:
                    lens=['R']*len(dist)
                    break
                else:
                    lens=['D']*len(dist)
            except:
                print(booking)
        #alllen.extend(lens)
        #ran=[0,995,1374,2957,3894,4062,4643,5616,6534]# add from traffic light 20m
        difran=[]
        for ranout in ran:
            difran.append(abs(dist[-1]-ranout))
        exitn=[ran[difran.index(min(difran))]-20]*len(dist)
        exitno.extend(exitn)
        trafexit=ran[ran.index(exitn[0]+20)-1]
        for num in range(len(dist)) :
            if dist[num] < trafexit :
                lens[num]='D'
        alllen.extend(lens)
        #print(booking)
        #print(lens)
    logfile=pd.DataFrame({'boolkingid':boolkingid,'timestamp': time,'lat':latbe,'lon':lonbe,'location': location,'speed':speed,'alllen':alllen,'exitno':exitno})
    logfile=logfile[logfile['speed']>0]
    return(logfile)


def line2latlon(dis,direction='w',dataroad=datarama4):
    coutdis=0
    roadpo=dataroad[direction]['allroad'][0]
    for road in dataroad[direction]['allroad']:
        coutdis+=dataroad[direction][road]['len']

        if coutdis>dis:
            coutdis-=dataroad[direction][road]['len']
            startnode=dataroad[direction][road]['node'][0]
            #startnode=datarama4['w'][road]['nodedata'][datarama4['w'][road]['node'][0]]
            for nodes in dataroad[direction][road]['node'][1:]:
                #stopnode=datarama4['w'][road]['nodedata'][nodes]
                stopnode=nodes
                startnodeutm=utm.from_latlon(dataroad[direction][road]['nodedata'][startnode][0],dataroad[direction][road]['nodedata'][startnode][1])
                stopnodeutm=utm.from_latlon(dataroad[direction][road]['nodedata'][stopnode][0],dataroad[direction][road]['nodedata'][stopnode][1])
                coutdis+=((startnodeutm[0]-stopnodeutm[0])**2+(startnodeutm[1]-stopnodeutm[1])**2)**0.5
                if coutdis>dis:
                    #print(coutdis)
                    coutdis-=((startnodeutm[0]-stopnodeutm[0])**2+(startnodeutm[1]-stopnodeutm[1])**2)**0.5
                    diff=dis-coutdis
                    R=np.arctan((startnodeutm[1]-stopnodeutm[1])/(startnodeutm[0]-stopnodeutm[0]))
                    x=startnodeutm[0]+diff*math.cos(R)
                    y=startnodeutm[1]+diff*math.sin(R)
                    point=utm.to_latlon(x,y,47,'P')
                    break
                else:
                    startnode=nodes
            break
        else:
            roadpo=road
    return(point,startnode,stopnode)


with open("raw_funtion/rama4nodesq", "rb") as fin :
       datarama4sq = pickle.load(fin)   


def latlon2dis(lat,lon,direction,datarama4=datarama4sq):
    point=utm.from_latlon(float(lat),float(lon))[0:2]
    nodes=datarama4[direction]['allnode']
    dis2node=[]
    for node in nodes:
        [latn,lonn]=datarama4[direction][node]
        pointnode=utm.from_latlon(float(latn),float(lonn))[0:2]
        dis2node.append(((point[0]-pointnode[0])**2+(point[1]-pointnode[1])**2)**0.5)
    m1, m2 = float('inf'), float('inf')
    for x in dis2node:
        if x <= m1:
            m1, m2 = x, m1
        elif x < m2:
            m2 = x
    indexnode=min([dis2node.index(m1),dis2node.index(m2)])
    startnd=nodes[indexnode]
    stopnd=nodes[max([dis2node.index(m1),dis2node.index(m2)])]
    print(startnd,stopnd)
    fnode=datarama4[direction][nodes[0]]
    distant=0
    if startnd==nodes[0]:
        fnodeutm=utm.from_latlon(float(fnode[0]),float(fnode[1]))[0:2]
        distant=(((point[0]-fnodeutm[0])**2+(point[1]-fnodeutm[1])**2)**0.5)
    else:
        for node in nodes[1:]:
            if int(node)==int(startnd):
                snodeutm=utm.from_latlon(float(snode[0]),float(snode[1]))[0:2]
                distant+=(((point[0]-snodeutm[0])**2+(point[1]-snodeutm[1])**2)**0.5)
                break
            else:
                snode=datarama4[direction][node]
                fnodeutm=utm.from_latlon(float(fnode[0]),float(fnode[1]))[0:2]
                snodeutm=utm.from_latlon(float(snode[0]),float(snode[1]))[0:2]
                distant+=(((fnodeutm[0]-snodeutm[0])**2+(fnodeutm[1]-snodeutm[1])**2)**0.5)
                fnode=snode
    return(distant,startnd,stopnd)
def dis2latlonv2(distant,direction='w',datarama4=datarama4sq):
    nodes=datarama4[direction]['allnode']
    n1node=[]
    dis=0
    stnode=datarama4[direction][nodes[0]]
    stnodeid=nodes[0]
    for node in nodes[1:]:
        spnode=datarama4[direction][node]
        spnodeid=node
        stnodeutm=utm.from_latlon(float(stnode[0]),float(stnode[1]))
        spnodeutm=utm.from_latlon(float(spnode[0]),float(spnode[1]))
        dis+=((stnodeutm[0]-spnodeutm[0])**2+(stnodeutm[1]-spnodeutm[1])**2)**0.5
        if dis>=distant:
            diff=distant-dis+((stnodeutm[0]-spnodeutm[0])**2+(stnodeutm[1]-spnodeutm[1])**2)**0.5
            break
        stnode=spnode
        stnodeid=spnodeid
    x=stnodeutm[0]+diff*(spnodeutm[0]-stnodeutm[0])/diff
    y=stnodeutm[1]+diff*(spnodeutm[1]-stnodeutm[1])/diff
    point=utm.to_latlon(x,y,47,'P')
    return(([point[0],point[1]],stnodeid,spnodeid))


def latlon2disitic(lat,lon,direction='w',datarama4=datarama4sq):
    if lon<100.5929 and lat>13.7123 and lat< 13.7454 and lon>100.5057:    
        point=utm.from_latlon(float(lat),float(lon))[0:2]
        nodes=datarama4[direction]['allnode']
        dis2node=[]
        for node in nodes:
            [latn,lonn]=datarama4[direction][node]
            pointnode=utm.from_latlon(float(latn),float(lonn))[0:2]
            dis2node.append(((point[0]-pointnode[0])**2+(point[1]-pointnode[1])**2)**0.5)
        m1, m2 = float('inf'), float('inf')
        for x in dis2node:
            if x <= m1:
                m1, m2 = x, m1
            elif x < m2:
                m2 = x
        indexnode=min([dis2node.index(m1),dis2node.index(m2)])
        startnd=nodes[indexnode]
        stopnd=nodes[max([dis2node.index(m1),dis2node.index(m2)])]
        pointstartndoe=utm.from_latlon(datarama4[direction][startnd][0],datarama4[direction][startnd][1])[0:2]
        pointstotndoe=utm.from_latlon(datarama4[direction][stopnd][0],datarama4[direction][stopnd][1])[0:2]
        disbtwnode=(((pointstartndoe[0]-pointstotndoe[0])**2+(pointstartndoe[1]-pointstotndoe[1])**2)**0.5)
        startdis=(((pointstartndoe[0]-point[0])**2+(pointstartndoe[1]-point[1])**2)**0.5)
        stopdis=(((point[0]-pointstotndoe[0])**2+(point[1]-pointstotndoe[1])**2)**0.5)
        if startdis+stopdis-30< disbtwnode:
            logic=True
        else:
            logic=False
    else:
        logic=False
    return(logic)