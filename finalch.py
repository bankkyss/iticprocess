import multiprocessing 
import time
import pandas as pd
import os 
from raw_funtion import funtion
from os import listdir
from os.path import isfile, join 
from geopy.distance import great_circle
from geopy.geocoders import Nominatim

    



outputpath='D:/position/8'
mypath='D:/PROBE-201908'
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))] 

for name in onlyfiles[6:14]:
    data1=pd.read_csv(mypath+'/'+name,names=['VehicleID','gpsvalid','lat','lon','timestamp','speed','heading','for_hire_light','engine_acc'])
    idcar=data1.VehicleID.unique().tolist()
        def pocesslocation(idcar,data=data1):
        dataselec=data[data.VehicleID==idcar]
        a=dataselec[dataselec.timestamp == dataselec.timestamp.max()]
        b=dataselec[dataselec.timestamp == dataselec.timestamp.min()]
        dis=great_circle((a.lat.values[0],a.lon.values[0]),(b.lat.values[0],b.lon.values[0])).meters
        if dis>400:
            try:
                geolocator = Nominatim(timeout=100000)
                city1=geolocator.reverse(str(a.lat.values[0])+', '+str(a.lon.values[0]))[0].split(', ')
                for ist in city1:
                    if ist.isdigit():
                        stt=ist
                city2=geolocator.reverse(str(b.lat.values[0])+', '+str(b.lon.values[0]))[0].split(', ')
                for isp in city2:
                    if isp.isdigit():
                        stp=isp
                if (int(stt) in postidd) and (int(stp) in postidd):
                    start=stt
                    stop=stp
                else:
                    start=0
                    stop=0
                idcar=[start,stop]                
            except:
                start=0
                stop=0
                idcar=[start,stop]
        else:
            idcar=[0,0]    
        return idcar
    postid=pd.read_csv('idbankok.csv')
    postidd=postid.รหัสไปรษณีย์.values.tolist()  
    pool = multiprocessing.Pool(processes=4) 
    outputs_async = pool.map_async(pocesslocation,idcar) 
    outputs = outputs_async.get()
    df = pd.DataFrame(outputs, columns =['start', 'stop'])
    df=df[df.start!=0]  
    df.to_csv(outputpath+'/'+name)