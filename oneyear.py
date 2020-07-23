import multiprocessing 
import time
import pandas as pd
import os 
from raw_funtion import funtion
from os import listdir
from os.path import isfile, join 
from geopy.distance import great_circle
from geopy.geocoders import Nominatim

def pocesslocation(idcar,data):
    dataselec=data[data.VehicleID==idcar]
    a=dataselec[dataselec.timestamp == dataselec.timestamp.max()]
    b=dataselec[dataselec.timestamp == dataselec.timestamp.min()]
    dis=great_circle((a.lat.values[0],a.lon.values[0]),(b.lat.values[0],b.lon.values[0])).meters
    if dis>400:
        idcar=[idcar,b.lat.values[0],b.lon.values[0],a.lat.values[0],a.lon.values[0]]
    else:
        idcar=[0,0,0,0,0]    
    return idcar
no=1
mypath='D:/PROBE-201909'
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))] 
data1=pd.read_csv(mypath+'/'+onlyfiles[no],names=['VehicleID','gpsvalid','lat','lon','timestamp','speed','heading','for_hire_light','engine_acc'])    
idcar=data1.VehicleID.unique().tolist()
outputpath='D:/New folder (3)'
pool = multiprocessing.Pool(processes=4) 
outputs_async = pool.map_async(pocesslocation,idcar) 
outputs = outputs_async.get()
df = pd.DataFrame(outputs, columns =['ids','latstartl','lonstartl', 'latstop','lonstop'])
df=df[df.start!=0]  
df.to_csv(outputpath+'/'+onlyfiles[no])