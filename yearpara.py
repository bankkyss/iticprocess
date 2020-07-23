from functools import partial
import multiprocessing 
import time
import pandas as pd
import os 
from os import listdir
from os.path import isfile, join 
from geopy.distance import great_circle

def pocesslocation(data,idcar):
    dataselec=data[data.VehicleID==idcar]
    a=dataselec[dataselec.timestamp == dataselec.timestamp.max()]
    b=dataselec[dataselec.timestamp == dataselec.timestamp.min()]
    dis=great_circle((a.lat.values[0],a.lon.values[0]),(b.lat.values[0],b.lon.values[0])).meters
    if dis>400:
        out=[idcar,b.lat.values[0],b.lon.values[0],a.lat.values[0],a.lon.values[0]]
    else:
        out=[0,0,0,0,0] 
    return out

def main(mypath,outputpath):
    core=15
    #core=1
    #print(core)
    #mypath='D:/PROBE-201909'
    #outputpath='D:/New folder (3)'
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))] 
    for name in onlyfiles [1:]:
        print(name)
        data1=pd.read_csv(mypath+'/'+name,names=['VehicleID','gpsvalid','lat','lon','timestamp','speed','heading','for_hire_light','engine_acc'])  
        idcar=data1.VehicleID.unique().tolist()
        pool = multiprocessing.Pool(processes=core) 
        func = partial(pocesslocation,data1)
        outputs =pool.map(func,idcar)
        df = pd.DataFrame(outputs, columns =['ids','latstartl','lonstartl', 'latstop','lonstop'])
        df=df[df.ids!=0]  
        df.to_csv(outputpath+'/'+name)
        pool.close()
        pool.join()

if __name__ == "__main__":
    mypath=input('pathin')
    outputpath=input('pathout')
    main(mypath,outputpath)
