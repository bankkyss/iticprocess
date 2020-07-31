from functools import partial
import multiprocessing 
import time
import pandas as pd
import os 
#from raw_funtion import funtion
from os import listdir
from os.path import isfile, join 
from geopy.distance import great_circle

def pocesslocation(data,ids):
    selecter=data1[(data1.VehicleID==ids) & (data1.for_hire_light==1) ]
    car = selecter.sort_values(by=['timestampns'])
    countrow=1
    for j, row in car.iterrows():
        if countrow ==1:
            starttime=row['timestampns']
            beforetime=row['timestampns']
            latstartl=row['lat']
            lonstartl=row['lon']
            countrow+=1
        else:
            diff=row['timestampns']-beforetime
            if diff.seconds<240:
                latstop=row['lat']
                lonstop=row['lon']                        
                beforetime=row['timestampns']
            elif diff.seconds>240:
                output.append([latstartl,lonstartl,starttime,latstop,lonstop,beforetime])
                starttime=row['timestampns']
                beforetime=row['timestampns']
                latstartl=row['lat']
                lonstartl=row['lon']
                return output

def main():
    mypath='D:/PROBE-201909'
    outputpath='D:/New folder (4)'
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))] 
    for name in onlyfiles [1:]:
        data1=pd.read_csv(mypath+'/'+name,names=['VehicleID','gpsvalid','lat','lon','timestamp','speed','heading','for_hire_light','engine_acc'])  
        data1['timestampns']=pd.to_datetime(data1['timestamp'])
        idcar=data1.VehicleID.unique().tolist()
        pool = multiprocessing.Pool(processes=4) 
        func = partial(pocesslocation,data1)
        outputs =pool.map(func,idcar)
        df = pd.DataFrame(outputs, columns =['latstartl','lonstartl','timestart','latstop','lonstop','timestop'])
        df.to_csv(outputpath+'/'+name)
        pool.close()
        pool.join()

if __name__ == "__main__":
    main()