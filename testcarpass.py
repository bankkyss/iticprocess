from functools import partial
import multiprocessing 
import pandas as pd
import os 
from os import listdir
from os.path import isfile, join 
from geopy.distance import great_circle


def checkrama4(lat,lon):
    loref=[(13.714316,100.592477),(13.712731,100.589730),(13.713065,100.580975),(13.719719,100.565197),(13.718896,100.562440),(13.738243,100.515321)]
    logic=False
    for i in range(1,len(loref)):
        try:
            if great_circle(loref[i],loref[i-1]).meters+100>great_circle((lat,lon),loref[i-1]).meters+great_circle(loref[i],(lat,lon)).meters:
                logic=True
        except:
            print('error')
    return logic
def main(mypath,outputpath):
    #mypath='D:/PROBE-201909'
    #outputpath='D:/New folder (3)'
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))] 
    for name in onlyfiles :
            data1=pd.read_csv(mypath+'/'+name,names=['VehicleID','gpsvalid','lat','lon','timestamp','speed','heading','for_hire_light','engine_acc'])  
            data1['timestampns']=pd.to_datetime(data1['timestamp'])
            idcar=data1.VehicleID.unique().tolist()
            output=[]
            a=1
            for ids in idcar:
                print(a)
                a+=1
                selecter=data1[(data1.VehicleID==ids) & (data1.for_hire_light==1) ]
                car = selecter.sort_values(by=['timestampns'])
                countrow=1
                inrama4=False
                for j , row in car.iterrows():
                    if countrow ==1:
                        starttime=row['timestampns']
                        beforetime=row['timestampns']
                        latstartl=row['lat']
                        lonstartl=row['lon']
                        countrow+=1
                        if checkrama4(row['lat'],row['lon']):
                            inrama4=True
                    else:
                        if checkrama4(row['lat'],row['lon']):
                            inrama4=True
                        diff=row['timestampns']-beforetime
                        if diff.seconds<240:
                            latstop=row['lat']
                            lonstop=row['lon']                        
                            beforetime=row['timestampns']
                        elif diff.seconds>240:
                            if(beforetime-starttime).seconds<10800:
                                output.append([latstartl,lonstartl,starttime,latstop,lonstop,beforetime,inrama4])
                                inrama4=False
                            starttime=row['timestampns']
                            beforetime=row['timestampns']
                            latstartl=row['lat']
                            lonstartl=row['lon']


                            
            df = pd.DataFrame(output, columns =['latstartl','lonstartl','timestart','latstop','lonstop','timestop','rama4status'])
            dffinal=df[df.rama4status==True]
            dffinal.to_csv(outputpath+'/'+name)


if __name__ == "__main__":
    mypath=input('pathin :')
    outputpath=input('pathout :')
    main(mypath,outputpath)