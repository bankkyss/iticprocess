import folium
import folium.plugins as plugins
import numpy as np
import pandas as pd
#mypath='C:/Users/supan/Desktop/test/test'
mypath=input('path :')
out=input('namefileoutput :')
wat=input('size :')
opacity=input('opacity :')
typesmap=input('Stamen Terrain :')
from os import listdir
from os.path import isfile, join
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
alldata=pd.read_csv(mypath+'/'+onlyfiles[0])
for name in onlyfiles[1:]:
    print(name)
    data=pd.read_csv(mypath+'/'+name)
    alldata=pd.concat([alldata,data], ignore_index=True, sort=False)
alldata['timestop'] = pd.to_datetime(alldata['timestop'])
alldata['timestart'] = pd.to_datetime(alldata['timestart'])
alldata['timestopnum'] = alldata['timestop'].dt.hour+alldata['timestop'].dt.minute/60
alldata['timestartnum'] =alldata['timestart'].dt.hour+alldata['timestart'].dt.minute/60
start=[0]

while start[-1] < 23.75:
    start.append(start[-1]+15/60)
final=[]
for j in range(1,len(start)):
    datase=alldata[(alldata.timestartnum<start[j]) & (alldata.timestartnum>start[j-1])]
    timedata=[]
    for j , row in datase.iterrows():
        timedata.append([row['latstop'],row['lonstop'],wat])
    final.append(timedata)
from datetime import datetime, timedelta
time_index = [
    (datetime(1996, 9, 19) + k * timedelta(minutes=15)).strftime('%H:%M:%S') for
    k in range(24*4)
]
m = folium.Map([13.737797, 100.559699], tiles=typesmap, zoom_start=12)

hm = plugins.HeatMapWithTime(
    final,
    index=time_index[1:],
    auto_play=True,
    max_opacity=opacity
)
hm.add_to(m)
m.save(out+".html")
