from raw_funtion import funtion
import pandas as pd
data=pd.read_csv("20190807.csv.out",names=['VehicleID','gpsvalid','lat','lon','timestamp','speed','heading','for_hire_light','engine_acc'])



import timeit

start = timeit.timeit()
data['logic']=data.apply(lambda x: funtion.latlon2disitic(x['lat'], x['lon']), axis=1)
end = timeit.timeit()
print(end - start)


#sh=data.shape[0]
#logic=[]
#for j, row in data.iterrows():
#    print(int(len(logic)/sh*1000))
#    logic.append(funtion.latlon2disitic(row['lat'], row['lon']))
#data['logic']=logic
data2=data[data['logic']==True]
data2.to_csv('select.csv')