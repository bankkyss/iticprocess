import numpy as np
from multiprocessing import  Pool
import pandas as pd

#Some example dataframe
np.random.seed(4)
layer = pd.DataFrame(np.random.randint(0,25,size=(10000, 4)),
    columns=list(['basalareap','notofinterest', 'basalareas', 'basalaread']))

###Filter Fields by selecting columns of interest
fields = ["basalareap","basalareas","basalaread"]

#Check pandas syntax online if you have doubt on how to use it. This is how you 
#keep only a list of columns.
layer = layer[fields]

def denom(df):
    df['denominator'] = df[["basalareap","basalareas","basalaread"]].sum(axis=1)
    #You need to return a dataframe for use in you parallelize function below
    return df


def parallelize_dataframe(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)

    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))

    pool.close()
    pool.join()

    return df

#In windows you need to insert a guard in the main module to avoid creating 
#subprocesses recursively
if __name__ == '__main__':
    test = parallelize_dataframe(df=layer, func=denom)

    print(test.head())