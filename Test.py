import pandas as pd
from datetime import timedelta
from joblib import Parallel, delayed
import multiprocessing
import time

data = pd.read_csv('~/PycharmProjects/HunterDouglas/QualityData.csv',sep=',', encoding='latin-1', error_bad_lines=False, dtype = {'ORIGINAL_ORDER':str}, parse_dates = ['SO_CREATED_DATE'])

### Remove rows where order is NA
df = data[pd.notnull(data['ORIGINAL_ORDER'])]

#### Sort values by:
df = df.sort_values(['ORIGINAL_ORDER', 'SO_CREATED_DATE'])

### Subset rows
df = df.head(100000)

### Need to reset index after sorting values
df = df.reset_index(drop = True)



### Filter date using split-apply-combine (pandas)
def filter_date(temp):
    temp = temp.reset_index(drop = True)
    temp = temp[temp['SO_CREATED_DATE'] <= temp.loc[0,'SO_CREATED_DATE'] + timedelta(days=90) ]
    return temp

def applyParallel(dfGrouped, func):
    retLst = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)



start = time.time()
data_final = df.groupby('ORIGINAL_ORDER').apply(lambda x: filter_date(x))
end = time.time()
print("Execution time normal :" + str(end - start))


start = time.time()
parallel = applyParallel(df.groupby('ORIGINAL_ORDER'), filter_date)
end = time.time()
print("Execution time parallel :" + str(end - start))

