import pandas as pd

PATH = '/home/users/dwest77/Documents/kerchunk_dev/parquet/dev'
raw = None
for x in range(0,76):
    df = pd.read_parquet(f'{PATH}/batch/esacci7_full/time/refs.{x}.parq')
    if not raw:
        raw = df['raw'][0]
    else:
        raw += df['raw'][0]

df.to_parquet(f'{PATH}/batch/esacci7_full/time/refs.0.parq')

#df.to_csv('time0.7.csv')