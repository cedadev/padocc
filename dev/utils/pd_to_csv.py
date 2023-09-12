import pandas as pd

df = pd.read_parquet('/home/users/dwest77/Documents/kerchunk_dev/kerchunk-builder/test_parqs/parqs/esacci25/freeboard/refs.0.parq')

df.to_csv('cache.csv')