import pandas as pd

df = pd.read_parquet('/gws/nopw/j04/esacci_portal/kerchunk/parq/ocean_daily_all_parts/batch155/adg_412/refs.0.parq')

df.to_csv('cache.csv')