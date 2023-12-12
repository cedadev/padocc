import pandas as pd
import os, sys

def update_pd(ref, old, new):
    df = pd.read_parquet(ref)
    df['path'] = df['path'].str.replace(old, new)
    df.to_parquet(ref)
    print(ref)


def recursive_locate_refs(pq, old, new):
    files = os.listdir(pq)
    for f in files:
        if f.endswith('.parq'):
            update_pd(os.path.join(pq,f), old, new)
        elif not os.path.isfile(os.path.join(pq,f)):
            recursive_locate_refs(os.path.join(pq,f), old, new)
        else:
            pass

if __name__ == '__main__':
    pq = sys.argv[-3]
    old = sys.argv[-2]
    new = sys.argv[-1]

    recursive_locate_refs(pq, old, new)