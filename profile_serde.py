import pandas as pd
from time import time
from pyarrow.feather import write_feather, read_feather
import os
import tempfile
from timeit import timeit
import psutil


def dump(df, file_name, mode=None):
    if mode == 'HD5':
        df.to_hdf(file_name, 'dataframe')
    elif mode == 'msgpack':
        df.to_msgpack(file_name)
    elif mode == 'parquet':
        df.to_parquet(file_name, engine='pyarrow') # defaults to snappy compression
    elif mode == 'pickle.gzip':
        df.to_pickle(file_name, compression='gzip')
    elif mode == 'feather':
        write_feather(df, file_name)


def load(file_name, mode=None):
    if mode == 'HD5':
        df = pd.read_hdf(file_name, 'dataframe')
    elif mode == 'msgpack':
        pd.read_msgpack(file_name)
    elif mode == 'parquet':
        df = pd.read_parquet(file_name, engine='pyarrow', use_pandas_metadata=True)
    elif mode == 'pickle.gzip':
        df = pd.read_pickle(file_name, compression='gzip')
    elif mode == 'feather':
        read_feather(file_name)

def make_df(rows=10**6):
    df = pd.DataFrame({'numbers': range(rows),
                       'strings': ['a']*rows})
    return df


# had to install pytables for hd5 fies
MODES = ['parquet', 'msgpack', 'HD5', 'pickle.gzip', 'feather']
ROWS = [10**5, 10**7]

perf_df = pd.DataFrame(columns=['rows', 'mode', 'action', 'time'])
size_df = pd.DataFrame(columns=['rows', 'mode', 'size'])

for n_rows in ROWS:
    df = make_df(rows=n_rows)
    for mode in MODES:
        with tempfile.NamedTemporaryFile() as f:
            dump_time = timeit(lambda: dump(df, f.name, mode=mode), number=3)
            #get cpu percent util since last call
            perf_df = perf_df.append({'rows': n_rows, 'mode': mode, 'action': 'dumps',
                                          'time': dump_time},
                                         ignore_index=True)
            size = os.path.getsize(f.name)
            size_df = size_df.append({'rows': n_rows, 'mode': mode, 'size': size},
                                     ignore_index=True)
            load_time = timeit(lambda: load(f.name, mode=mode), number=3)
            perf_df = perf_df.append({'rows': n_rows, 'mode': mode, 'action': 'loads',
                                          'time': load_time},
                                         ignore_index=True)


perf_df.to_csv('data/performance.csv', index=False)
size_df.to_csv('data/timing.csv', index=False)



