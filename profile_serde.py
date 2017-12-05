import pandas as pd
from time import time
from pyarrow.feather import write_feather, read_feather
import os
import tempfile
from timeit import timeit
from psutil import cpu_percent
import configparser
import click

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

MODES = ['parquet', 'pickle.gzip', 'parquet', 'msgpack', 'HD5', 'feather']

def make_df(rows=10**6):
    df = pd.DataFrame({'numbers': range(rows),
                       'strings': ['a']*rows})
    return df

def profile_synthetic_data():
    ROWS = [10**5, 10**6, 2*(10**6), 10**7]
    perf_df = pd.DataFrame(columns=['rows', 'mode', 'load_time', 'cpu_percent', 'size'])

    for n_rows in ROWS:
        df = make_df(rows=n_rows)
        for mode in MODES:
            with tempfile.NamedTemporaryFile() as f:
                dump(df, f.name, mode=mode)
                size = os.path.getsize(f.name)

                cpu_percent(interval=None)
                load_time = timeit(lambda: load(f.name, mode=mode), number=1)
                cpu_perc = cpu_percent(interval=None)
                perf_df = perf_df.append({'rows': n_rows, 'mode': mode, 'size': size,
                                          'load_time': load_time, 'cpu_percent': cpu_perc},
                                             ignore_index=True)


    perf_df.to_csv('data/performance.csv', index=False)

def profile_real_data():
    #load actual uri info from a config file to conceal where we keep our data.
    Config = configparser.ConfigParser()
    Config.read('.config.ini')
    for key, path in Config.items('where'):
        db_name, table_name = path.split('.')
        from r2d2 import get_df
        data_df = get_df(db_name, table_name)
        rows = len(data_df.index)
        cols = len(data_df.columns)
        real_perf_df = pd.DataFrame(columns=['dataset', 'mode', 'load_time', 'cpu_percent', 'size',
                                             'rows', 'cols'])
        for mode in MODES:
            with tempfile.NamedTemporaryFile() as f:
                dump(data_df, f.name, mode=mode)
                size = os.path.getsize(f.name)
                cpu_percent(interval=None)
                load_time = timeit(lambda: load(f.name, mode=mode), number=1)
                cpu_perc = cpu_percent(interval=None)
                real_perf_df = real_perf_df.append({'dataset': key, 'mode': mode,
                                                    'size': size, 'load_time': load_time,
                                                    'cpu_percent': cpu_perc, 'rows': rows,
                                                    'cols': cols}, ignore_index=True)

    real_perf_df.to_csv('data/real_performance.csv', index=False)

@click.command()
@click.option('--profile_synthetic', default=False, help='Only profile serdes on synthetic data.')
@click.option('--profile_real', default=False, help='Only profile serdes on real data.')
def main(profile_synthetic, profile_real):
    if (profile_real == False) & (profile_synthetic == False):
        main(True, True)
    if profile_synthetic:
        profile_synthetic_data()
    if profile_real:
        profile_real_data()


if __name__ == '__main__':
    main()