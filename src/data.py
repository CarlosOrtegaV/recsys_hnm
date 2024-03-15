
import time
import pandas as pd
from typing import List
import duckdb
from pathlib import Path

from src.paths import DATA_DIR

def create_parquet_dataset(tables: List[str])->None:
    for table in tables:
        path_csv = Path(DATA_DIR / '{}.csv'.format(table))
        path_parquet = Path(DATA_DIR / '{}.parquet'.format(table))

        _df = pd.read_csv(
            path_csv, 
            on_bad_lines='skip'
        )
        # show the df
        print(_df.head())
        print(_df.dtypes)
        # print the lenght
        print("Total rows for {}: {}\n\n".format(table, len(_df)))
        # dump to parquet (better than csv for duckdb)
        _df.to_parquet(path_parquet)

    return
    

def run_duckdb_test()->None:
    # query counts for a few days, months, year
    start = time.time()
    con = duckdb.connect()
    # query different time spans
    end_dates = ['2018-09-22', '2018-10-20', '2019-10-22']
    for d in end_dates:
        print(duckdb.query('''
            SELECT 
                COUNT(customer_id)
            FROM 
                read_parquet('data/transactions_train.parquet')
            WHERE 
                t_dat BETWEEN '2018-09-20' AND '2018-9-22'
        ''').fetchall())
        print("Elapsed time (s) for {}: {}".format(d, time.time() - start))

    return