
from src.data import create_parquet_dataset, run_duckdb_test
from src.logger import get_logger

logger = get_logger()



def run():
    
    logger.info('Starting data ingestion')
    
    TABLES = [
    'articles',
    'customers',
    'transactions_train'
    ]
    
    logger.info('Creating parquet files from raw data')
    create_parquet_dataset(TABLES)
    
    logger.info('Running tests on DuckDB')
    run_duckdb_test()

    # start a job to insert the data into the feature group
    logger.info('All done\n\nSee you, space cowboy\n')

if __name__ == '__main__':
    run()
    