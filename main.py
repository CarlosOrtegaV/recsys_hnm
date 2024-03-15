import argparse
from scripts.data_ingestion import run as data_ingestion_run

def main():
    parser = argparse.ArgumentParser(description='Run different scripts based on CLI arguments')
    parser.add_argument('script', choices=['data_ingestion', 'another_script'], help='The script to run')
    args = parser.parse_args()

    if args.script == 'data_ingestion':
        data_ingestion_run()
    elif args.script == 'dbt_transformation':
        
        # Call another script function here
        pass
    else:
        print('Invalid script name')


if __name__ == '__main__':
    main()
