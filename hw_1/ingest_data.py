import argparse
import sys

from tqdm import tqdm
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

BATCH_WRITE_SIZE = 100000


def ingest_data(data, engine, table_name):
    num_data_rows = len(data.index)
    if num_data_rows > BATCH_WRITE_SIZE:
        df_chunks = np.array_split(data, num_data_rows / BATCH_WRITE_SIZE)
    else:
        df_chunks = [data]

    # Write header first:
    # (then check \d in pg...)
    df = df_chunks[0]
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Ingest all data
    with tqdm(total=num_data_rows) as pbar:
        for df in df_chunks:
            df.to_sql(name=table_name, con=engine, if_exists='append')
            pbar.update(len(df))


def create_engine_connect(username, password, hostname, port, db_name):
    engine = create_engine(
        f'postgresql://{username}:{password}@{hostname}:{port}/{db_name}'
    )
    engine.connect()

    return engine


def run_ingestion(url, engine, table_name):
    if url.endswith('.parquet'):
        data = pd.read_parquet(url, engine='pyarrow')
    elif url.endswith('.csv'):
        data = pd.read_csv(url)
    else:
        raise Exception('Unsupported data format received from URL')

    # Transform data types
    if 'tpep_pickup_datetime' in data:
        data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
        data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)

    ingest_data(data, engine, table_name)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Ingest parquet data from URL to new table in postgres DB.')
    parser.add_argument('--username', help='username in database')
    parser.add_argument('--password', help='password for user in database')
    parser.add_argument('--host', help='database hostname')
    parser.add_argument('--port', help='database connection port')
    parser.add_argument('--db', help='database name')
    parser.add_argument('--table', help='new table name')
    parser.add_argument('--url', help='URL of parquet data')

    return parser.parse_args()


def main():
    args = parse_args()
    engine = create_engine_connect(
        args.username,
        args.password,
        args.host,
        args.port,
        args.db
    )
    run_ingestion(url=args.url, engine=engine, table_name=args.table)


if __name__ == '__main__':
    main()
