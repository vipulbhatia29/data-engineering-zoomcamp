#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table=params.table
    table2=params.table2
    parquetURL=params.parquetURL
    csvURL=params.csvURL
    parquetfile='output.parquet'
    csvfile='output.csv'


    os.system(f"wget {parquetURL} -O {parquetfile}")
    os.system(f"wget {csvURL} -O {csvfile}")
    df=pd.read_parquet(parquetfile)
    df1=pd.read_csv(csvfile)
    #df_iter=pd.read_parquet("yellow_tripdata_2021-01.parquet",iterator=True,chunksize=100000)
    """df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])"""
    engine=create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()
    #print(pd.io.sql.get_schema(df,name=table,con=engine))
    # create empty table
    print("we are there")
    #print(df.head())
    df.head(n=0).to_sql(name=table,con=engine,if_exists='replace')
    df1.head(n=0).to_sql(name=table2,con=engine,if_exists='replace')
    # create empty table
    print("we are here")
    print(df.to_sql(name=table,con=engine,if_exists='append',chunksize=10000))
    print(df1.to_sql(name=table2,con=engine,if_exists='append',chunksize=10000))


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Ingest Taxi data from parquet into postgres')
    parser.add_argument('--user',help='username for postgres')
    parser.add_argument('--password',help='password for postgres')
    parser.add_argument('--host',help='hostname for postgres')
    parser.add_argument('--port',help='port # for postgres')
    parser.add_argument('--db',help='db name for postgres')
    parser.add_argument('--table',help='table name for postgres')
    parser.add_argument('--parquetURL',help='url for the parquet file to ingest')
    parser.add_argument('--csvURL',help='url for the csv file to ingest')
    parser.add_argument('--table2',help='table name for the taxi zones lookup')
    args = parser.parse_args()
    main(args)