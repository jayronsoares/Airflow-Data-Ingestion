#!/usr/bin/env python
# coding: utf-8

# Cleanup: Resolves data inconsistencies and missing values</li>
# Normalization: Formatting rules are applied to the dataset</li>
# Deduplication: Redundant data is excluded or removed</li>
# Verification: Unusable data is deleted and anomalies are reported</li>
# Sort: The data is sorted by type</li>


import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine, select, MetaData, Table, and_, func, case, text
from pandas.io import sql
from pathlib import Path
import datetime
from datetime import datetime
import numpy as np
import psycopg2
import s3fs
import pyarrow.parquet as pq
import boto3
import warnings
import re
import os
from unidecode import unidecode
import logging
import itertools

import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

warnings.filterwarnings("ignore")
s3 = s3fs.S3FileSystem(anon=False, key='AKIAXSTJGBKSOBDBYVGZ', secret='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')

###########################################
# ## S3 Bucket Setup & Parameters ##
###########################################

organization_id = 'company'
s3_bucket = 'bucket-data'
filename='filename.xlsx'

###########################################
## database parameters
###########################################

PSQL_DB = os.getenv("PSQL_DB", "stagging_db")
PSQL_USER = os.getenv("PSQL_USER", "master")
PSQL_PASSWORD = os.getenv("PSQL_PASSWORD", "passord")
PSQL_PORT = os.getenv("PSQL_PORT", "5432")
PSQL_HOST = os.getenv("PSQL_HOST", "code.us-east-1.rds.amazonaws.com")

schemaname= 'stg_schema'
tablename= 'stg_table'

###########################################
# ## Read csv | xlsx files ##
###########################################

df = pd.read_excel(s3.open(f'{s3_bucket}/business_cases/{organization_id}/INPUTS/raw/{filename}',index_col=None, engine='openpyxl', skiprows=0, header=2, date_parser=pd.datetime))

# ## Fix header columns ##

def fix_header(df):
    df.columns = df.columns.map(str)
    return df


def fix_unicode(df):
    df.columns = [unidecode(x) for x in df.columns.str.lower()]
    df.columns = df.columns.map(unidecode).str.lower()
    return df

# ## Remove special characters ##

def rem_special_char(df):
	df_non_numeric = df.select_dtypes(exclude=[np.number])
	non_numeric_cols = df_non_numeric.columns.values
	special_char = ['"', '*', '/', '(', ')', ':', '\n', '#', "$", ' ', '\t']
	special_char_escaped = list(map(re.escape, special_char))
	for col in non_numeric_cols:
		df[col] =df[col].replace(special_char_escaped,'', regex=True)
	return df

############################################################################
# ## Memory usage reduction ##
############################################################################

# This function is used to reduce memory of a pandas dataframe
# The idea is cast the numeric type to another more memory-effective type

def reduce_mem_usage(df):
    """ iterate through all the columns of a dataframe and modify the data type
        to reduce memory usage.        
    """
    start_mem = df.memory_usage().sum() / 1024**2
    print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))
    
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type != object and col_type.name != 'category' and 'datetime' not in col_type.name:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        elif 'datetime' not in col_type.name:
            df[col] = df[col].astype('category')

    end_mem = df.memory_usage().sum() / 1024**2
    print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
    print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))

    return df

############################################################
## checking for duplicates values in large datasets
############################################################
# remove duplicates rows

df= df[~df.duplicated()]

# remove duplicates columns
df = df.T.drop_duplicates().T

# Verify missing values

def chk_missing_values(df):
    mis_val = df.isnull().sum()
    mis_val_percent = 100 * df.isnull().sum() / len(df)
    df_mz = pd.concat([mis_val, mis_val_percent], axis=1)
    df_mz = df_mz.rename(columns = {0 : 'Missing Values', 1 : '% of Total Values'})
    df_mz['Total Missing Values'] = df_mz['Missing Values']
    df_mz['% Total Missing Values'] = 100 * df_mz['Total Missing Values'] / len(df)
    df_mz['Data Type'] = df.dtypes
    df_mz = df_mz[df_mz.iloc[:,1] != 0].sort_values('% of Total Values', ascending=False).round(1)
    print ("\nThe dataframe has " + str(df.shape[1]) + " columns and " + str(df.shape[0]) + " Rows.\n"
           "\nThere are " + str(df_mz.shape[0]) +" columns that have missing values.")
    return df_mz

###########################################################
# Add execution_date as a column to Pandas dataframe
###########################################################
df["airflow_execution_date"] = pd.to_datetime("today")

###########################################################
# ## LOADING DATA INTO POSTGRES ##
###########################################################
import time
def loading_data(df,schema,tb_target):
    tic = time.perf_counter()    
    engine = create_engine(f"postgresql+psycopg2://{PSQL_USER}:{PSQL_PASSWORD}@{PSQL_HOST}/{PSQL_DB}", echo_pool=True, pool_size=10, max_overflow=20,executemany_mode='values',
                           executemany_values_page_size=10000,
                           executemany_batch_page_size=500)

    print("\n ################################ \n")                       
    print("\n Loading Process Running OK! \n")
    print("\n ################################ \n")                       
    with engine.connect() as conn, conn.begin():

        try:
            insp = engine.execute(f"select exists(select * from information_schema.tables where table_name=%s)", (tb_target,))
            if insp.fetchone()[0]:                
                dfl=[]
                for chunk in pd.read_sql_query(f"SELECT * FROM {schema}.{tb_target};", con=engine, chunksize=500):
                    dfl.append(chunk)
                df_saved = pd.concat(dfl)
                
                if df.shape[1] > df_saved.shape[1]: # check if the layout columns are different
                    engine.execute(f"""drop table {schema}.{tb_target} CASCADE""")
                    
                    df2 = pd.concat([df,df_saved],axis=0, ignore_index=True, sort=False).drop_duplicates(keep='last')
                    df2.to_sql(name=tb_target,con=engine,index=False, if_exists='replace', schema=schema, chunksize=10000)
                    
                else:
                    df.to_sql(name=tb_target,con=engine,index=False, if_exists='append', schema=schema, chunksize=10000)
            else:
                df.to_sql(name=tb_target,con=engine,index=False, if_exists='append', schema=schema, chunksize=10000)
            
        except Exception as err:
            print(str(err))
        
        newRows = engine.execute(f"SELECT count(1) FROM {schema}.{tb_target};")
        countRows = newRows.first()[0]
        
    print("\nClosing database connection! \n")
    engine.dispose()
    
    toc = time.perf_counter()
    return print(f" \nTotal of {abs(countRows)} rows loaded into the target table in {toc - tic:0.4f} seconds!")

if __name__ == '__main__':
    logging.info('starting execution')
    fix_header(df)
    fix_unicode(df)
    rem_special_char(df)
    reduce_mem_usage(df)
    chk_missing_values(df)    
    loading_data(df,schemaname,tablename)
