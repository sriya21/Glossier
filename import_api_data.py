import json
import time
import requests, zipfile, io
import pandas as pd
import schedule as schedule
from pandas.io.json import json_normalize
from sqlalchemy import create_engine


def db_connect():
    """
    Connect to the database to communicate with database
    :return: postgres engine, connection string and cursor to operate with database
    """
    engine = create_engine(
        'postgresql://srig:iheartmilkyj3lly@data-candidate-homework.c7iises9xj4e.us-east-1.rds.amazon'
        'aws.com:5432/cleanser_db')
    con = engine.raw_connection()
    cur = con.cursor()
    return con, cur, engine


def insert_data_postgres(con, cur, engine, df, table_name):
    """
    Create a new table and delete the existing table if any
    created output string to insert the data into the postgres table
    converted the data into csv table to avoid converting them on the fly to improve performance
    Commit the inserted table
    :param con: connection string to connect to the database
    :param cur: cursor to operate with the database
    :param engine: postgres engine created
    :param df: data frame to insert into table
    :param table_name: table name to be created in database
    :return: none
    """
    df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, table_name, null="")
    con.commit()


def db_terminate(con):
    """
    To terminate the database connection
    :param con: postgres connection string
    :return: none
    """
    con.close()
    print("connection terminated with postgres database")


def normalize_json(df_json, flag):
    """
    Normalizing the data to store it directly in postgres database
    :param df_json: data frame with JSON structure
    :param flag: to identify which normalization to use
    :return: normalized data frame
    """
    if flag:
        df = json_normalize(df_json)
    else:
        df = json_normalize(df_json, 'line_items', ['id'], record_prefix='line_items')
    return df


def zip_file_extraction():
    """
    Downloaded zip file from s3 location
    Extract it and combining it into a single dataframe
    :return: dataframe with downloaded data
    """
    zip_file_url = "https://s3.amazonaws.com/data-eng-homework/v1/data.zip"
    r = requests.get(zip_file_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    files_list = z.namelist()
    dfs = []
    for file in files_list:
        data = pd.read_json(z.open(file))
        dfs.append(data.orders)
    df_json = pd.concat(dfs, ignore_index=True)
    return df_json


def datatype_conversion(df):
    """
    Given a data frame with string datatype for few columns
    Return the data frame with column's data type as float
    :param df: data frame with columns with datatype as string
    :return: data frame with converted datatypes for necessary columns
    """
    convert_dict = {'total_price': float,
                    'subtotal_price': float,
                    'total_tax': float,
                    'total_discounts': float,
                    'total_line_items_price': float,
                    'total_price_usd': float,
                    }
    return df.astype(convert_dict)


def daily_job():
    """
    Zip file is extracted and concatenated all the JSON structure
    Normalized the data so that it can be stored in the postgres table
    create a connection with the database to insert the record
    Insert the records in their respective tables after scrutinizing the data
    :return:None
    """
    df_json = zip_file_extraction()
    con, cur, engine = db_connect()
    df_orders = normalize_json(df_json, True)
    df = datatype_conversion(df_orders)
    df_orders = df.drop(columns=['line_items'])
    insert_data_postgres(con, cur, engine, df_orders, 'orders')
    df_line_items = normalize_json(df_json, False)
    insert_data_postgres(con, cur, engine, df_line_items, 'line_items')
    df_users = df_orders[['user_id', 'total_price']]
    df_users_summary = df_users.groupby('user_id').agg(['mean', 'count'])
    insert_data_postgres(con, cur, engine, df_users_summary, 'users')
    db_terminate(con)


if __name__ == "__main__":
    """
    User needs to schedule this job every day 
    so the this job is scheduled to run at 10:30 AM every day
    """
    # schedule.every().day.at("10:30").do(daily_job)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    ## Added this function call to test this code
    daily_job()
