import json
import time
import requests, zipfile, io
import pandas as pd
import schedule as schedule
from pandas import DataFrame
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
from tabulate import tabulate


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
    print("Connected to postgres database")
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
    print("Records are inserted into the table")


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
        df = json_normalize(df_json, 'line_items', ['id'], record_prefix='line_items_')
    print("Data frame is normalized")
    return df


def zip_file_extraction():
    """
    Downloaded zip file from s3 location
    Extract it and combining it into a single dataframe
    :return: dataframe with downloaded data
    """
    zip_file_url = "https://s3.amazonaws.com/data-eng-homework/v1/data.zip"
    r = requests.get(zip_file_url)
    print("Downloaded zip files")
    z = zipfile.ZipFile(io.BytesIO(r.content))
    print("Zip files are extracted")
    files_list = z.namelist()
    dfs = []
    for file in files_list:
        data = pd.read_json(z.open(file))
        dfs.append(data.orders)
    print("Collected the json into a data frame")
    df_json = pd.concat(dfs, ignore_index=True)
    print("Concatenated the data frame")
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
    print("daily job started")
    df_json = zip_file_extraction()
    print("Extracted and converted the data into data frame with JSON structure")
    con, cur, engine = db_connect()
    print("Connect to the postgres database")
    print("Normalize the data to create orders table")
    df_orders = normalize_json(df_json, True)
    df = datatype_conversion(df_orders)
    print("remove line_items to create it as a new table")
    df_orders = df.drop(columns=['line_items'])
    print("Create or Replace orders table")
    insert_data_postgres(con, cur, engine, df_orders, 'orders')
    print("Normalize line_items dataframe")
    df_line_items = normalize_json(df_json, False)
    print("Create line_items table with id as a foreign key")
    insert_data_postgres(con, cur, engine, df_line_items, 'line_items')
    print("Create user summary metrics")
    df_users = df_orders[['user_id', 'total_price']]
    print("Find out the overall orders every user made with average amount they spent in shopify")
    df_users_summary = df_users.groupby('user_id').agg(['mean', 'count']).reset_index()
    print("Insert the user metrics in the users table")
    insert_data_postgres(con, cur, engine, df_users_summary, 'users')
    df_product = df_line_items[['line_items_product_id', 'line_items_quantity']]
    df_product_summary = df_product.groupby('line_items_product_id').sum().reset_index()
    print(df_product_summary.columns.values)
    print("Insert the product metrics in the products table")
    insert_data_postgres(con, cur, engine, df_product_summary, 'products')
    print("Terminate the database connection")
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
    # Added this function call to test this code
    daily_job()
