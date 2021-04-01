import json

import requests, zipfile, io
import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import create_engine


def db_connection(r, table_name):
    engine = create_engine(
        'postgresql://srig:iheartmilkyj3lly@data-candidate-homework.c7iises9xj4e.us-east-1.rds.amazon'
        'aws.com:5432/cleanser_db')
    r.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    r.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, table_name, null="")  # null values become ''
    conn.commit()
    print("Successful")


if __name__ == "__main__":
    zip_file_url = "https://s3.amazonaws.com/data-eng-homework/v1/data.zip"
    r = requests.get(zip_file_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    files_list = z.namelist()
    dfs = []
    for file in files_list:
        data = pd.read_json(z.open(file))
        dfs.append(data.orders)
    temp = pd.concat(dfs, ignore_index=True)
    print(temp)
    r = json_normalize(temp)
    r =r.drop(columns = ['line_items'])
    db_connection(r, 'orders')
    line_items = json_normalize(temp, 'line_items', ['id'], record_prefix='line_items')
    db_connection(line_items, 'line_items')
