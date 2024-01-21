import pandas as pd
from fastapi import FastAPI, status
import json
import sqlalchemy
from sqlalchemy import text
import property_config

engine = sqlalchemy.create_engine(f'postgresql+psycopg2://postgres:{property_config.db_passowrd}@{property_config.db_username}/{property_config.db_name}', 
                echo=True, pool_pre_ping=True, pool_size=20, max_overflow=0)
app = FastAPI()

def get_min_df(x, x_value, x_text, col, schema, table):
    if x_text in x:
        query = text(f'SELECT * FROM {schema}.{table} WHERE "{col}" > {x_value}')
        x = x.replace(x_text, '')
        df = pd.read_sql_query(
            sql = query,
            con = engine.connect()
        )
        return df
    return None

def get_max_df(x, x_value, x_text, col, schema, table):
    if x_text in x:
        x = x.replace(x_text, '')
        query = text(f'SELECT * FROM {schema}.{table} WHERE "{col}" < {x_value}')
        df = pd.read_sql_query(
            sql = query,
            con = engine.connect()
        )
        return df
    return None

def get_filtered_df(json_data):
    df = []
    for x in json_data:
        if 'price_min' in x:
            df1 = get_min_df(x, json_data[x], 'price_min', 'Price', 'public', 'test').drop_duplicates(subset='pid')
            df.append(df1)
        elif 'price_max' in x:
            df2 = get_max_df(x, json_data[x], 'price_max', 'Price', 'public', 'test').drop_duplicates(subset='pid')
            df.append(df2)
        elif 'floor_min' in x:
            df3 = get_min_df(x, json_data[x], 'floor_min', 'Alapterulet', 'public', 'test').drop_duplicates(subset='pid')
            df.append(df3)
        elif 'floor_max' in x:
            df4 = get_max_df(x, json_data[x], 'floor_max', 'Alapterulet', 'public', 'test').drop_duplicates(subset='pid')
            df.append(df4)
        elif 'room_min' in x:
            df5 = get_min_df(x, json_data[x], 'room_min', 'Szobak', 'public', 'test').drop_duplicates(subset='pid')
            df.append(df5)
        elif 'room_max' in x:
            df6 = get_max_df(x, json_data[x], 'room_max', 'Szobak', 'public', 'test').drop_duplicates(subset='pid')
            df.append(df6)
    if len(df) < 1:
        return pd.DataFrame(df)
    else:
        dfs = pd.concat(df, ignore_index=True)
        dfGroup = dfs.groupby(list(dfs.columns))
        res = [k[0] for k in dfGroup.groups.values() if len(k) > 1]
        all_df = dfs.reindex(res)
        return all_df

@app.get('/get-filtered-data/{schema}/{json_data}', status_code=status.HTTP_200_OK)
def Get_Filter_data(schema:str, json_data: str):
    con = engine.connect()
    json_data = json_data.replace("'", '"')
    json_data = json.loads(json_data)
    df = get_filtered_df(json_data)
    if df.empty:
        df = pd.read_sql_table(
            table_name='property',
            schema=schema,
            con = con
        )
        all_json_data = json.loads(df.to_json(orient='records'))
    else:
        all_json_data = json.loads(df.to_json(orient='records'))
    con.close()
    return all_json_data

@app.get('/get-all/{schema}', status_code=status.HTTP_200_OK)
def Get_Data(schema):
    con = engine.connect()
    df = pd.read_sql_table(
        table_name='property',
        schema=schema,
        con = con
    )
    all_json_data = json.loads(df.to_json(orient='records'))
    con.close()
    return all_json_data
