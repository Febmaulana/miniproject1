import connection
import os
import sqlparse
import pandas as pd

if __name__ == '__main__':
    # market
    conf = connection.config('marketplace_prod')
    conn, engine = connection.get_conn(conf, 'DataSource')
    cursor = conn.cursor()
    
    # dwh
    conf_dwh = connection.config('dwh')
    conn_dwh, engine_dwh = connection.get_conn(conf_dwh, 'DWH')
    cursor_dwh = conn.cursor()

    # query
    path_query = os.getcwd() + '/query/'
    query = sqlparse.format(
        open(path_query + 'query.sql', 'r').read(), strip_comments=True
    ).strip()
    dwh_design = sqlparse.format(
        open(path_query + 'dwh_design.sql', 'r').read(), strip_comments=True
    ).strip()
    
    try:
        # get data
        print('[INFO] waiting etl service sql...')
        df = pd.read_sql(query, engine)

        # create schema
        cursor_dwh.execute(dwh_design)
        conn_dwh.commit()

        # ingest data to dwh
        df.to_sql(
            'dim_orders_febri',
            engine_dwh,
            schema='public',
            if_exists='replace',
            index=False
        )

        print('[INFO] Berhasil memuat etl!...')
    except Exception as e:
        print('[INFO] failed at etl')
        print(str(e))
    