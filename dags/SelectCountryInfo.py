from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

import requests
import logging
import json

#redshift connection
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


#세계 나라 정보 api와 연결하여 각 나라의 공식 이름, 인구 수, 지역만 추출
@task
def get_country_info():
    url = f"https://restcountries.com/v3/all"
    response = requests.get(url)
    data = json.loads(response.text)
    
    records = []
    for country_data in data:
        records.append([country_data["name"]["official"], country_data["population"], country_data["area"]])
    
    return records

@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};") #테이블이 있으면 테이블을 드롭하고 다시 생성
        cur.execute(f"""CREATE TABLE {schema}.{table} (
                            COUNTRY VARCHAR,
                            POPULATION BIGINT,
                            AREA FLOAT
                        );""")
        # FULL REFRESH기 때문에 INSERT INTO 진행
        for r in records:
            #작은 따옴표로 넣을 시 국가명에 작은 따옴표가 있는 경우가 존재해 오류가 발생하여 이스케이프 처리함
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s);" 
            print(sql)
            cur.execute(sql, (r[0], r[1], r[2]))
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")


with DAG(
    dag_id = 'SelectCountryInfo',
    start_date = datetime(2023,6,8),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:
    
    results = get_country_info()
    load("ssong_ji_hy", "COUNTRY_INFO", results)