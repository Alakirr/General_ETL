from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import psycopg2
import pandas as pd
import requests
import json
import time

current_date_time = datetime.now()
date_time = '_date_' + str(current_date_time.date()).replace('-', '_') + '_time_' + str(current_date_time.time())[0:5].replace(':', '_')

PATH = '/opt/airflow/data'

default_args = {
    'start_date':datetime(2024, 12, 8, 23, 55),
    'owner':'airflow'
}

dag = DAG(
    'ETL',
    default_args=default_args,
    description='Get_data',
    schedule_interval='0 0/3 * * *'
)


def get_data(page:int=0):

    main_df = pd.DataFrame()

    for page in range(0, 10):
        params = {
            'text': 'NAME:Аналитик',
            'area': 1,  # Поиск по Москве
            'page': page,
            'per_page': 100  # Кол-во вакансий на 1 стр.
        }
        print('ТУТ!')
        req = requests.get('https://api.hh.ru/vacancies', params)  # Запрос к API
        data = req.content.decode()  # Декодируем ответ, чтобы Кириллица отображалась корректно
        req.close()
        df = pd.DataFrame(json.loads(data))
        df = pd.concat([df.drop('items', axis=1), df['items'].apply(pd.Series)], axis=1)
        df = df.rename(columns={'id':'vacancy_id', 'name':'vacancy'})
        df = df[['page', 'vacancy_id', 'vacancy','employer','professional_roles','experience']]
        df = pd.concat([df.drop('experience', axis=1), df['experience'].apply(pd.Series)], axis=1)
        df = df.rename(columns={'name':'experience'})
        df = df[['page', 'vacancy_id', 'vacancy','employer','professional_roles', 'experience']]
        df = pd.concat([df.drop('professional_roles', axis=1), df['professional_roles'].apply(pd.Series)], axis=1)
        df = pd.concat([df.drop(0, axis=1), df[0].apply(pd.Series)], axis=1)
        df = df.rename(columns={'name':'specialize', 'id':'specialize_id'})
        df = pd.concat([df.drop('employer', axis=1), df['employer'].apply(pd.Series)], axis=1)
        df = df.rename(columns={'name':'company'})
        df = df[['page', 'vacancy_id', 'vacancy', 'experience', 'company', 'specialize_id']]
        main_df = pd.concat([df, main_df], axis=0, ignore_index=True)

    main_df.to_csv(f'{PATH}/vacancy.csv', index=False)


def etl_company():
    spark = SparkSession.builder.getOrCreate()
    df_sp = spark.read.csv(f'{PATH}/vacancy.csv', header=True)

    df_sp = df_sp.withColumn(colName='vacancy', col=F.split(str=df_sp.vacancy, pattern=r" \(").getItem(0))
    df_sp = df_sp.withColumn(colName='vacancy', col=F.split(str=df_sp.vacancy, pattern=r" \/").getItem(0))

    df_sp_company = df_sp\
        .groupBy('company')\
        .agg(F.count('company')\
        .alias('count_vacancy'))

    df_sp_company = df_sp_company\
        .filter(df_sp_company.count_vacancy >= 4)\
        .orderBy(['count_vacancy'], ascending=[False])    

    conn = psycopg2.connect(host='postgres', database='airflow', user='airflow', password='airflow', port=5432)
    cursor = conn.cursor()

    cursor.execute(f'CREATE TABLE vacancy_company_{date_time}(company VARCHAR(150), count_vacancy INT)')
    data = list(df_sp_company.toLocalIterator())
    data = [(_[0], _[1]) for _ in data]

    columns = ','.join(df_sp_company.columns)
                    
    placeholders = ', '.join(['%s'] * len(df_sp_company.columns))
                                            
    query = f'INSERT INTO vacancy_company_{date_time}({columns}) VALUES ({placeholders})'
    cursor.executemany(query, data)
    
    conn.commit()
    cursor.close()
    conn.close()


def etl_experience():
    spark = SparkSession.builder.getOrCreate()
    df_sp = spark.read.csv(f'{PATH}/vacancy.csv', header=True)
        
    df_sp = df_sp.withColumn(colName='vacancy', col=F.split(str=df_sp.vacancy, pattern=r" \(").getItem(0))
    df_sp = df_sp.withColumn(colName='vacancy', col=F.split(str=df_sp.vacancy, pattern=r" \/").getItem(0))
        
    df_sp_experience = df_sp\
        .groupBy('experience')\
        .agg(F.count('experience')\
        .alias('count_experience'))\
        .orderBy(['count_experience'], ascending=[False])    
    
    conn = psycopg2.connect(host='postgres', database='airflow', user='airflow', password='airflow', port=5432)
    cursor = conn.cursor()
        
    cursor.execute(f'CREATE TABLE vacancy_experience_{date_time}(experience VARCHAR(50), count_experience INT)')
    data = list(df_sp_experience.toLocalIterator())
    data = [(_[0], _[1]) for _ in data]
            
    columns = ','.join(df_sp_experience.columns)

    placeholders = ', '.join(['%s'] * len(df_sp_experience.columns))
    
    query = f'INSERT INTO vacancy_experience_{date_time}({columns}) VALUES ({placeholders})'
    cursor.executemany(query, data)
    
    conn.commit()
    cursor.close()
    conn.close()



def etl_vacancy():
    spark = SparkSession.builder.getOrCreate()
    df_sp = spark.read.csv(f'{PATH}/vacancy.csv', header=True)
        
    df_sp = df_sp.withColumn(colName='vacancy', col=F.split(str=df_sp.vacancy, pattern=r" \(").getItem(0))
    df_sp = df_sp.withColumn(colName='vacancy', col=F.split(str=df_sp.vacancy, pattern=r" \/").getItem(0))
    
    df_sp_vacancy = df_sp\
        .groupBy('vacancy')\
        .agg(F.count('vacancy')\
        .alias('vacancy_count'))\
        .orderBy(['vacancy_count'], ascending=[False])

    df_sp_vacancy = df_sp_vacancy.filter(df_sp_vacancy.vacancy_count>=4)    
    
    conn = psycopg2.connect(host='postgres', database='airflow', user='airflow', password='airflow', port=5432)
    cursor = conn.cursor()
    
    cursor.execute(f'CREATE TABLE vacancy_vac_{date_time}(vacancy VARCHAR(200), vacancy_count INT)')
    data = list(df_sp_vacancy.toLocalIterator())
    data = [(_[0], _[1]) for _ in data]
    
    columns = ','.join(df_sp_vacancy.columns)
    
    placeholders = ', '.join(['%s'] * len(df_sp_vacancy.columns))
        
    query = f'INSERT INTO vacancy_vac_{date_time}({columns}) VALUES ({placeholders})'
    cursor.executemany(query, data)
    
    conn.commit()
    cursor.close()
    conn.close()


def make_copy():
    df = pd.read_csv(f'{PATH}/vacancy.csv')
    df.to_csv(f'{PATH}/vacancy_2.csv', index=False)



task_get_data = PythonOperator(task_id='get_data',
                                  python_callable=get_data,
                                  dag=dag,
                                  provide_context=True)


task_make_copy = PythonOperator(task_id='make_copy',
                                  python_callable=make_copy,
                                  dag=dag,
                                  provide_context=True)


task_elt_company = PythonOperator(task_id='etl_company',
                                  python_callable=etl_company,
                                  dag=dag,
                                  provide_context=True)


task_elt_experience = PythonOperator(task_id='etl_experience',
                                  python_callable=etl_experience,
                                  dag=dag,
                                  provide_context=True)


task_elt_vacancy = PythonOperator(task_id='etl_vacancy',
                                  python_callable=etl_vacancy,
                                  dag=dag,
                                  provide_context=True)


task_get_data >> task_make_copy >> task_elt_company >> task_elt_experience >> task_elt_vacancy
