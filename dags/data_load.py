from airflow import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
from datetime import datetime
import transform

FILE_CONNECTION_ID = 'fs_default'
COLUMNS_BASE = {
    "Country/Region":"country_region",
    "Province/State":"province_state",
    "Lat":"lat",
    "Long":"long",
    "event_date": "event_date",
    "value":"cases",
    "cases_per_day":"cases_per_day",
    "cum_sum_by_country":"cum_sum_by_country",
    "cum_max":"cum_max",
    "cases_per_day_per_country":"cases_per_day_per_country"
}
COLUMNS_C = {
    "cases":"c_cases",
    "cases_per_day":"c_cases_per_day",
    "cases_per_day_per_country":"c_cases_per_day_per_country",
    "cum_sum_by_country":"c_cum_sum_by_country",
    "cum_max":"c_cum_max"
}
COLUMNS_R = {
    "cases":"r_cases",
    "cases_per_day":"r_cases_per_day",
    "cases_per_day_per_country":"r_cases_per_day_per_country",
    "cum_sum_by_country":"r_cum_sum_by_country",
    "cum_max":"r_cum_max"
}
COLUMNS_D = {
    "cases":"d_cases",
    "cases_per_day":"d_cases_per_day",
    "cases_per_day_per_country":"d_cases_per_day_per_country",
    "cum_sum_by_country":"d_cum_sum_by_country",
    "cum_max":"d_cum_max"
}
COLUMNS_VIEW = ['country_region','province_state','lat','long','event_date','c_cases','r_cases','d_cases',
                'c_cases_per_day','r_cases_per_day','d_cases_per_day','c_cases_per_day_per_country',
                'r_cases_per_day_per_country','d_cases_per_day_per_country','c_cum_sum_by_country',
                'r_cum_sum_by_country','d_cum_sum_by_country','c_cum_max','r_cum_max','d_cum_max',
                'mortality_rate','recovery_rate'],

FILE_NAME_C = "time_series_covid19_confirmed_global.csv"
OUTPUT_TRANSFORM_FILE_C = '_time_series_covid19_confirmed_global_tmp.csv'
FILE_NAME_R = "time_series_covid19_recovered_global.csv"
OUTPUT_TRANSFORM_FILE_R = '_time_series_covid19_recovered_global_tmp.csv'
FILE_NAME_D = "time_series_covid19_deaths_global.csv"
OUTPUT_TRANSFORM_FILE_D = '_time_series_covid19_deaths_global_tmp.csv'


dag = DAG('data_load', description='Load of al data cases of covid',
          default_args={
              'owner': 'friendly.system',
              'depends_on_past': False,
              'max_active_runs': 1,
              'start_date': days_ago(5)
          },
          schedule_interval='0 0 * * *',
          catchup=False)

file_sensor_task_C = FileSensor(dag=dag,
                              task_id="file_sensor_C",
                              fs_conn_id=FILE_CONNECTION_ID,
                              filepath=FILE_NAME_C,
                              poke_interval=10,
                              timeout=300
                              )

def transform_func_C(**kwargs):
    folder_path = FSHook(conn_id=FILE_CONNECTION_ID).get_path()
    file_path = f"{folder_path}/{FILE_NAME_C}"
    destination_file = f"{folder_path}/{OUTPUT_TRANSFORM_FILE_C}"
    df = pd.read_csv(file_path,encoding="ISO-8859-1")
    df_final = transform.transformm_df(df)
    df_final = df_final.rename(columns=COLUMNS_BASE)
    df_final.to_csv(destination_file, index=False)
    os.remove(file_path)
    return destination_file

transform_process_C = PythonOperator(dag=dag,
                                   task_id="transform_process_C",
                                   python_callable=transform_func_C,
                                   provide_context=True
                                   )

def insert_process_C(**kwargs):
    ti = kwargs['ti']
    source_file = ti.xcom_pull(task_ids='transform_process_C')
    db_connection = MySqlHook('airflow_db').get_sqlalchemy_engine()
    df = pd.read_csv(source_file)
    with db_connection.begin() as transaction:
        transaction.execute("DELETE FROM covid.confirmed WHERE 1=1")
        df.to_sql("confirmed", con=transaction, schema="covid", if_exists="append",
                  index=False)
    os.remove(source_file)

insert_process_C = PythonOperator(dag=dag,
                                task_id="insert_process_C",
                                provide_context=True,
                                python_callable=insert_process_C)

file_sensor_task_R = FileSensor(dag=dag,
                              task_id="file_sensor_R",
                              fs_conn_id=FILE_CONNECTION_ID,
                              filepath=FILE_NAME_R,
                              poke_interval=10,
                              timeout=300
                              )

def transform_func_R(**kwargs):
    folder_path = FSHook(conn_id=FILE_CONNECTION_ID).get_path()
    file_path = f"{folder_path}/{FILE_NAME_R}"
    destination_file = f"{folder_path}/{OUTPUT_TRANSFORM_FILE_R}"
    df = pd.read_csv(file_path,encoding="ISO-8859-1")
    df_final = transform.transformm_df(df)
    df_final = df_final.rename(columns=COLUMNS_BASE)
    df_final.to_csv(destination_file, index=False)
    os.remove(file_path)
    return destination_file

transform_process_R = PythonOperator(dag=dag,
                                   task_id="transform_process_R",
                                   python_callable=transform_func_R,
                                   provide_context=True
                                   )

def insert_process_R(**kwargs):
    ti = kwargs['ti']
    source_file = ti.xcom_pull(task_ids='transform_process_R')
    db_connection = MySqlHook('airflow_db').get_sqlalchemy_engine()
    df = pd.read_csv(source_file)
    with db_connection.begin() as transaction:
        transaction.execute("DELETE FROM covid.recovered WHERE 1=1")
        df.to_sql("recovered", con=transaction, schema="covid", if_exists="append",
                  index=False)
    os.remove(source_file)

insert_process_R = PythonOperator(dag=dag,
                                task_id="insert_process_R",
                                provide_context=True,
                                python_callable=insert_process_R)

file_sensor_task_D = FileSensor(dag=dag,
                              task_id="file_sensor",
                              fs_conn_id=FILE_CONNECTION_ID,
                              filepath=FILE_NAME_D,
                              poke_interval=10,
                              timeout=300
                              )

def transform_func_D(**kwargs):
    folder_path = FSHook(conn_id=FILE_CONNECTION_ID).get_path()
    file_path = f"{folder_path}/{FILE_NAME_D}"
    destination_file = f"{folder_path}/{OUTPUT_TRANSFORM_FILE_D}"
    df = pd.read_csv(file_path,encoding="ISO-8859-1")
    df_final = transform.transformm_df(df)
    df_final = df_final.rename(columns=COLUMNS_BASE)
    df_final.to_csv(destination_file, index=False)
    os.remove(file_path)
    return destination_file

transform_process_D = PythonOperator(dag=dag,
                                   task_id="transform_process_D",
                                   python_callable=transform_func_D,
                                   provide_context=True
                                   )

def insert_process_D(**kwargs):
    ti = kwargs['ti']
    source_file = ti.xcom_pull(task_ids='transform_process_D')
    db_connection = MySqlHook('airflow_db').get_sqlalchemy_engine()
    df = pd.read_csv(source_file)
    with db_connection.begin() as transaction:
        transaction.execute("DELETE FROM covid.deaths WHERE 1=1")
        df.to_sql("deaths", con=transaction, schema="covid", if_exists="append",
                index=False)
    os.remove(source_file)

insert_process_D = PythonOperator(dag=dag,
                                task_id="insert_process_D",
                                provide_context=True,
                                python_callable=insert_process_D)



def integration_procces(**kwargs):
    db_connection = MySqlHook('airflow_db').get_sqlalchemy_engine()
    with db_connection.begin() as transaction:
        df_C = pd.read_sql_table("confirmed",con=transaction,schema='covid')
        df_R = pd.read_sql_table("recovered",con=transaction,schema='covid')
        df_D = pd.read_sql_table("deaths",con=transaction,schema='covid')  
    df_C = df_C.drop(columns = ['id']) 
    df_R = df_R.drop(columns = ['id','lat','long']) 
    df_D = df_D.drop(columns = ['id','lat','long']) 
    df_C['province_state'] = df_C['province_state'].fillna('')
    df_R['province_state'] = df_R['province_state'].fillna('')
    df_D['province_state'] = df_D['province_state'].fillna('')
    df_C = df_C.rename(columns=COLUMNS_C)
    df_R = df_R.rename(columns=COLUMNS_R)
    df_D = df_D.rename(columns=COLUMNS_D)
    df = pd.merge(df_C, df_R, on = ['country_region','province_state','event_date'])
    df = pd.merge(df, df_D, on = ['country_region','province_state','event_date']) 
    df['mortality_rate'] = df['d_cases'] / df['c_cases'] 
    df['recovery_rate'] = df['r_cases'] / df['c_cases'] 
    #df_final = df[COLUMNS_VIEW]
    df_final = df
    with db_connection.begin() as transaction:
        transaction.execute("DELETE FROM covid.cases_data WHERE 1=1")
        df_final.to_sql("cases_data", con=transaction, schema="covid", if_exists="append",
                index=False)


integration_procces = PythonOperator(dag=dag,
                                task_id="integration_procces",
                                provide_context=True,
                                python_callable=integration_procces)

file_sensor_task_C >> transform_process_C >> insert_process_C
file_sensor_task_R >> transform_process_R >> insert_process_R
file_sensor_task_D >> transform_process_D >> insert_process_D

#[insert_process_C,insert_process_R,insert_process_D] >> integration_procces
integration_procces.set_upstream(insert_process_C)
integration_procces.set_upstream(insert_process_R)
integration_procces.set_upstream(insert_process_D)