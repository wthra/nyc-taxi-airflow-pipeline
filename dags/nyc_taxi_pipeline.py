import os
import requests
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from sqlalchemy import create_engine

# ตั้งค่าพื้นฐานสำหรับ Tasks ทุกตัว
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1, # ถ้ารันพัง ให้ลองใหม่ 1 ครั้ง
    'retry_delay': timedelta(minutes=5), # รอ 5 นาทีก่อนลองใหม่
}

# ประกาศตัว DAG
dag = DAG(
    'nyc_taxi_pipeline',
    default_args=default_args,
    description='Pipeline ดึงข้อมูล แท็กซี่นิวยอร์ก',
    start_date=datetime(2026, 3, 14),
    schedule='@daily', # ให้รันทุกวัน
    catchup=False,
)

def ingest_taxi_data(**context):
    url = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    output_path = "/tmp/nyc_taxi_raw.csv"
    
    # วนลูปดาวน์โหลด (พยายามสูงสุด 3 ครั้งเผื่อเน็ตหลุด)
    for attempt in range(3):
        try:
            logging.info(f"กำลังดาวน์โหลดข้อมูล (ครั้งที่ {attempt+1})...")
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
            break # ถ้าสำเร็จให้ออกจากลูป
        except Exception as e:
            if attempt == 2:
                raise AirflowException(f"ดาวน์โหลดล้มเหลว: {e}")
    
    # เช็คว่าไฟล์มีข้อมูลเกิน 1000 บรรทัดไหม (Validation)
    df_sample = pd.read_csv(output_path)
    if len(df_sample) < 1000:
        raise AirflowException("ข้อมูลน้อยกว่า 1,000 บรรทัด อาจมีข้อผิดพลาด")
    
    # ฝากที่อยู่ไฟล์ไว้ใน XCom เพื่อให้ Task หน้าดึงไปใช้ต่อได้
    context["ti"].xcom_push(key="raw_path", value=output_path)
    return output_path


def clean_taxi_data(**context):
    # ดึงที่อยู่ไฟล์จาก Task ที่แล้ว
    raw_path = context["ti"].xcom_pull(task_ids="ingest_taxi_data", key="raw_path")
    df = pd.read_csv(raw_path)
    
    # คลีนข้อมูล: เอาเฉพาะค่าโดยสารที่มากกว่า 0 แต่ไม่เกิน 500
    if 'fare_amount' in df.columns:
        df = df[(df['fare_amount'] > 0) & (df['fare_amount'] < 500)]
    
    # ลบแถวที่มีค่าว่างทิ้งไป
    df = df.dropna()
    
    # เซฟเป็นไฟล์ใหม่
    clean_path = "/tmp/nyc_taxi_clean.csv"
    df.to_csv(clean_path, index=False)
    
    # ส่งที่อยู่ไฟล์ใหม่ไปให้ Task ถัดไป
    context["ti"].xcom_push(key="clean_path", value=clean_path)


def transform_taxi_data(**context):
    # ดึงไฟล์ที่คลีนแล้วมา
    clean_path = context["ti"].xcom_pull(task_ids="clean_taxi_data", key="clean_path")
    df = pd.read_csv(clean_path)
    
    # ค้นหาคอลัมน์ที่เกี่ยวกับเวลา Pickup และ Dropoff
    time_cols = [col for col in df.columns if 'pickup_datetime' in col.lower() or 'dropoff_datetime' in col.lower()]
    
    if len(time_cols) >= 2:
        pickup_col, dropoff_col = time_cols[0], time_cols[1]
        df[pickup_col] = pd.to_datetime(df[pickup_col], errors='coerce')
        df[dropoff_col] = pd.to_datetime(df[dropoff_col], errors='coerce')
        
        # 1. สร้างคอลัมน์ระยะเวลา (นาที)
        df['trip_duration_minutes'] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 60.0
        df = df[df['trip_duration_minutes'] > 0] # ตัดเวลาติดลบ
        
        # 2. สร้างคอลัมน์ความเร็ว (mph)
        if 'trip_distance' in df.columns:
            df['speed_mph'] = df['trip_distance'] / (df['trip_duration_minutes'] / 60.0)
            df['speed_mph'] = df['speed_mph'].replace([np.inf, -np.inf], np.nan).fillna(0)

    # เซฟและส่งต่อ
    transform_path = "/tmp/nyc_taxi_transformed.csv"
    df.to_csv(transform_path, index=False)
    context["ti"].xcom_push(key="transform_path", value=transform_path)


def load_taxi_model(**context):
    transform_path = context["ti"].xcom_pull(task_ids="transform_taxi_data", key="transform_path")
    df = pd.read_csv(transform_path)
    
    try:
        # เชื่อมต่อ Database โดยดึงรหัสผ่านจาก Airflow Variables
        engine = create_engine(
            f"mysql+pymysql://{Variable.get('MYSQL_USER')}:{Variable.get('MYSQL_PASSWORD')}"
            f"@{Variable.get('MYSQL_HOST')}/{Variable.get('MYSQL_DB')}"
        )
        # โหลดข้อมูลเข้าตาราง fact_nyc_taxi_trips (ทยอยโหลดทีละ 2000 บรรทัดกันแรมเต็ม)
        df.to_sql("fact_nyc_taxi_trips", engine, if_exists="replace", index=False, chunksize=2000)
        logging.info("โหลดเข้า MySQL สำเร็จ!")
    except Exception as e:
        # ใส่ Try-Except ไว้ เผื่อคุณยังไม่ได้ลง MySQL ในเครื่อง โค้ดจะได้ไม่พังตอนทดสอบรัน Task 1-3
        logging.error(f"ไม่สามารถเชื่อมต่อ Database ได้ (ถ้ายังไม่ตั้งค่าให้ข้ามไปก่อน): {e}")


# ---------------------------------------------------------
# 1. แปลงฟังก์ชันให้เป็น Airflow Tasks
# ---------------------------------------------------------
task_ingest = PythonOperator(
    task_id='ingest_taxi_data',
    python_callable=ingest_taxi_data,
    dag=dag
)

task_clean = PythonOperator(
    task_id='clean_taxi_data',
    python_callable=clean_taxi_data,
    dag=dag
)

task_transform = PythonOperator(
    task_id='transform_taxi_data',
    python_callable=transform_taxi_data,
    dag=dag
)

task_load = PythonOperator(
    task_id='load_taxi_model',
    python_callable=load_taxi_model,
    dag=dag
)

# 2. กำหนดลำดับการทำงาน (Dependency)
task_ingest >> task_clean >> task_transform >> task_load