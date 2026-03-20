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

# --- [ฟีเจอร์ใหม่ 1] ฟังก์ชันแจ้งเตือนเมื่อ Task พัง ---
def on_failure_callback(context):
    failed_task = context.get('task_instance').task_id
    error_msg = context.get('exception')
    # ในการใช้งานจริง สามารถเปลี่ยนเป็นรัน SlackWebhookOperator หรือส่ง LINE API ตรงนี้ได้
    logging.error(f"🚨 ALERT: Task '{failed_task}' ล้มเหลว! สาเหตุ: {error_msg}")

# ตั้งค่าพื้นฐานสำหรับ Tasks ทุกตัว
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback, # <--- ผูกฟังก์ชันแจ้งเตือนเข้ากับทุก Task
}

# ประกาศตัว DAG
dag = DAG(
    'nyc_taxi_pipeline_advanced',
    default_args=default_args,
    description='Pipeline ดึงข้อมูล แท็กซี่นิวยอร์ก พร้อมระบบ Report และ Cleanup',
    start_date=datetime(2026, 3, 14),
    schedule='@daily',
    catchup=False,
)

# ... (ฟังก์ชัน ingest_taxi_data, clean_taxi_data, transform_taxi_data ใช้โค้ดเดิมของคุณได้เลย) ...
def ingest_taxi_data(**context):
    url = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    output_path = "/tmp/nyc_taxi_raw.csv"
    for attempt in range(3):
        try:
            logging.info(f"กำลังดาวน์โหลดข้อมูล (ครั้งที่ {attempt+1})...")
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
            break
        except Exception as e:
            if attempt == 2:
                raise AirflowException(f"ดาวน์โหลดล้มเหลว: {e}")
    df_sample = pd.read_csv(output_path)
    if len(df_sample) < 1000:
        raise AirflowException("ข้อมูลน้อยกว่า 1,000 บรรทัด อาจมีข้อผิดพลาด")
    context["ti"].xcom_push(key="raw_path", value=output_path)
    return output_path

def clean_taxi_data(**context):
    raw_path = context["ti"].xcom_pull(task_ids="ingest_taxi_data", key="raw_path")
    df = pd.read_csv(raw_path)
    if 'fare_amount' in df.columns:
        df = df[(df['fare_amount'] > 0) & (df['fare_amount'] < 500)]
    df = df.dropna()
    clean_path = "/tmp/nyc_taxi_clean.csv"
    df.to_csv(clean_path, index=False)
    context["ti"].xcom_push(key="clean_path", value=clean_path)

def transform_taxi_data(**context):
    clean_path = context["ti"].xcom_pull(task_ids="clean_taxi_data", key="clean_path")
    df = pd.read_csv(clean_path)
    time_cols = [col for col in df.columns if 'pickup_datetime' in col.lower() or 'dropoff_datetime' in col.lower()]
    if len(time_cols) >= 2:
        pickup_col, dropoff_col = time_cols[0], time_cols[1]
        df[pickup_col] = pd.to_datetime(df[pickup_col], errors='coerce')
        df[dropoff_col] = pd.to_datetime(df[dropoff_col], errors='coerce')
        df['trip_duration_minutes'] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 60.0
        df = df[df['trip_duration_minutes'] > 0]
        if 'trip_distance' in df.columns:
            df['speed_mph'] = df['trip_distance'] / (df['trip_duration_minutes'] / 60.0)
            df['speed_mph'] = df['speed_mph'].replace([np.inf, -np.inf], np.nan).fillna(0)
    transform_path = "/tmp/nyc_taxi_transformed.csv"
    df.to_csv(transform_path, index=False)
    context["ti"].xcom_push(key="transform_path", value=transform_path)

def load_taxi_model(**context):
    transform_path = context["ti"].xcom_pull(task_ids="transform_taxi_data", key="transform_path")
    df = pd.read_csv(transform_path)
    try:
        engine = create_engine(
            f"mysql+pymysql://{Variable.get('MYSQL_USER')}:{Variable.get('MYSQL_PASSWORD')}"
            f"@{Variable.get('MYSQL_HOST')}/{Variable.get('MYSQL_DB')}"
        )
        df.to_sql("fact_nyc_taxi_trips", engine, if_exists="replace", index=False, chunksize=2000)
        logging.info("โหลดเข้า MySQL สำเร็จ!")
    except Exception as e:
        logging.warning(f"ข้ามการโหลดเข้า DB ไปก่อน: {e}")

# --- [ฟีเจอร์ใหม่ 2] ฟังก์ชันสร้างรายงานสรุป ---
def generate_summary_report(**context):
    transform_path = context["ti"].xcom_pull(task_ids="transform_taxi_data", key="transform_path")
    df = pd.read_csv(transform_path)
    
    # สรุปข้อมูลภาพรวม
    summary_data = {
        'total_trips': len(df),
        'total_revenue': df['fare_amount'].sum(),
        'avg_fare': df['fare_amount'].mean(),
        'avg_speed_mph': df['speed_mph'].mean(),
        'report_date': datetime.now().strftime("%Y-%m-%d")
    }
    
    summary_df = pd.DataFrame([summary_data])
    logging.info(f"รายงานสรุปประจำวัน:\n{summary_df.to_string(index=False)}")
    
    # สามารถนำ summary_df ไปบันทึกทับใน Table 'daily_taxi_summary' ใน MySQL ได้ด้วยเช่นกัน
    return summary_data

# --- [ฟีเจอร์ใหม่ 3] ฟังก์ชันลบไฟล์ Temp ---
def cleanup_temp_files(**context):
    files_to_delete = [
        "/tmp/nyc_taxi_raw.csv",
        "/tmp/nyc_taxi_clean.csv",
        "/tmp/nyc_taxi_transformed.csv"
    ]
    for file_path in files_to_delete:
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"🗑️ ลบไฟล์ชั่วคราวทิ้งแล้ว: {file_path}")

# ---------------------------------------------------------
# กำหนด Airflow Tasks
# ---------------------------------------------------------
task_ingest = PythonOperator(task_id='ingest_taxi_data', python_callable=ingest_taxi_data, dag=dag)
task_clean = PythonOperator(task_id='clean_taxi_data', python_callable=clean_taxi_data, dag=dag)
task_transform = PythonOperator(task_id='transform_taxi_data', python_callable=transform_taxi_data, dag=dag)
task_load = PythonOperator(task_id='load_taxi_model', python_callable=load_taxi_model, dag=dag)

task_summary = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag
)

task_cleanup = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag,
    trigger_rule='all_done' # ให้รันเคลียร์ไฟล์เสมอ ไม่ว่า Task ก่อนหน้าจะพังหรือผ่าน
)

# ---------------------------------------------------------
# กำหนดลำดับการทำงาน (Dependency)
# ---------------------------------------------------------
# Ingest -> Clean -> Transform -> [Load เข้า DB, ทำ Report สรุป] -> Cleanup ลบไฟล์ทิ้ง
task_ingest >> task_clean >> task_transform >> [task_load, task_summary] >> task_cleanup