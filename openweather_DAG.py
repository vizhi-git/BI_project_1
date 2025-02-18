from airflow import DAG
import requests
import pandas as pd
from datetime import datetime,timedelta
import boto3
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

s3_client=boto3.client('s3')
#s3 bucket
target_bucket_name='weather-transform-zone-yml'

url="https://api.openweathermap.org/data/2.5/weather?q=lcity&appid=your_api_key"
def extract_data(url):
    response=requests.get(url)
    data=response.json()
    now=datetime.now()
    date_now_string=now.strftime("%d%m%Y%H%M%S")
    file_str='weather_data_'+ date_now_string
    output_file_path=f"/tmp/{file_str}.json"
    with open(output_file_path, 'w') as file:
        pd.DataFrame([data]).to_json(file, orient='records')
    output_list=[output_file_path,file_str]
    return output_list

def transform_data(task_instance):
    r_data=task_instance.xcom_pull(task_ids="tsk_extract_data")[0]
    object_key=task_instance.xcom_pull(task_ids="tsk_extract_data")[1]
    df=pd.read_json(r_data) 
      
    def timestamp(row):
      return datetime.utcfromtimestamp(row['dt']+row['timezone'])
    
    Transformed_data={
        'country':df['sys']['country'],
        'city':df['name'],
        'temperature':df['main']['temp']-273.15,
        'humidity':df['main']['humidity'],
        'timestamp':timestamp(df.iloc[0]),
        'description':df['weather'][0]['description']
    }

    list_Transformed_data=[Transformed_data]
    df_data=pd.DataFrame(list_Transformed_data)
#convert dataframe to csv
    csv_data=df_data.to_csv()

#upload csv to s3
    object_key=f"{object_key}.csv"
    s3_client.put_object(Bucket=target_bucket_name,Key=object_key,Body=csv_data)

default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2025,2,18),
    'email':['yemail@domain.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay':timedelta(seconds=15)
}

with DAG('Open_Weather_api',
         default_args=default_args,
         catchup=False) as dag:
    
    extract_weather_data=PythonOperator(
    task_id='tsk_extract_data',
    python_callable=extract_data,
    )

    transform_weather_data=PythonOperator(
    task_id='tsk_transform_data',
    python_callable=transform_data    
    )

load_to_s3=BashOperator(
    task_id='tsk_load_to_s3',
    bash_command='aws s3 mv {{ti.xcom_pull("tsk_extract_data")[0]}} s3://store-raw-data-yml',
)

extract_data>>transform_data>>load_to_s3                                                    
