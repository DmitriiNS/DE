#Импорт необходимых библиотек
import csv
import requests
import pandas as pd
import os
from os import path
import dateutil
import datetime
import time
from datetime import datetime
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def initialization_download(api_key,path_for_download_data):

    #список акций с которыми будем работать
    stocks_list = ['META','AAPL', 'AMZN', 'NFLX', 'GOOG']

    # Для уменьшения нагрузки для оборудование (исполнение происходит не на кластере) в данном режиме используется
    # интервал 60 мин
    # Циклом проходимся по опциям API источника и загрузим данные в датафрейм пандас
    for stock_name in stocks_list:
        for num_year in range (1,2):
            for num_month in range (1,12):
                url_for_downloading = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&' \
                                      f'symbol={stock_name}&interval=60min&slice=year{num_year}month{num_month}&apikey={api_key}'
                with requests.Session() as s:
                    download = s.get(url_for_downloading)
                    decoded_content = download.content.decode('utf-8')
                    data_from_csv = csv.reader(decoded_content.splitlines(), delimiter=',')
                    temp_data_list = list(data_from_csv)

                    # В связи с ограничениями API необходимо приостанавливать запросы на 1 мин
                    time.sleep(60)

                    # Полученные данные в формате списков помещаем в ДатаФрейм
                    df=pd.DataFrame(temp_data_list,columns=['time','open','high','low','close','volume'])
                    df.drop(index=df.index [0], axis= 0 , inplace= True )

                    # В столбце time есть дата которая нам необходима для формирования файла, поэтому разделим
                    # данные и перенесем дату в отдельный столбец Date, а интервал времени сделаем
                    #отдельным столбцом
                    new_df = df['time'].str.split(' ',expand=True)
                    new_df.columns = ['date','time_interval']
                    df = pd.concat([df,new_df],axis=1)
                    df.drop('time', axis= 1 , inplace= True )
                    df=df[['date','time_interval','open','high','low','close','volume']]

                    # Директория хранения исходных файлов /data/ . Имена файлов в формате "ГГГГ-ММ-ТикетАкции"
                    str_wiht_date = str(df['date'][len(df)])
                    date_in_file_name =str_wiht_date[ :str_wiht_date.rfind("-")]

                    file_name = f'{date_in_file_name}-{stock_name}.csv'

                    #Если файлы уже существуют, то удаляем их: вероятно они повреждены, если мы загружаем их снова
                    if path.exists(f'{path_for_download_data}{file_name}') == True:
                        os.remove(f'{path_for_download_data}{file_name}')
                    df.to_csv(f'{path_for_download_data}{file_name}')
                    #df.head()# Можно посмотреть что уходит в csv файлы

# Аргументы для инициализируещего dag: он запускается однократно, поэтому в расписание не вносится
dag_ini_down_args = {
    'owner': 'Dmitrii',
    'depends_on_past': False,
    'email': ['shutkov.dmitry2013@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': 'None',
}

dag = DAG(
    dag_id='initialization_download',
    start_date=pendulum.datetime(2022, 12, 30, tz='Europe/Moscow'),
    catchup=False,
    default_args=dag_ini_down_args
)

#Секретный ключ APIKEY берется из отдельного файла с целью защиты информации от несанцкионированного доступа
with open('/app/api-key.txt',mode="r",encoding='utf-8') as file:
    api_key = file.readline().strip()

#Проверка что директория для хранения "сырых" данных существует, если нет - создается
if path.exists('/app/data') == False:
    os.mkdir('/app/data')
path_for_download_data = '/app/data/'

initialization_download = PythonOperator(
    task_id='initialization_download',
    python_callable=initialization_download,
    op_kwargs={'api_key': api_key, 'path_for_download_data': path_for_download_data},
    dag=dag
)

initialization_download