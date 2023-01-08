#Импорт необходимых библиотек
import csv
import requests
import pandas as pd
import os
from os import path
import dateutil
import datetime
from datetime import timedelta
import pendulum
import shutil
import psycopg2 as ps
import numpy as np
import pyarrow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def incremental_download(api_key,path_for_download_data,year,month,day):

    #список акций с которыми будем работать
    stocks_list = ['META','AAPL', 'AMZN', 'NFLX', 'GOOG']

    #Циклом проходим по списку акций и загружаем "сырые" данные по API. Интервал минимальный - 1 мин
    for stock_name in stocks_list:
        url_for_downloading = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&' \
                              f'symbol={stock_name}&interval=1min&apikey={api_key}&datatype=csv'
        with requests.Session() as s:
            download = s.get(url_for_downloading)
            decoded_content = download.content.decode('utf-8')
            data_from_csv = csv.reader(decoded_content.splitlines(), delimiter=',')
            temp_data_list = list(data_from_csv)

            # Полученные данные в формате списков помещаем в ДатаФрейм
            df=pd.DataFrame(temp_data_list,columns=['time','open','high','low','close','volume'])
            df.drop(index=df.index [0], axis= 0 , inplace= True )

            # Директория хранения исходных файлов /data/ . Имена файлов в формате "ГГГГ_ММ_ДД_ТикетАкции"
            file_name = f'{year}_{month}_{day}_{stock_name}.csv'

            #Если файлы уже существуют, то удаляем их: вероятно они повреждены, если мы загружаем их снова
            if path.exists(f'{path_for_download_data}{file_name}') == True:
                os.remove(f'{path_for_download_data}{file_name}')
            df.to_csv(f'{path_for_download_data}{file_name}')

def trans_and_load_to_postgres(path_for_download_data,year,month,day):

    #список акций с которыми будем работать
    stocks_list = ['META','AAPL', 'AMZN', 'NFLX', 'GOOG']

    for stock_name in stocks_list:
        file_name = f'{year}_{month}_{day}_{stock_name}.csv'

        # Загрузка данных из файлов csv в датафреймы
        df = pd.read_csv(f'{path_for_download_data}{file_name}')
        df.drop(columns=df.columns [0], axis= 0 , inplace= True )

        # В таблице отсутствует наименование компании - добавляем
        df.insert(0,'stock_name',stock_name,False)

        # В столбце time есть только дата и время, но нет интервала, поэтому разделим
        # данные и перенесем дату в отдельный столбец Date, а интервал времени сделаем
        #отдельным столбцом: время начала из нижней строки, время окончания - текущая строка
        new_df = df['time'].str.split(' ',expand=True)
        new_df.columns = ['date','time_interval']
        df = pd.concat([df,new_df],axis=1)
        df.drop('time', axis= 1 , inplace= True )
        df=df[['stock_name','date','time_interval','open','high','low','close','volume']]

        prev_time_interval = df['time_interval'].shift(-1).fillna(np.nan)
        df_t_i = prev_time_interval.to_frame(name='time_interval')

        df_t_i['time_interval1'] = df_t_i['time_interval'] + " " + df['time_interval']
        df_t_i.drop('time_interval', axis= 1 , inplace= True)
        df = pd.concat([df,df_t_i],axis=1)

        #Из-за смещения появились Nan - заменим их на реальное время
        num_of_line_nan = df.shape[0] - 1
        df_without_nan = df.loc[:, ['time_interval1']]
        right_value = df['time_interval'][num_of_line_nan] + " " + df['time_interval'][num_of_line_nan]
        df_without_nan['time_interval1'][num_of_line_nan] = right_value
        df.drop('time_interval1', axis= 1 , inplace= True)

        # Формируем окончательный датафрейм для выгрузки в базу postgres
        df = pd.concat([df,df_without_nan],axis=1)
        df=df[['stock_name','date','time_interval1','open','high','low','close','volume']]
        df.rename(columns={'time_interval1': 'time_interval'}, inplace=True)

        # Подключаемся к базе postgres
        conn = ps.connect(database='final_2', user='airflow',
                          password='airflow', host='localhost', port='65432')

        # Загружаем данные в базу postgres
        for index, row in df.iterrows():
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO stock_rates 
                (stock_name,
                 date,
                 time_interval,
                 open,
                 high,
                 low,
                 close,
                 volume)
                 VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
                [row.stock_name,row.date,row.time_interval,row.open,row.high,row.low,row.close,row.volume]
            )

        # Загрузка окончена, закрываем соединение
        conn.commit()
        conn.close()

def upload_to_data_marts():

    #список акций с которыми будем работать
    stocks_list = ['META','AAPL', 'AMZN', 'NFLX', 'GOOG']
    # Параметры соединения с базой Postgres в airflow доккера
    param_dic = {
        "host"      : "localhost",
        "database"  : "final_2",
        "user"      : "airflow",
        "password"  : "airflow",
        "port"      :  '65432'
    }
    def connect(params_dic):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # connect to the PostgreSQL server
            conn = ps.connect(**params_dic)
        except (Exception, ps.DatabaseError) as error:
            #print(error)
            sys.exit(1)
            #print("Connection successful")
        return conn

    # Приведение параметров к необходимому формату для запроса к postgres
    today = datetime.date.today()
    previous_date = today - dateutil.relativedelta.relativedelta(days=1)
    date =str("'"+str(datetime.datetime.strftime(previous_date,'%Y-%m-%d'))+"'")

    # Обработка в цикле, тк акций несколько
    for stock_name in stocks_list:

        #Приведение тикета акции к формату в таблице postgres
        stock_name = "'"+str.upper(stock_name)+"'"

        # Функция трансформации запроса к postgres в pandas dataframe
        def postgresql_to_dataframe(conn, select_query, column_names):
            cursor = conn.cursor()
            try:
                cursor.execute(select_query)
            except (Exception, ps.DatabaseError) as error:
                print("Error: %s" % error)
                cursor.close()
                return 1

            # Получаем на выходе list of tupples
            tupples = cursor.fetchall()
            conn.commit()
            conn.close()

            # Передача данных в pandas dataframe
            df = pd.DataFrame(tupples, columns=column_names)
            return df

        # В выходном датафрейме именуем колонки
        column_names = ['sum_volume','open_value','close_value','difference_percent','time_MAX_vol','time_MAX_price','time_LOW_price']

        # Выполнение query
        conn = connect(param_dic)
        df = postgresql_to_dataframe(conn, f'ALTER TABLE stock_rates ' \
                                           f'ALTER COLUMN volume ' \
                                           f'TYPE numeric(10,2)  ' \
                                           f'USING volume::numeric(10,2); ' \
                                           f'ALTER TABLE stock_rates  ' \
                                           f'ALTER COLUMN close  ' \
                                           f'TYPE numeric(10,2) ' \
                                           f'USING close::numeric(10,2);' \
                                           f'ALTER TABLE stock_rates ' \
                                           f'ALTER COLUMN open ' \
                                           f'TYPE numeric(10,2) ' \
                                           f'USING open::numeric(10,2); ' \
                                           f'WITH q_sum_volume AS ( ' \
                                           f'    SELECT SUM(volume) AS sum_volume ' \
                                           f'    FROM stock_rates ' \
                                           f'    WHERE stock_name = {stock_name} ' \
                                           f'   AND date = {date} ' \
                                           f'   ), ' \
                                           f'   q_open_value AS (  ' \
                                           f'  SELECT open AS open_value  ' \
                                           f'   FROM stock_rates ' \
                                           f'   WHERE stock_name = {stock_name}  ' \
                                           f'   AND date = {date}  ' \
                                           f'   ORDER BY time_interval  ' \
                                           f'   LIMIT 1 ' \
                                           f'   ), ' \
                                           f'  q_close_value AS ( ' \
                                           f'   SELECT close AS close_value  ' \
                                           f'   FROM stock_rates  ' \
                                           f'   WHERE stock_name = {stock_name}  ' \
                                           f'   AND date = {date}  ' \
                                           f'   LIMIT 1 ' \
                                           f'  ), ' \
                                           f'   q_difference_percent AS ( ' \
                                           f'   WITH one AS  ' \
                                           f'       (SELECT open AS open_value  ' \
                                           f'       FROM stock_rates ' \
                                           f'       WHERE stock_name = {stock_name} ' \
                                           f'       AND date = {date}  ' \
                                           f'      ORDER BY time_interval  ' \
                                           f'     LIMIT 1), ' \
                                           f'     two AS  ' \
                                           f'      (SELECT close AS close_value  ' \
                                           f'     FROM stock_rates  ' \
                                           f'     WHERE stock_name = {stock_name}  ' \
                                           f'      AND date = {date}  ' \
                                           f'      LIMIT 1) ' \
                                           f'   SELECT ROUND ((one.open_value-two.close_value)/one.open_value*100,2)  ' \
                                           f'      AS difference_percent  ' \
                                           f'   FROM one,two ' \
                                           f'   ), ' \
                                           f'  q_time_MAX_vol AS ( ' \
                                           f'   SELECT time_interval AS time_MAX_vol  ' \
                                           f'   FROM stock_rates  ' \
                                           f'   WHERE stock_name = {stock_name}  ' \
                                           f'   AND date = {date}  ' \
                                           f'   ORDER BY volume DESC  ' \
                                           f'   LIMIT 1 ' \
                                           f'   ), ' \
                                           f'   q_time_MAX_price AS ( ' \
                                           f'   SELECT time_interval AS time_MAX_price  ' \
                                           f'   FROM stock_rates  ' \
                                           f'   WHERE stock_name = {stock_name} ' \
                                           f'   AND date = {date}  ' \
                                           f'   ORDER BY high DESC  ' \
                                           f'   LIMIT 1 ' \
                                           f'   ), ' \
                                           f'   q_time_LOW_price AS ( ' \
                                           f'   SELECT time_interval AS time_LOW_price  ' \
                                           f'   FROM stock_rates  ' \
                                           f'   WHERE stock_name = {stock_name}  ' \
                                           f'   AND date = {date}  ' \
                                           f'   ORDER BY low   ' \
                                           f'   LIMIT 1 ' \
                                           f'   ) ' \
                                           f'   SELECT q_sum_volume.sum_volume, ' \
                                           f'      q_open_value.open_value, ' \
                                           f'      q_close_value.close_value, ' \
                                           f'      q_difference_percent.difference_percent, ' \
                                           f'      q_time_MAX_vol.time_MAX_vol, ' \
                                           f'      q_time_MAX_price.time_MAX_price, ' \
                                           f'      q_time_LOW_price.time_LOW_price ' \
                                           f'   FROM q_sum_volume, ' \
                                           f'    q_open_value, ' \
                                           f'    q_close_value, ' \
                                           f'    q_difference_percent, ' \
                                           f'    q_time_MAX_vol, ' \
                                           f'   q_time_MAX_price, ' \
                                           f'   q_time_LOW_price', column_names)

        # В таблице отсутствует дата - добавляем
        df.insert(0,'date',date,False)

        # В таблице отсутствует наименование компании - добавляем
        df.insert(0,'stock_name',stock_name,False)

        # Выгрузка датафрейм в файл parquet
        df.to_parquet(f'/app/data marts/datamart_{stock_name}_{date}.parquet')

dag_inc_download_args = {
    'owner': 'Dmitrii',
    'depends_on_past': False,
    'email': ['shutkov.dmitry2013@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@dayly',

}

dag = DAG(
    dag_id='Dayly_ETL_stock_rates',
    start_date=pendulum.datetime(2022, 12, 30, tz='Europe/Moscow'),
    schedule_interval='30 9 * * *',
    catchup=False,
    default_args=dag_inc_download_args
)

#Секретный ключ APIKEY берется из отдельного файла с целью защиты информации от несанцкионированного доступа
with open('/app/api-key.txt',mode="r",encoding='utf-8') as file:
    api_key = file.readline().strip()

#Проверка что директория для хранения "сырых" данных существует, если нет - создается
if path.exists('/app/data') == False:
    os.mkdir('/app/data')
path_for_download_data = '/app/data/'

#Определение дат для имени файла

today = datetime.date.today()
previous_date = today - dateutil.relativedelta.relativedelta(days=1)
#Использование встроенной переменной airflow {{ yesterday_ds }} не работает
#previous_date = {{ yesterday_ds }}
year = previous_date.year
month = previous_date.month
day = previous_date.day

# Определение python operator для task incremental_download
incremental_download = PythonOperator(
    task_id='incremental_download',
    python_callable=incremental_download,
    op_kwargs={'api_key': api_key, 'path_for_download_data': path_for_download_data,'year': year,'month': month, 'day': day},
    dag=dag
)

# Определение python operator для task trans_and_load_to_postgres
trans_and_load_to_postgres = PythonOperator(
    task_id='trans_and_load_to_postgres',
    python_callable=trans_and_load_to_postgres,
    op_kwargs={'path_for_download_data': path_for_download_data,'year': year,'month': month, 'day': day},
    dag=dag
)

# Определение python operator для task upload_to_data_marts
upload_to_data_marts = PythonOperator(
    task_id='upload_to_data_marts',
    python_callable=upload_to_data_marts,
    op_kwargs={},
    dag=dag
)

incremental_download>>trans_and_load_to_postgres>>upload_to_data_marts