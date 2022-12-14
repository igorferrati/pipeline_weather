from airflow import DAG
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
from os.path import join
import pandas as pd


with DAG(
    'dados_climáticos',
    start_date = pendulum.datetime(2022, 11, 28, tz='UTC'),
    schedule_interval = '0 0 * * 1',    #executa toda segunda feira
    #0 0 * * 1 = 0 hora, 0 min, * dias, * mes, 1 segunda feira
) as dag:

    task_1 = BashOperator(
        task_id = 'criar_pasta', #nome task
        bash_command= 'mkdir -p "/home/igor/Documents/airflowalura/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extrai_dados(data_interval_end):

        city = 'Boston'
        key = 'UXQAWQQX5X9P8586HTKD6QHNA'

        URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
            f"{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv")

        dados = pd.read_csv(URL)

        file_path = f'/home/igor/Documents/airflowalura/semana={data_interval_end}/'

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + ' condicoes.csv')



    task_2 = PythonOperator(
        task_id = 'extrai dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    task_1 >> task_2
