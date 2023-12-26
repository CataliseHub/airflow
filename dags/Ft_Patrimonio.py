from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import requests
import json

def e_load(**kwargs):
    webhook_url = "https://tinyurl.com/ylrbmlto"
    mensagem = {"text": f"Teste Ok: Disparado pelo Airflow."}

    try:
        requests.post(webhook_url, data=json.dumps(mensagem), headers={"Content-Type": "application/json"}, verify=False) 

        return "valido"
    except Exception as e:
        return "n_valido"


# Defina os argumentos padrão para a DAG
default_args = {
    'owner': 'Cassio Luan',
    'start_date': datetime(2023, 12, 19),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Inicialize a DAG
with DAG(
    "Ft_Patrimonio",
    default_args=default_args,
    description='Verifica repositório da administradora MASTER',
    schedule_interval='30 * * * *',
    catchup=False,
    tags=['Teste'],
) as dag:
    

    e_load = PythonOperator(
        task_id="e_load",
        python_callable=e_load,
        provide_context=True  # Indica que o contexto do Airflow deve ser passado para a função
    )

    valido = BashOperator(
        task_id="valido",
        bash_command="echo 'Arquivo_Gerado'"
    )

    n_valido = BashOperator(
        task_id="n_valido",
        bash_command="echo 'Falhou'"
    )

    e_load >> [valido, n_valido]
