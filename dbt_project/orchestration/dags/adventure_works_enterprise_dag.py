"""
Adventure Works Analytics Pipeline - Enterprise Edition
Usando Astro CLI + dbt + Databricks

Autor: Diego Brito
Data: 2025-01-15
Versão: 2.0 - Astro Enterprise
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
import logging

# Configurações otimizadas para Astro
default_args = {
    'owner': 'diego.brito@indicium.tech',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['diego.brito@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1,
    'max_active_tasks': 16,
}

# DAG com configurações empresariais
dag = DAG(
    'adventure_works_analytics_enterprise',
    default_args=default_args,
    description='Pipeline Analytics Adventure Works - Enterprise com Astro + dbt + Databricks',
    schedule_interval='0 6 * * *',  # Diário às 6h
    catchup=False,
    doc_md=__doc__,
    tags=['adventure-works', 'dbt', 'databricks', 'enterprise', 'astro'],
)

# Função para logging avançado
def log_pipeline_start(**context):
    logging.info(f"🚀 Iniciando Pipeline Adventure Works Enterprise - Run: {context['run_id']}")
    logging.info(f"📅 Data de execução: {context['ds']}")
    logging.info(f"🏗️ Ambiente: Astro CLI + dbt + Databricks")
    logging.info(f"👤 Executado por: {context['dag'].owner}")
    return "Pipeline iniciado com sucesso"

def log_pipeline_end(**context):
    logging.info(f"🎉 Pipeline Adventure Works Enterprise concluído com sucesso!")
    logging.info(f"📅 Data de execução: {context['ds']}")
    logging.info(f"🏗️ Ambiente: Astro CLI + dbt + Databricks")
    logging.info(f"✅ Status: Pipeline executado com sucesso")
    return "Pipeline concluído"

# Configurações de diretório dbt
DBT_PROJECT_DIR = '/opt/airflow/dbt_project'
DBT_PROFILES_DIR = '/opt/airflow/dbt_project'

# Tasks principais
iniciar_pipeline = PythonOperator(
    task_id='iniciar_pipeline_enterprise',
    python_callable=log_pipeline_start,
    dag=dag,
)

verificar_ambiente = BashOperator(
    task_id='verificar_ambiente_dbt',
    bash_command=f'''
    echo "🔍 Verificando ambiente dbt..." && \\
    dbt --version && \\
    echo "✅ Ambiente dbt verificado"
    ''',
    dag=dag,
)

executar_staging = BashOperator(
    task_id='executar_modelos_staging',
    bash_command=f'''
    echo "🔄 Executando modelos staging..." && \\
    echo "✅ Modelos staging executados (simulado)"
    ''',
    dag=dag,
)

executar_intermediate = BashOperator(
    task_id='executar_modelos_intermediate',
    bash_command=f'''
    echo "🔄 Executando modelos intermediate..." && \\
    echo "✅ Modelos intermediate executados (simulado)"
    ''',
    dag=dag,
)

executar_marts = BashOperator(
    task_id='executar_modelos_marts',
    bash_command=f'''
    echo "🔄 Executando modelos marts..." && \\
    echo "✅ Modelos marts executados (simulado)"
    ''',
    dag=dag,
)

executar_testes = BashOperator(
    task_id='executar_testes_dbt',
    bash_command=f'''
    echo "🧪 Executando testes de qualidade..." && \\
    echo "✅ Testes de qualidade executados (simulado)"
    ''',
    dag=dag,
)

gerar_docs = BashOperator(
    task_id='gerar_documentacao_dbt',
    bash_command=f'''
    echo "📚 Gerando documentação..." && \\
    echo "✅ Documentação gerada (simulado)"
    ''',
    dag=dag,
)

finalizar_pipeline = PythonOperator(
    task_id='finalizar_pipeline_enterprise',
    python_callable=log_pipeline_end,
    dag=dag,
)

# Notificação de sucesso
notificacao_sucesso = DummyOperator(
    task_id='notificacao_sucesso_enterprise',
    dag=dag,
)

# Definir dependências
iniciar_pipeline >> verificar_ambiente >> executar_staging >> executar_intermediate >> executar_marts >> executar_testes >> gerar_docs >> finalizar_pipeline >> notificacao_sucesso