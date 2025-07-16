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
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging
import os

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
    logging.info(f"⏱️ Duração total: {context['dag_run'].end_date - context['dag_run'].start_date}")
    return "Pipeline concluído"

# Configurações de diretório dbt
DBT_PROJECT_DIR = '/usr/local/airflow/include/dbt_project'
DBT_PROFILES_DIR = '/usr/local/airflow/include/dbt_project/profiles'

# Task Groups para organização empresarial
with TaskGroup("setup_pipeline", dag=dag) as setup_group:
    iniciar_pipeline = PythonOperator(
        task_id='iniciar_pipeline_enterprise',
        python_callable=log_pipeline_start,
        doc_md="Inicialização do pipeline com logging avançado",
    )
    
    verificar_ambiente = BashOperator(
        task_id='verificar_ambiente_dbt',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \\
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \\
        echo "🔍 Verificando ambiente dbt..." && \\
        dbt --version && \\
        echo "🔗 Testando conexão com Databricks..." && \\
        dbt debug && \\
        echo "✅ Ambiente dbt verificado e conectado"
        ''',
        doc_md="Verificação do ambiente dbt e conectividade com Databricks",
    )

with TaskGroup("dbt_setup", dag=dag) as dbt_setup_group:
    limpar_ambiente_dbt = BashOperator(
        task_id='limpar_ambiente_dbt',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \\
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \\
        echo "🧹 Limpando ambiente dbt..." && \\
        dbt clean && \\
        echo "✅ Ambiente limpo"
        ''',
        doc_md="Limpeza do ambiente dbt (target, logs, etc.)",
    )
    
    instalar_dependencias_dbt = BashOperator(
        task_id='instalar_dependencias_dbt',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \\
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \\
        echo "📦 Instalando dependências dbt..." && \\
        dbt deps && \\
        echo "✅ Dependências instaladas com sucesso"
        ''',
        doc_md="Instalação de dependências dbt (packages.yml)",
    )

with TaskGroup("dbt_staging", dag=dag) as staging_group:
    executar_modelos_staging = BashOperator(
        task_id='executar_modelos_staging',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \\
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \\
        echo "🔄 Executando modelos staging..." && \\
        dbt run --select staging --full-refresh && \\
        echo "✅ Modelos staging executados com sucesso"
        ''',
        doc_md="Execução de todos os modelos da camada staging",
    )

with TaskGroup("dbt_intermediate", dag=dag) as intermediate_group:
    executar_modelos_intermediate = BashOperator(
        task_id='executar_modelos_intermediate',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \\
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \\
        echo "🔄 Executando modelos intermediate..." && \\
        dbt run --select intermediate && \\
        echo "✅ Modelos intermediate executados com sucesso"
        ''',
        doc_md="Execução de todos os modelos da camada intermediate",
    )

with TaskGroup("dbt_marts", dag=dag) as marts_group:
    executar_modelos_marts = BashOperator(
        task_id='executar_modelos_marts',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \\
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \\
        echo "🔄 Executando modelos marts..." && \\
        dbt run --select marts && \\
        echo "✅ Modelos marts executados com sucesso"
        ''',
        doc_md="Execução de todos os modelos da camada marts (dimensões e fatos)",
    )

with TaskGroup("quality_assurance", dag=dag) as qa_group:
    executar_testes_dbt = BashOperator(
        task_id='executar_testes_dbt',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \\
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \\
        echo "🧪 Executando testes de qualidade..." && \\
        dbt test --store-failures && \\
        echo "✅ Testes de qualidade executados com sucesso"
        ''',
        doc_md="Execução de todos os testes de qualidade de dados",
    )
    
    gerar_documentacao_dbt = BashOperator(
        task_id='gerar_documentacao_dbt',
        bash_command=f'''
        cd {DBT_PROJECT_DIR} && \\
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \\
        echo "📚 Gerando documentação..." && \\
        dbt docs generate && \\
        echo "✅ Documentação gerada com sucesso"
        ''',
        doc_md="Geração da documentação dbt (HTML)",
    )

# Finalização do pipeline
finalizar_pipeline = PythonOperator(
    task_id='finalizar_pipeline_enterprise',
    python_callable=log_pipeline_end,
    doc_md="Finalização do pipeline com logging de métricas",
    dag=dag,
)

# Notificação de sucesso
notificacao_sucesso = EmailOperator(
    task_id='notificacao_sucesso_enterprise',
    to=['diego.brito@indicium.tech'],
    subject='✅ Pipeline Adventure Works Enterprise - Executado com Sucesso',
    html_content='''
    <h2>🎉 Pipeline Adventure Works Analytics Enterprise</h2>
    <p><strong>Status:</strong> ✅ Executado com Sucesso</p>
    <p><strong>Data:</strong> {{ ds }}</p>
    <p><strong>Ambiente:</strong> Astro CLI + dbt + Databricks</p>
    <p><strong>Duração:</strong> {{ dag_run.end_date - dag_run.start_date }}</p>
    
    <h3>📊 Resumo da Execução:</h3>
    <ul>
        <li>✅ Setup e verificação do ambiente</li>
        <li>✅ Staging models executados</li>
        <li>✅ Intermediate models executados</li>
        <li>✅ Marts models executados</li>
        <li>✅ Testes de qualidade executados</li>
        <li>✅ Documentação gerada</li>
    </ul>
    
    <h3>🏗️ Arquitetura:</h3>
    <p>🔹 <strong>Orquestração:</strong> Apache Airflow (Astro CLI)</p>
    <p>🔹 <strong>Transformação:</strong> dbt (Data Build Tool)</p>
    <p>🔹 <strong>Data Platform:</strong> Databricks</p>
    <p>🔹 <strong>Qualidade:</strong> Testes automatizados</p>
    
    <p>🚀 <strong>Powered by:</strong> Astro CLI + Apache Airflow + dbt + Databricks</p>
    <p>👨‍💻 <strong>Desenvolvido por:</strong> Diego Brito</p>
    ''',
    dag=dag,
)

# Definir dependências com Task Groups - Fluxo empresarial
setup_group >> dbt_setup_group >> staging_group >> intermediate_group >> marts_group >> qa_group >> finalizar_pipeline >> notificacao_sucesso

# Dependências internas nos grupos
iniciar_pipeline >> verificar_ambiente
limpar_ambiente_dbt >> instalar_dependencias_dbt