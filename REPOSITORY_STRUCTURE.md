```markdown
# Estrutura do Repositório - Adventure Works Analytics

## Organização de Diretórios

### Estrutura Principal
adventure-works-analytics/
├── .github/                    # Configuração GitHub
├── dbt_project/               # Projeto principal dbt
├── orchestration/             # Orquestração Airflow
├── docs/                      # Documentação do projeto
├── scripts/                   # Scripts utilitários
├── environments/              # Configurações de ambiente
├── tests/                     # Testes de nível de projeto
└── arquivos de configuração raiz

## Detalhamento por Diretório

### .github/
**Configurações do GitHub e CI/CD**

#### workflows/
- `dbt-ci.yml`: Integração contínua do dbt
- `dbt-docs.yml`: Deploy da documentação
- `release.yml`: Automação de releases

#### ISSUE_TEMPLATE/
- `bug_report.md`: Template para relatório de bugs
- `feature_request.md`: Template para solicitação de funcionalidades
- `data_quality_issue.md`: Template para problemas de qualidade de dados

#### Outros arquivos
- `pull_request_template.md`: Template para pull requests
- `BRANCH_STRATEGY.md`: Diretrizes de branching
- `CONTRIBUTING.md`: Guia de contribuição

### dbt_project/
**Projeto principal de transformação de dados**

#### analyses/
Consultas analíticas exploratórias:
- `customer_cohort_analysis.sql`: Análise de coorte de clientes
- `product_performance_trends.sql`: Tendências de performance de produtos
- `territory_growth_analysis.sql`: Análise de crescimento territorial

#### macros/
Macros customizadas do dbt:
- `generate_schema_name.sql`: Geração de nomes de schema
- `test_helpers.sql`: Auxiliares para testes
- `business_metrics.sql`: Métricas de negócio

#### models/
Modelos de dados organizados por camada:

**staging/** - Limpeza de dados brutos
- `api_sales/`: Dados da API de vendas
- `products/`: Dados de produtos
- `sales/`: Dados de vendas
- `sources/sources.yml`: Definição de fontes

**intermediate/** - Lógica de negócio
- `int_sales__enriched.sql`: Vendas enriquecidas
- `int_products__hierarchy.sql`: Hierarquia de produtos
- `intermediate.yml`: Documentação da camada

**marts/** - Modelos prontos para negócio
- `dimensions/`: Tabelas dimensão
  - `dim_customer.sql`: Dimensão cliente
  - `dim_product.sql`: Dimensão produto
  - `dim_date.sql`: Dimensão data
- `sales/`: Mart de vendas
  - `fact_sales_transactions.sql`: Fato transações
  - `fact_sales_monthly_agg.sql`: Agregação mensal
- Dimensões analíticas avançadas:
  - `dim_customers_enhanced.sql`: Clientes enriquecidos
  - `dim_products_performance.sql`: Performance de produtos
  - `dim_territories_performance.sql`: Performance territorial
  - `dim_channels_performance.sql`: Performance de canais
  - `dim_product_associations.sql`: Associações de produtos

#### seeds/
Dados de referência em CSV:
- `territory_mappings.csv`: Mapeamento de territórios
- `product_categories.csv`: Categorias de produtos

#### snapshots/
Rastreamento SCD Tipo 2:
- `customers_snapshot.sql`: Snapshot de clientes
- `products_snapshot.sql`: Snapshot de produtos

#### tests/
Testes customizados de dados:
- `assert_positive_clv.sql`: Validação CLV positivo
- `assert_valid_lifecycle_stages.sql`: Estágios de ciclo válidos
- `assert_valid_lift_values.sql`: Valores de lift válidos
- `assert_revenue_consistency.sql`: Consistência de receita

#### Arquivos de configuração
- `dbt_project.yml`: Configuração do projeto dbt
- `packages.yml`: Dependências de pacotes dbt
- `CLAUDE.md`: Instruções para Claude Code

### orchestration/
**Orquestração com Airflow**

#### dags/
- `adventure_works_etl.py`: DAG principal de ETL
- `data_quality_monitoring.py`: Monitoramento de qualidade

#### config/
- `profiles.yml`: Perfis de conexão
- `airflow.cfg`: Configuração do Airflow

#### Arquivos Docker
- `docker-compose.yml`: Configuração Docker
- `Dockerfile`: Definição do container
- `requirements.txt`: Dependências Python
- `.env.example`: Template de variáveis de ambiente

### docs/
**Documentação do projeto**

#### architecture/
- `data_model_diagram.md`: Diagrama do modelo de dados
- `pipeline_architecture.md`: Arquitetura do pipeline
- `security_model.md`: Modelo de segurança

#### business/
- `kpis_and_metrics.md`: KPIs e métricas
- `data_dictionary.md`: Dicionário de dados
- `use_cases.md`: Casos de uso

#### technical/
- `setup_guide.md`: Guia de configuração
- `deployment_guide.md`: Guia de deploy
- `troubleshooting.md`: Solução de problemas
- `api_documentation.md`: Documentação da API

#### images/
- `architecture_diagram.png`: Diagrama de arquitetura
- `data_lineage.png`: Linhagem de dados

### scripts/
**Scripts utilitários**

#### setup/
- `install_dependencies.sh`: Instalação de dependências
- `setup_databricks.sh`: Configuração Databricks
- `configure_profiles.sh`: Configuração de perfis

#### deployment/
- `deploy_dev.sh`: Deploy desenvolvimento
- `deploy_prod.sh`: Deploy produção
- `run_tests.sh`: Execução de testes

#### maintenance/
- `backup_data.sh`: Backup de dados
- `cleanup_logs.sh`: Limpeza de logs
- `health_check.py`: Verificação de saúde

### environments/
**Configurações por ambiente**

Cada ambiente (dev, staging, prod) contém:
- `profiles.yml`: Perfis de conexão específicos
- `env_vars.yml`: Variáveis de ambiente

### tests/
**Testes de nível de projeto**

#### integration/
- `test_data_pipeline.py`: Testes de pipeline
- `test_api_endpoints.py`: Testes de endpoints

#### unit/
- `test_transformations.py`: Testes de transformações
- `test_business_logic.py`: Testes de lógica de negócio

#### performance/
- `test_query_performance.py`: Testes de performance
- `benchmark_pipeline.py`: Benchmark do pipeline

## Arquivos de Configuração Raiz

### Git e Qualidade
- `.gitignore`: Regras de exclusão Git
- `.pre-commit-config.yaml`: Hooks de pré-commit

### Documentação
- `README.md`: Visão geral do projeto
- `CHANGELOG.md`: Histórico de versões
- `LICENSE`: Licença do projeto

### Dependências
- `requirements.txt`: Dependências Python principais

## Descrição dos Arquivos Principais

### Arquivos Core dbt
- **dbt_project.yml**: Configuração do projeto, caminhos de modelos e definições de materialização
- **packages.yml**: Dependências de pacotes externos do dbt
- **CLAUDE.md**: Instruções para assistente AI Claude Code

### Modelos de Dados
- **staging/**: Limpeza e padronização de dados brutos (views)
- **intermediate/**: Lógica de negócio e joins complexos (views)
- **marts/**: Modelos finais prontos para negócio (tabelas)

### Orquestração
- **orchestration/**: DAGs do Airflow e configuração Docker
- **dags/**: Definições de pipeline ETL e agendamento

### Documentação
- **docs/**: Documentação abrangente do projeto
- **README.md**: Guia de início rápido e visão geral

### CI/CD
- **.github/workflows/**: Testes automatizados e deploy
- **scripts/**: Scripts utilitários para configuração e manutenção

## Organização de Assets

### Por Camada - Arquitetura Medallion
- **Bronze**: `staging/` - Ingestão de dados brutos
- **Silver**: `intermediate/` - Dados limpos e unidos
- **Gold**: `marts/` - Analytics prontos para negócio

### Por Domínio
- **Sales**: Analytics de clientes, pedidos e receita
- **Products**: Catálogo, performance e ciclo de vida
- **Territories**: Performance geográfica e ROI

### Por Propósito
- **Facts**: Dados transacionais e agregados
- **Dimensions**: Atributos descritivos e hierarquias
- **Analytics**: Cálculos avançados e insights

## Configurações Específicas por Ambiente

### Ambiente de Desenvolvimento
- Configurações de banco de dados local
- Variáveis de ambiente de desenvolvimento
- Perfis de conexão para testes

### Ambiente de Staging
- Configurações de pré-produção
- Testes de integração
- Validações de qualidade

### Ambiente de Produção
- Configurações otimizadas
- Monitoramento ativo
- Backups automatizados
```
