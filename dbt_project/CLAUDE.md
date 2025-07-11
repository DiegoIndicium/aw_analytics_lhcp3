# CLAUDE.md

Este arquivo fornece orientações ao Claude Code (claude.ai/code) ao trabalhar com código neste repositório.

## Visão Geral do Projeto

Este é um projeto **dbt (Data Build Tool)** para analytics Adventure Works, implementando uma arquitetura moderna de data warehouse com padrão medallion (staging → intermediate → marts). O projeto transforma dados brutos Adventure Works de fontes de data warehouse e API em modelos limpos e prontos para negócio.

## Arquitetura

### Estrutura das Camadas
- **Staging** (`models/staging/`): Limpeza e padronização de dados brutos
  - `api_sales/`: Dados de vendas da API
  - `products/`: Dados mestres de produtos com categorias
  - `sales/`: Transações de vendas, clientes, territórios
- **Intermediate** (`models/intermediate/`): Lógica de negócio e joins
  - `int_sales__enriched.sql`: Fato de vendas abrangente com todas as dimensões
  - `int_products__hierarchy.sql`: Hierarquias de categorias de produtos
- **Marts** (`models/marts/`): Modelos finais prontos para negócio
  - `products/`: Analytics de produtos
  - `sales/`: Analytics de vendas

### Fontes de Dados
- **adventure_works_dw**: Tabelas principais do data warehouse (database: `ted_dev`, schema: `dev_diego_brito`)
- **adventure_works_api**: Dados recentes/validação da API

### Convenções de Nomenclatura
- `stg_` = modelos staging
- `int_` = modelos intermediate
- `fact_` = tabelas fato (marts)
- `dim_` = tabelas dimensão (marts)

## Comandos de Desenvolvimento

### Comandos dbt Principais
```bash
# Executar todos os modelos
dbt run

# Executar modelo específico
dbt run --select stg_sales__customer

# Executar modelos com dependências
dbt run --select +int_sales__enriched

# Testar todos os modelos
dbt test

# Testar modelo específico
dbt test --select stg_sales__customer

# Gerar documentação
dbt docs generate
dbt docs serve

# Verificar dependências do modelo
dbt deps

# Limpar diretório target
dbt clean
```

### Fluxo de Desenvolvimento
```bash
# Ciclo completo de desenvolvimento
dbt clean && dbt deps && dbt run && dbt test

# Desenvolvimento incremental
dbt run --select +nome_modelo && dbt test --select nome_modelo
```

## Configuração Principal

- **Profile**: `adventure_works_analytics`
- **Schema de destino**: Usa configuração `+schema` (staging, intermediate, marts)
- **Variável de dados brutos**: `raw_schema: "ted_dev.dev_diego_brito"`
- **Estratégia de materialização**:
  - Staging: `view`
  - Intermediate: `view`
  - Marts: `table`

## Relacionamentos Importantes entre Modelos

- `int_sales__enriched` é o fato de vendas principal enriquecido unindo todas as dimensões
- Modelos de hierarquia de produtos usam relacionamentos auto-referenciados para árvores de categoria
- Todos os modelos staging referenciam sources definidos em `models/sources/sources.yml`
- Classificação de clientes distingue segmentos B2B vs B2C
- Modelos de território incluem métricas de performance e agrupamentos regionais

## Configuração do Ambiente

O projeto usa um ambiente virtual com dbt instalado em `/home/diego/adventure-works-analytics/venv/bin/dbt`.