# Histórico de Mudanças - Adventure Works Analytics

## Sobre este Documento
Este arquivo documenta todas as mudanças significativas do projeto Adventure Works Analytics, seguindo as práticas de [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) e [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.0.0] - 2025-07-10

### Resumo da Versão
Versão principal com foco em dimensões analíticas avançadas e melhorias de infraestrutura.

### Novas Funcionalidades

#### Dimensões Analíticas
- **dim_customers_enhanced**: Análise de Customer Lifetime Value
  - Segmentação VIP/Premium/Regular/Básico
  - Métricas de valor do cliente
  - Análise de comportamento de compra

- **dim_products_performance**: Análise de ciclo de vida do produto
  - Classificação em Crescimento/Maturidade/Declínio/Descontinuado
  - Métricas de performance por produto
  - Indicadores de rentabilidade

- **dim_territories_performance**: Análise de ROI territorial
  - Rankings de eficiência por território
  - Métricas de performance regional
  - Comparativos de mercado

- **dim_channels_performance**: Performance de canais de venda
  - Análise Online vs Revenda
  - Segmentação por valor
  - Eficiência de cada canal

- **dim_product_associations**: Análise de cesta de mercado
  - Métricas de lift, confiança e suporte
  - Recomendações de produtos
  - Padrões de compra associada

#### Qualidade e Testes
- **45+ testes automatizados** implementados
- **3 testes customizados de negócio**:
  - `assert_positive_clv`: Validação de valores CLV positivos
  - `assert_valid_lifecycle_stages`: Verificação de estágios válidos
  - `assert_valid_lift_values`: Validação de valores de lift
- **100% de cobertura** nas novas dimensões

#### Infraestrutura e CI/CD
- **Pipeline completo GitHub Actions**:
  - Testes automatizados em cada PR
  - Deploy multi-ambiente (dev → staging → produção)
  - Verificações de segurança
  - Geração automática de documentação

- **Integração Databricks aprimorada**:
  - Configuração otimizada de warehouse
  - Tratamento robusto de erros
  - Compatibilidade Spark SQL

### Melhorias Técnicas

#### Performance
- **Otimização de consultas SQL**
- **Joins e agregações melhoradas**
- **Tempo de execução mantido em ~1 minuto**

#### Padronização
- **Convenções de nomenclatura uniformes**
- **Estrutura de código consistente**
- **Documentação padronizada**

#### Compatibilidade
- **Funções de data adequadas para Spark SQL**
- **Uso de ADD_MONTHS ao invés de DATE_SUB**
- **Tipos de dados otimizados**

### Correções de Bugs
- **Problemas de conexão Databricks** resolvidos
- **Erros de referência de coluna** corrigidos
- **Falhas de teste por incompatibilidade** solucionadas
- **Issues de performance** otimizadas

### Documentação Atualizada
- **README.md** reescrito com estrutura profissional
- **Diretrizes de contribuição** detalhadas
- **Documentação de arquitetura** com diagramas
- **Documentação interativa dbt** com linhagem

---

## [1.0.0] - 2025-07-07

### Resumo da Versão
Versão inicial da plataforma Adventure Works Analytics com arquitetura medallion completa.

### Funcionalidades Principais

#### Arquitetura de Dados
- **Camada Bronze (Staging)**:
  - Limpeza de dados brutos
  - Padronização de formatos
  - Validações iniciais

- **Camada Silver (Intermediate)**:
  - Aplicação de lógica de negócio
  - Enriquecimento de dados
  - Transformações complexas

- **Camada Gold (Marts)**:
  - Fatos e dimensões finais
  - Dados prontos para consumo
  - Otimizados para analytics

#### Modelos de Dados

##### Tabelas Fato
- **fact_sales_transactions**: Transações de venda detalhadas
- **fact_sales_monthly_agg**: Agregações mensais de vendas
- **fact_territorial_performance**: Performance territorial trimestral

##### Dimensões
- **dim_customer**: Dados mestres de clientes com demografia
- **dim_product**: Catálogo completo de produtos
- **dim_date**: Dimensão temporal com períodos fiscais

#### Infraestrutura

##### Orquestração
- **Apache Airflow** configurado com Docker Compose
- **DAGs** para execução automatizada
- **Monitoramento** de pipeline

##### Armazenamento
- **Databricks** como data warehouse principal
- **Delta Lake** para confiabilidade de dados
- **Integração** com fontes múltiplas

##### Ambientes
- **Desenvolvimento** local com Docker
- **Staging** para testes
- **Produção** no Databricks

#### Fontes de Dados
- **Adventure Works DW**: Sistema principal
- **Adventure Works API**: Dados em tempo real
- **Cobertura completa**: Vendas, clientes, produtos, territórios

### Configuração Técnica

#### Stack Tecnológico
- **dbt 1.6.14** com adapter Databricks
- **Python 3.8+** em ambiente virtual
- **Docker & Docker Compose** para containerização
- **Git** com workflow GitFlow

#### Testes Implementados
- **Testes de schema** para validação estrutural
- **Testes de lógica de negócio** para precisão
- **Testes de atualização** para monitoramento

---

## Estatísticas do Projeto

### Versão 2.0.0
| Métrica | Valor |
|---------|-------|
| Dimensões analíticas | 5 novas |
| Testes automatizados | 45+ |
| Taxa de sucesso | 100% |
| Tempo de execução | ~1 minuto |
| Workflows CI/CD | 6 |

### Versão 1.0.0
| Métrica | Valor |
|---------|-------|
| Tabelas fato | 2 |
| Dimensões principais | 3 |
| Modelos staging | 15+ |
| Integração | Databricks + Airflow |

---

## Informações do Projeto

### Contribuidor Principal
**Diego Brito** - Engenheiro de Dados Lead  
GitHub: [@DiegoIndicium](https://github.com/DiegoIndicium)

### Repositório
**URL**: [github.com/DiegoIndicium/aw_analytics_lhcp3](https://github.com/DiegoIndicium/aw_analytics_lhcp3)  
**Tipo**: Repositório privado  
**Licença**: Proprietária

### Versões Disponíveis
- **v2.0.0**: Release Dimensões Analíticas Avançadas
- **v1.0.0**: Release Inicial da Plataforma

---

## Referências
- [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
- [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
- [GitHub Releases](https://github.com/DiegoIndicium/aw_analytics_lhcp3/releases)
