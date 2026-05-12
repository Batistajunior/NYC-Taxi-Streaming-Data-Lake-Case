# NYC Taxi Streaming Data Lake Case

## Objetivo

Este projeto implementa uma solução de Engenharia de Dados baseada no dataset **New York City Taxi Fare Prediction**, disponível no Kaggle.

O objetivo é simular uma arquitetura de streaming, onde cada linha do arquivo `train.csv` é transformada em um evento de corrida de táxi. Esses eventos são publicados em um tópico Kafka, consumidos por uma aplicação Python, armazenados em formato Parquet e posteriormente processados em uma estrutura de Data Lake com camadas **Bronze**, **Silver** e **Gold**.

A solução também inclui:

- Streaming com Kafka
- Consumer com filtros por data e localização
- Armazenamento em Parquet
- Data Lake local com camadas Bronze, Silver e Gold
- Processamento batch
- Orquestração com Airflow
- Consultas analíticas com DuckDB
- Testes unitários com Pytest
- Docker Compose para execução local

---

## Arquitetura da Solução

```text
Dataset Kaggle train.csv
        |
        v
Producer Python
        |
        v
Kafka Topic: taxi-trips
        |
        v
Consumer Python
        |
        v
Data Lake Bronze - Parquet
        |
        v
Batch Processing - Airflow
        |
        v
Data Lake Silver - Dados tratados
        |
        v
Data Lake Gold - Dados agregados
        |
        v
DuckDB / SQL Analytics
```

---

## Tecnologias Utilizadas

- Python
- Pandas
- PyArrow
- Kafka
- Kafka UI
- Docker
- Docker Compose
- Apache Airflow
- PostgreSQL para metadata do Airflow
- DuckDB
- Pytest
- Parquet

---

## Dataset

O dataset utilizado é o **New York City Taxi Fare Prediction**.

Link:

```text
https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/data
```

O arquivo necessário é:

```text
train.csv
```

Ele deve ser colocado no caminho:

```text
data/raw/train.csv
```

O dataset não é versionado no GitHub por ser um arquivo grande. Por isso, o arquivo `train.csv` deve ser baixado manualmente pelo avaliador ou pelo desenvolvedor.

---

## Estrutura do Projeto

```text
nyc-taxi-streaming-case/
│
├── README.md
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
├── pytest.ini
│
├── data/
│   ├── raw/
│   │   └── train.csv
│   └── lake/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│
├── src/
│   ├── __init__.py
│   ├── producer/
│   │   ├── __init__.py
│   │   └── producer.py
│   │
│   ├── consumer/
│   │   ├── __init__.py
│   │   └── consumer.py
│   │
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── explore_raw_data.py
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   │
│   └── utils/
│       ├── __init__.py
│       ├── config.py
│       └── logger.py
│
├── dags/
│   └── taxi_pipeline_dag.py
│
├── queries/
│   ├── consulta_por_data.sql
│   ├── consulta_por_local.sql
│   ├── metricas_gold.sql
│   ├── inspect_raw_csv.py
│   ├── run_gold_query.py
│   └── run_sql_file.py
│
├── tests/
│   ├── __init__.py
│   ├── test_transformations.py
│   └── test_filters.py
│
└── reports/
```

---

## Camadas do Data Lake

### Bronze

A camada Bronze armazena os eventos crus consumidos do Kafka em formato Parquet.

Caminho:

```text
data/lake/bronze/taxi_trips/
```

Características:

- Dados vindos diretamente do streaming
- Estrutura próxima ao evento original
- Inclui timestamp de ingestão
- Formato Parquet

---

### Silver

A camada Silver contém dados tratados, limpos e enriquecidos.

Caminho:

```text
data/lake/silver/taxi_trips/
```

Regras aplicadas:

- Conversão de `pickup_datetime` para timestamp
- Conversão de colunas numéricas
- Remoção de registros nulos em campos obrigatórios
- Remoção de corridas com `fare_amount <= 0`
- Remoção de corridas com `passenger_count <= 0`
- Filtro de coordenadas fora da região esperada de Nova York
- Criação de colunas temporais:
  - `pickup_date`
  - `pickup_year`
  - `pickup_month`
  - `pickup_day`
  - `pickup_hour`

---

### Gold

A camada Gold contém dados analíticos agregados.

Caminho:

```text
data/lake/gold/taxi_trips_daily/
```

Métricas geradas:

- Total de corridas
- Valor médio da corrida
- Receita total
- Média de passageiros

Agrupamento:

```text
pickup_date
pickup_hour
```

---

## Análise Exploratória dos Dados

Antes da construção do pipeline, foi criada uma etapa de análise exploratória do arquivo `train.csv`.

Script:

```text
src/processing/explore_raw_data.py
```

Executar:

```bash
python -m src.processing.explore_raw_data
```

Essa análise verifica:

- Colunas disponíveis no dataset
- Tipos de dados inferidos
- Primeiras linhas
- Quantidade de nulos
- Estatísticas das colunas numéricas
- Valores inválidos de corrida
- Valores inválidos de passageiros
- Faixa das coordenadas de origem e destino

A análise confirmou as seguintes colunas:

```text
key
fare_amount
pickup_datetime
pickup_longitude
pickup_latitude
dropoff_longitude
dropoff_latitude
passenger_count
```

Também foram identificados problemas de qualidade, como:

- `pickup_datetime` sendo lido como texto
- Corridas com valor menor ou igual a zero
- Corridas com zero passageiros
- Coordenadas fora do intervalo esperado para Nova York

Com base nisso, foram definidas as regras de tratamento da camada Silver.

---

## Pré-requisitos

Antes de executar o projeto, tenha instalado:

- Python 3.11 ou superior
- Docker
- Docker Compose
- Git
- Conta no Kaggle para baixar o dataset

Verifique se o Docker está funcionando:

```bash
docker --version
docker compose version
```

---

## Configuração Inicial

Clone o repositório:

```bash
git clone https://github.com/Batistajunior/NYC-Taxi-Streaming-Data-Lake-Case.git
cd NYC-Taxi-Streaming-Data-Lake-Case
```

Crie o ambiente virtual:

```bash
python -m venv .venv
```

Ative o ambiente virtual no Windows PowerShell:

```bash
.\.venv\Scripts\Activate.ps1
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

Crie o arquivo `.env` a partir do exemplo:

```bash
copy .env.example .env
```

---

## Configuração do Dataset

Baixe o arquivo `train.csv` no Kaggle:

```text
https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/data
```

Coloque o arquivo em:

```text
data/raw/train.csv
```

A estrutura deve ficar assim:

```text
data/
└── raw/
    └── train.csv
```

---

## Subindo Kafka, Kafka UI e Airflow

Suba todos os serviços com Docker Compose:

```bash
docker compose up -d
```

Verifique os containers:

```bash
docker ps
```

Containers esperados:

```text
taxi-zookeeper
taxi-kafka
taxi-kafka-ui
taxi-airflow-postgres
taxi-airflow-init
taxi-airflow-webserver
taxi-airflow-scheduler
```

---

## Acessos Locais

### Airflow UI

```text
http://localhost:8080
```

Credenciais:

```text
Usuário: admin
Senha: admin
```

### Kafka UI

```text
http://localhost:8081
```

---

## Testando o Kafka

Com os containers rodando, acesse:

```text
http://localhost:8081
```

Na interface do Kafka UI, você deve ver o cluster:

```text
local-taxi-cluster
```

O tópico utilizado pelo projeto é:

```text
taxi-trips
```

O tópico pode ser criado automaticamente quando o producer enviar os primeiros eventos.

---

## Executando o Producer

O producer lê o arquivo `train.csv` e envia cada linha como evento JSON para o Kafka.

Execute:

```bash
python -m src.producer.producer
```

---

## Executando o Consumer

Em outro terminal, execute o consumer:

```bash
python -m src.consumer.consumer
```

O consumer:

- Lê eventos do tópico Kafka `taxi-trips`
- Valida campos obrigatórios
- Aplica filtros de localização
- Grava os eventos na camada Bronze em Parquet

Caminho de saída:

```text
data/lake/bronze/taxi_trips/
```

---

## Fluxo Streaming

Ordem recomendada para testar o streaming:

Terminal 1:

```bash
docker compose up -d
```

Terminal 2:

```bash
python -m src.consumer.consumer
```

Terminal 3:

```bash
python -m src.producer.producer
```

Após alguns eventos serem consumidos, pare o consumer com:

```text
CTRL + C
```

Verifique os arquivos Parquet na Bronze:

```bash
dir data\lake\bronze\taxi_trips
```

---

## Processamento Batch Manual

Após gerar dados na Bronze, execute a transformação para Silver:

```bash
python -m src.processing.bronze_to_silver
```

Depois execute a transformação para Gold:

```bash
python -m src.processing.silver_to_gold
```

---

## Orquestração com Airflow

O Airflow é utilizado para orquestrar o processamento batch após a ingestão streaming.

A DAG está localizada em:

```text
dags/taxi_pipeline_dag.py
```

Nome da DAG:

```text
nyc_taxi_batch_pipeline
```

Tasks da DAG:

```text
bronze_to_silver
        ↓
silver_to_gold
```

A DAG executa:

1. Leitura da camada Bronze
2. Tratamento e geração da camada Silver
3. Agregação e geração da camada Gold

---

## Executando a DAG pelo Airflow UI

Acesse:

```text
http://localhost:8080
```

Depois:

1. Localize a DAG `nyc_taxi_batch_pipeline`
2. Ative a DAG
3. Clique em **Trigger DAG**
4. Acompanhe pelas abas **Grid** ou **Graph**

---

## Executando a DAG pelo Terminal

Também é possível executar a DAG pela linha de comando:

```bash
docker exec -it taxi-airflow-scheduler airflow dags unpause nyc_taxi_batch_pipeline
```

Disparar execução:

```bash
docker exec -it taxi-airflow-scheduler airflow dags trigger nyc_taxi_batch_pipeline
```

Listar execuções:

```bash
docker exec -it taxi-airflow-scheduler airflow dags list-runs -d nyc_taxi_batch_pipeline
```

Resultado esperado:

```text
nyc_taxi_batch_pipeline | success
```

---

## Consultas Analíticas com DuckDB

A camada Gold pode ser consultada com DuckDB.

Script principal:

```text
queries/run_gold_query.py
```

Executar:

```bash
python queries/run_gold_query.py
```

---

## Arquivos SQL

O projeto possui consultas SQL na pasta:

```text
queries/
```

### Métricas da Gold

```text
queries/metricas_gold.sql
```

Executar:

```bash
python queries/run_sql_file.py queries/metricas_gold.sql
```

### Consulta por Data

```text
queries/consulta_por_data.sql
```

Executar:

```bash
python queries/run_sql_file.py queries/consulta_por_data.sql
```

### Consulta por Local

```text
queries/consulta_por_local.sql
```

Executar:

```bash
python queries/run_sql_file.py queries/consulta_por_local.sql
```

---

## Testes Unitários

Os testes unitários validam:

- Regras de validação dos eventos
- Filtros por localização
- Transformações da camada Silver
- Agregações da camada Gold

Executar:

```bash
pytest tests/
```

Resultado esperado:

```text
5 passed
```

---

## Validação Final Realizada

O pipeline foi validado ponta a ponta.

### Airflow DAG

```text
nyc_taxi_batch_pipeline | success
```

### DuckDB Gold Query

```bash
python queries/run_gold_query.py
```

Resultado:

```text
Consulta executada com sucesso sobre a camada Gold.
```

### Pytest

```bash
pytest tests/
```

Resultado:

```text
5 passed
```

---

## Principais Decisões Técnicas

### Kafka

Kafka foi utilizado para simular um canal de streaming real, onde eventos são produzidos e consumidos de forma assíncrona.

### Parquet

O formato Parquet foi escolhido por ser colunar, eficiente para consultas analíticas e amplamente usado em Data Lakes.

### Bronze, Silver e Gold

A arquitetura em camadas permite separar:

- Dados crus
- Dados tratados
- Dados analíticos

Esse padrão facilita governança, rastreabilidade e evolução do pipeline.

### Airflow

O Airflow foi utilizado para orquestrar o processamento batch das camadas Silver e Gold.

### DuckDB

DuckDB foi utilizado como motor analítico local para consultar arquivos Parquet sem necessidade de banco externo.

### Pytest

Pytest foi utilizado para garantir qualidade mínima das regras de filtro e transformação.

---

## Observações Sobre Ambiente Local

Durante a execução local em Docker, a camada Silver foi gravada como arquivo Parquet consolidado para garantir estabilidade no ambiente local.

Em um ambiente produtivo, uma evolução natural seria particionar a camada Silver por:

```text
pickup_year
pickup_month
```

E executar o processamento com Spark, Databricks, EMR ou outra engine distribuída.

---

## Como Parar os Serviços

Para parar os containers:

```bash
docker compose down
```

Para parar e remover volumes:

```bash
docker compose down -v
```

---

## Comandos Úteis

Ver containers:

```bash
docker ps
```

Ver logs do Airflow Webserver:

```bash
docker logs taxi-airflow-webserver
```

Ver logs do Scheduler:

```bash
docker logs taxi-airflow-scheduler
```

Ver DAGs disponíveis:

```bash
docker exec -it taxi-airflow-scheduler airflow dags list
```

Ver tasks da DAG:

```bash
docker exec -it taxi-airflow-scheduler airflow tasks list nyc_taxi_batch_pipeline
```

Testar task Bronze para Silver:

```bash
docker exec -it taxi-airflow-scheduler airflow tasks test nyc_taxi_batch_pipeline bronze_to_silver 2026-05-06
```

Testar task Silver para Gold:

```bash
docker exec -it taxi-airflow-scheduler airflow tasks test nyc_taxi_batch_pipeline silver_to_gold 2026-05-06
```

---

## Melhorias Futuras

Possíveis evoluções:

- Processamento distribuído com Spark
- Particionamento da Silver por ano e mês
- Data Lake em cloud com S3, ADLS ou GCS
- Glue Catalog, Unity Catalog ou Hive Metastore
- dbt para modelagem analítica
- Schema Registry para contratos de eventos
- CI/CD com GitHub Actions
- Observabilidade com Prometheus e Grafana
- Qualidade de dados com Great Expectations ou Soda
- Dashboard em Power BI ou Superset

---

## Finalidade do Projeto

Este projeto simula um fluxo real de Engenharia de Dados para dados de mobilidade urbana.

O dataset histórico do Kaggle é usado como fonte, mas cada linha é transformada em um evento de streaming. Esses eventos são enviados para o Kafka, consumidos por uma aplicação Python e armazenados em Parquet na camada Bronze do Data Lake.

Depois, um pipeline batch orquestrado pelo Airflow transforma os dados da Bronze para Silver, aplicando limpeza, conversão de tipos, validações e enriquecimento temporal. Em seguida, a camada Gold consolida métricas por data e hora da corrida.

Por fim, os dados analíticos são consultados com DuckDB e as principais regras são validadas com testes unitários.

---

## Autor

Antonio Carlos Batista Junior

GitHub:

```text
https://github.com/Batistajunior
```