# Cluster Spark com MinIO e Delta Lake

Deltaflow oferece um ambiente de estudo com Apache Spark, MinIO e Delta Lake provisionado e gerenciado com Docker
Compose. Ele permite processar dados e gerenciar armazenamento em object storage de forma eficiente, combinando a
robustez do Spark com a flexibilidade do Minio e a confiabilidade do Delta Lake.

## Tecnologias Utilizadas

- **Apache Spark**: Plataforma de computação distribuída para processamento de grandes volumes de dados.
- **MinIO**: Armazenamento de objetos S3 compatível para armazenamento e recuperação de grandes volumes de dados.
- **Delta Lake**: Camada de armazenamento unificada que traz confiabilidade e desempenho para dados no Apache Spark.
- **Docker**: Para containerização de serviços.
- **Docker Compose**: Para orquestração e provisionamento do ambiente.
- **Python**: Linguagem de programação utilizada para implementar scripts.

## Pré-requisitos

Certifique-se de ter conhecimento básico e a instalação das seguintes ferramentas em sua máquina:

- [Ubuntu 20.04](https://ubuntu.com/desktop/)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Poetry](https://python-poetry.org/)
- [Python 3.11](https://www.python.org/)

## Serviços Disponíveis

### Apache Spark

Apache Spark é um framework para processamento de dados em larga escala que permite computação distribuída. Neste
ambiente, o cluster contém um Master e dois Workers.

**Spark Master**:

- URL: `spark://172.20.0.2:7077`
- Interface Web: `http://172.20.0.2:8080`

**Spark Workers**:

- Cada worker se conecta automaticamente ao Master.
- Interface Web do Worker: `http://172.20.0.3:8081` e `http://172.20.0.4:8082`

### MinIO

MinIO é um serviço de armazenamento de objetos compatível com o Amazon S3, permitindo armazenar e recuperar dados de
forma eficiente.

- **Interface Web**: `http://172.20.0.5:9000`
- **Credenciais padrão**:
    - **Access Key**: minioadmin
    - **Secret Key**: minioadmin

#### Delta Lake

Delta Lake permite transações ACID no Apache Spark e fornece versionamento dos dados. Ele é integrado ao cluster Spark
neste ambiente para fornecer um sistema de armazenamento de dados otimizado para análises.

## Instalação

No Ubuntu, abra o terminal de linha de comando e realize as seguintes etapas:

1. Crie uma pasta de trabalho e acessa-o, por exemplo:
   ```bash
   cd ~
   mkdir workspace
   cd workspace

2. Clone este repositório:
   ```bash
   git clone https://github.com/edeilson-david/deltaflow.git

3. Navegue até o diretório do projeto:
   ```bash
   cd deltaflow

4. Provisione e inicialize os serviços com Docker Compose:
   ```bash
   docker-compose up -d

5. Verifique se todos os serviços estão em execução:

- Spark Master na URL: `http://172.20.0.2:8080`
- MinIO na URL: `http://172.20.0.5:9000`

## Utilizando o Ambiente

### Processamento com Spark

Você pode enviar jobs para o cluster Spark usando a interface de linha de comando do Spark ou conectar-se a partir de
notebooks, como o **Jupyter Notebook**, para executar operações interativas.

> **Nota**: Configuração e uso de Jupyter Notebook não faz parte desse projeto.

### Armazenamento de Dados com MinIO

Efetuaremos a leitura e escrita de dados no MinIO utilizando o SDK for Python (`minio`) e o Apache Spark. Você pode
utilizar ferramentas como **AWS CLI** ou **mc (MinIO Client)** ou interface Web MinIO para armazenar e recuperar dados
no MinIO.

### Exemplo de Conexão ao MinIO com Apache Spark utilizando Python.

```bash
packages = [
  "io.delta:delta-spark_2.12:3.2.0",
  "io.delta:delta-storage:3.2.0",
  "org.apache.hadoop:hadoop-common:3.3.4",
  "org.apache.hadoop:hadoop-aws:3.3.4",
  "com.amazonaws:aws-java-sdk-bundle:1.12.262"
]

spark = (
  SparkSession.builder.appName("ExampleAppSpark")
    .master("spark://172.20.0.2:7077")
    .config("spark.jars.packages", ",".join(packages))
    .config("spark.hadoop.fs.s3a.endpoint", "http://172.20.0.5:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .config("spark.hadoop.fs.s3a.fast.upload", True)
    .config("spark.hadoop.fs.s3a.multipart.size", 104857600)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
```

### Executando aplicação de Exemplo

No Ubuntu, abra o terminal de linha de comando e realize as seguintes etapas:

1. Através do Poetry configure o ambiente virtual Python e instale as dependências:

```bash
cd ~/workspace/deltaflow
poetry install
```

2. Execute o script de exemplo:

 ```bash
cd ~/workspace/deltaflow
poetry run python deltaflow/example_app.py
```

## Comandos Docker

### Provisionar o Ambiente

Para provisionar e iniciar todos os serviços (Master Workers e MinIO), utilize o comando:

```bash
cd ~/workspace/deltaflow
docker-compose up -d
```

### Parar o Ambiente

Para **parar** os serviços sem removê-los:

```bash
cd ~/workspace/deltaflow
docker-compose stop
```

### Iniciar os Serviços

Caso tenha parado os serviços, você pode **iniciá-los** novamente com:

```bash
cd ~/workspace/deltaflow
docker-compose start
```

### Remover os Serviços

Se quiser remover os contêineres e limpar o ambiente, execute:

```bash
cd ~/workspace/deltaflow
docker-compose down
```

### Remover Volumes e Dados

Caso queira remover volumes e dados persistentes (como os armazenados no MinIO), adicione a flag `-V`

```bash
cd ~/workspace/deltaflow
docker-compose down -v
```
