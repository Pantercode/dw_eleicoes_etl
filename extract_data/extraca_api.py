from pyspark.sql import SparkSession
import kagglehub
import os
import glob
import psycopg2

# ----------------------------------------
# 0. Cria banco raw e schema copy se não existirem
# ----------------------------------------

# CORREÇÃO: Conecta ao banco 'postgres' (padrão) inicialmente,
# pois o banco 'marcell' não existe.
conn = psycopg2.connect(
    host="localhost",
    database="postgres", # <-- AJUSTE AQUI
    user="marcell",
    password="123"
)
conn.autocommit = True
cursor = conn.cursor()

# Cria o banco raw se não existir
cursor.execute("SELECT 1 FROM pg_database WHERE datname='raw';")
if not cursor.fetchone():
    cursor.execute("CREATE DATABASE raw;")
    print("Banco 'raw' criado.")

# Conecta no banco raw
conn.close()
conn = psycopg2.connect(
    host="localhost",
    database="raw",
    user="marcell",
    password="123"
)
conn.autocommit = True
cursor = conn.cursor()

# Cria schema copy se não existir
cursor.execute("CREATE SCHEMA IF NOT EXISTS copy;")
cursor.execute("GRANT ALL PRIVILEGES ON SCHEMA copy TO marcell;")
cursor.close()
conn.close()

# ----------------------------------------
# 1. Inicia sessão Spark com conector PostgreSQL
# ----------------------------------------

# ATENÇÃO: Verifique se o caminho do seu JAR está correto. 
# O caminho '/caminho/postgresql-42.6.0.jar' geralmente precisa ser atualizado 
# para o local real do arquivo na sua máquina.
spark = SparkSession.builder \
    .appName("IngestaoRawKaggle") \
    .config("spark.jars", "/home/marcell/.local/share/DBeaverData/drivers/maven/maven-central/org.postgresql/postgresql-42.7.2.jar") \
    .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

# ----------------------------------------
# 2. Baixa dataset do Kaggle
# ----------------------------------------
path = kagglehub.dataset_download("unanimad/brazil-election-2022")
print("Path to dataset files:", path)

# ----------------------------------------
# 3. Configuração de conexão PostgreSQL
# ----------------------------------------
db_url = "jdbc:postgresql://localhost:5432/raw"
db_properties = {
    "user": "marcell",
    "password": "123",
    "driver": "org.postgresql.Driver"
}

# ----------------------------------------
# 4. Itera sobre todos os CSVs do dataset
# ----------------------------------------
files = glob.glob(f"{path}/*.csv")

for file in files:
    nome_tabela = os.path.splitext(os.path.basename(file))[0]
    print(f"Lendo arquivo: {file} -> tabela: {nome_tabela}")

    # Lê CSV como DataFrame (tudo string) usando inferSchema=False para ser mais rápido
    df = spark.read.csv(file, header=True, inferSchema=False)

    # Converte todas colunas para STRING
    for col_name in df.columns:
        df = df.withColumn(col_name, df[col_name].cast("string"))

    df.printSchema()

    # ----------------------------------------
    # 5. Escreve DataFrame no PostgreSQL (schema copy)
    # ----------------------------------------
    df.write \
        .jdbc(
            url=db_url,
            table=f"copy.{nome_tabela}",  # schema copy
            mode="overwrite",  # ou "append"
            properties=db_properties
        )
    print(f"Tabela {nome_tabela} carregada no PostgreSQL.copy")

# Encerra a sessão Spark
spark.stop()