from pyspark.sql import SparkSession
import kagglehub
import os
import glob
import psycopg2

# ----------------------------------------
# 0. Cria banco nba e schemas raw, silver, gold com usuário marcell
# ----------------------------------------

conn = psycopg2.connect(
    host="localhost",
    database="postgres",  # conecta no banco padrão
    user="marcell",
    password="123"
)
conn.autocommit = True
cursor = conn.cursor()

# Cria o banco nba se não existir
cursor.execute("SELECT 1 FROM pg_database WHERE datname='nba';")
if not cursor.fetchone():
    cursor.execute("CREATE DATABASE nba;")
    print("Banco 'nba' criado.")

cursor.close()
conn.close()

# Conecta ao banco nba
conn = psycopg2.connect(
    host="localhost",
    database="nba",
    user="marcell",
    password="123"
)
conn.autocommit = True
cursor = conn.cursor()

# Cria schemas raw, silver e gold
for schema in ["raw", "silver", "gold"]:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    print(f"Schema {schema} pronto.")

cursor.close()
conn.close()

# ----------------------------------------
# 1. Inicia sessão Spark com conector PostgreSQL
# ----------------------------------------

spark = SparkSession.builder \
    .appName("IngestaoRawKaggleNBA") \
    .config("spark.jars", "/home/marcell/.local/share/DBeaverData/drivers/maven/maven-central/org.postgresql/postgresql-42.7.2.jar") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# ----------------------------------------
# 2. Baixa dataset do Kaggle
# ----------------------------------------
path = kagglehub.dataset_download("wyattowalsh/basketball")  # Dataset NBA
print("Path to dataset files:", path)

# ----------------------------------------
# 3. Configuração de conexão PostgreSQL (usuário marcell)
# ----------------------------------------
db_url = "jdbc:postgresql://localhost:5432/nba"
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
    print(f"Lendo arquivo: {file} -> tabela: raw.{nome_tabela}")

    # Lê CSV como DataFrame (forçando tudo como string)
    df = spark.read.csv(file, header=True, inferSchema=False)

    for col_name in df.columns:
        df = df.withColumn(col_name, df[col_name].cast("string"))

    df.printSchema()

    # ----------------------------------------
    # 5. Escreve DataFrame no PostgreSQL (schema raw)
    # ----------------------------------------
    df.write \
        .jdbc(
            url=db_url,
            table=f"raw.{nome_tabela}",
            mode="overwrite",
            properties=db_properties
        )
    print(f"Tabela raw.{nome_tabela} carregada no PostgreSQL")

# Encerra a sessão Spark
spark.stop()
