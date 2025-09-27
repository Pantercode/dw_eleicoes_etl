from pyspark.sql import SparkSession
import kagglehub
import os
import glob

# ----------------------------------------
# 1. Inicia sessão Spark com conector MySQL
# ----------------------------------------
spark = SparkSession.builder \
    .appName("IngestaoRawKaggle") \
    .config("spark.jars", "/caminho/mysql-connector-j-8.4.0.jar") \
    .getOrCreate()

# ----------------------------------------
# 2. Baixa dataset do Kaggle
# ----------------------------------------
path = kagglehub.dataset_download("unanimad/brazil-election-2022")
print("Path to dataset files:", path)

# ----------------------------------------
# 3. Configuração de conexão MySQL
# ----------------------------------------
db_url = "jdbc:mysql://localhost:3306/raw"
db_properties = {
    "user": "marcell",
    "password": "123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# ----------------------------------------
# 4. Itera sobre todos os CSVs do dataset
# ----------------------------------------
files = glob.glob(f"{path}/*.csv")

for file in files:
    nome_tabela = os.path.splitext(os.path.basename(file))[0]
    print(f"Lendo arquivo: {file} -> tabela: {nome_tabela}")

    # Lê CSV como DataFrame (tudo string)
    df = spark.read.csv(file, header=True, inferSchema=False)

    # Converte todas colunas para STRING
    for col_name in df.columns:
        df = df.withColumn(col_name, df[col_name].cast("string"))

    df.printSchema()

    # ----------------------------------------
    # 5. Escreve DataFrame no MySQL
    # ----------------------------------------
    df.write.jdbc(
        url=db_url,
        table=nome_tabela,
        mode="overwrite",  # ou "append"
        properties=db_properties
    )

    print(f" Tabela {nome_tabela} carregada no MySQL.raw")