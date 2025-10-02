# -*- coding: utf-8 -*-
import kagglehub
import sqlite3
import pandas as pd
from pyspark.sql import SparkSession
import psycopg2, os, re, glob

# -----------------------------
# 0A) KaggleHub: localizar o arquivo .sqlite do dataset
# -----------------------------
kaggle_path = kagglehub.dataset_download("wyattowalsh/basketball")
print("Path to dataset files:", kaggle_path)

# acha um .sqlite (ou .db) dentro da pasta baixada
cands = glob.glob(os.path.join(kaggle_path, "**", "*.sqlite"), recursive=True)
if not cands:
    cands = glob.glob(os.path.join(kaggle_path, "**", "*.db"), recursive=True)
if not cands:
    raise FileNotFoundError(f"Nenhum arquivo .sqlite/.db encontrado em {kaggle_path}")
sqlite_path = cands[0]
print("Arquivo SQLite localizado:", sqlite_path)

# -----------------------------
# 0) Cria DB e schemas no Postgres
# -----------------------------
conn = psycopg2.connect(host="localhost", database="postgres", user="marcell", password="123")
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname='nba';")
if not cur.fetchone():
    cur.execute("CREATE DATABASE nba;")
    print("Banco 'nba' criado.")
cur.close(); conn.close()

conn = psycopg2.connect(host="localhost", database="nba", user="marcell", password="123")
conn.autocommit = True
cur = conn.cursor()
for schema in ("raw","silver","gold"):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    print(f"Schema {schema} pronto.")
cur.close(); conn.close()

# -----------------------------
# 1) Spark com JAR SOMENTE do Postgres (sem sqlite-jdbc)
# -----------------------------
pg_jar = "/home/marcell/.local/share/DBeaverData/drivers/maven/maven-central/org.postgresql/postgresql-42.7.2.jar"
if not os.path.exists(pg_jar):
    raise FileNotFoundError(f"JAR não encontrado: {pg_jar}")

spark = (
    SparkSession.builder
    .appName("IngestaoRawSQLiteNBA_semJDBC_SQLite")
    .config("spark.jars", pg_jar)
    .config("spark.driver.extraClassPath", pg_jar)
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# -----------------------------
# 2) Conexões
# -----------------------------
pg_url   = "jdbc:postgresql://localhost:5432/nba?currentSchema=raw"  # grava direto no schema raw
pg_props = {"user":"marcell","password":"123","driver":"org.postgresql.Driver"}

# -----------------------------
# 3) Listar tabelas via sqlite3
# -----------------------------
con_sqlite = sqlite3.connect(sqlite_path)
cur = con_sqlite.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
tabelas = [r[0] for r in cur.fetchall()]
print("Tabelas encontradas no SQLite:", tabelas)

def safe_ident(name: str) -> str:
    return f'"{name}"'   # mantém nome exato no Postgres

def sqlite_counts_and_cols(table: str):
    cur.execute(f'SELECT COUNT(*) FROM "{table}";')
    n = cur.fetchone()[0]
    cur.execute(f'PRAGMA table_info("{table}");')
    cols = [r[1] for r in cur.fetchall()]
    return n, cols

# -----------------------------
# 4) Ler em chunks (sqlite3+pandas) -> criar DataFrame Spark -> gravar no Postgres/raw
# -----------------------------
ok, fail = [], []
CHUNK = 200_000  # ajuste conforme memória

for tabela in tabelas:
    try:
        print(f"\nLendo {tabela} do SQLite (sem JDBC do SQLite)...")
        total_rows_sqlite, cols_sqlite = sqlite_counts_and_cols(tabela)
        print(f"SQLite -> {tabela}: {total_rows_sqlite} linhas, {len(cols_sqlite)} colunas")

        sql = f'SELECT * FROM "{tabela}"'
        chunk_iter = pd.read_sql_query(sql, con_sqlite, chunksize=CHUNK)

        total_written = 0
        wrote_any = False
        for i, pdf in enumerate(chunk_iter):
            sdf = spark.createDataFrame(pdf)  # infere schema automaticamente
            mode = "overwrite" if i == 0 else "append"
            sdf.write.jdbc(url=pg_url, table=safe_ident(tabela), mode=mode, properties=pg_props)
            total_written += len(pdf)
            wrote_any = True
            print(f"   chunk {i}: +{len(pdf)} (acumulado {total_written})")

        if not wrote_any:
            # se tabela estiver vazia, cria estrutura vazia no Postgres
            sdf_empty = spark.createDataFrame(pd.DataFrame(columns=cols_sqlite))
            sdf_empty.write.jdbc(url=pg_url, table=safe_ident(tabela), mode="overwrite", properties=pg_props)
            print("   tabela vazia; estrutura criada no Postgres")

        if total_written != total_rows_sqlite:
            print(f" Divergência {tabela}: gravado={total_written} vs sqlite={total_rows_sqlite}")
        else:
            print(f" Postgres: raw.{tabela} carregada ({total_written} linhas)")

        ok.append((tabela, total_written, len(cols_sqlite)))

    except Exception as e:
        print(f"Falha em {tabela}: {e}")
        fail.append((tabela, str(e)))

# -----------------------------
# 5) Resumo e fim
# -----------------------------
print("\n=== RESUMO ===")
for t, n, c in ok:
    print(f"OK  -> {t}: {n} linhas, {c} colunas")
for t, err in fail:
    print(f"FAIL-> {t}: {err}")

cur.close()
con_sqlite.close()
spark.stop()
