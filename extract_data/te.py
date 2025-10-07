# -*- coding: utf-8 -*-
import os, glob, sqlite3, shutil
import kagglehub
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

# =========================
# 0) KaggleHub: localizar .sqlite
# =========================
kaggle_path = kagglehub.dataset_download("wyattowalsh/basketball")
print("Path to dataset files:", kaggle_path)

cands = glob.glob(os.path.join(kaggle_path, "**", "*.sqlite"), recursive=True)
if not cands:
    cands = glob.glob(os.path.join(kaggle_path, "**", "*.db"), recursive=True)
if not cands:
    raise FileNotFoundError(f"Nenhum .sqlite/.db encontrado em {kaggle_path}")
sqlite_path = cands[0]
print("Arquivo SQLite:", sqlite_path)

# pasta parquet de saída
parquet_root = os.path.join(os.path.dirname(sqlite_path), "parquet_raw")
os.makedirs(parquet_root, exist_ok=True)

# =========================
# 1) Cria DB e schemas (psycopg2)
# =========================
pg_host, pg_db_admin, pg_user, pg_pass = "localhost", "postgres", "marcell", "123"

conn = psycopg2.connect(host=pg_host, database=pg_db_admin, user=pg_user, password=pg_pass)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname='nba';")
if not cur.fetchone():
    cur.execute("CREATE DATABASE nba;")
    print("Banco 'nba' criado.")
cur.close(); conn.close()

conn = psycopg2.connect(host=pg_host, database="nba", user=pg_user, password=pg_pass)
conn.autocommit = True
cur = conn.cursor()
for schema in ("raw","silver","gold"):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    print(f"Schema {schema} pronto.")
cur.close(); conn.close()

# =========================
# 2) Engine SQLAlchemy para Postgres (usaremos schema='raw')
# =========================
engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:5432/nba")

# =========================
# 3) Listar tabelas no SQLite
# =========================
con_sqlite = sqlite3.connect(sqlite_path)
cur_sqlite = con_sqlite.cursor()
cur_sqlite.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
tabelas = [r[0] for r in cur_sqlite.fetchall()]
print("Tabelas no SQLite:", tabelas)

def get_counts_and_cols(table: str):
    cur_sqlite.execute(f'PRAGMA table_info("{table}");')
    cols = [r[1] for r in cur_sqlite.fetchall()]
    cur_sqlite.execute(f'SELECT COUNT(*) FROM "{table}";')
    n = cur_sqlite.fetchone()[0]
    return n, cols

def safe_dir(name: str) -> str:
    import re
    return re.sub(r"[^a-zA-Z0-9_]+", "_", name)

# =========================
# 4) Loop: ler em chunks -> gravar no Postgres (raw) -> salvar Parquet -> checar
# =========================
CHUNK = 50_000     # ajuste se precisar de menos memória
ok, fail = [], []
with engine.begin() as conn_tx:
    # garante search_path pra 'raw' (opcional, já passamos schema no to_sql)
    conn_tx.execute(text("SET search_path TO raw, public"))

for tabela in tabelas:
    try:
        total_rows_sqlite, cols_sqlite = get_counts_and_cols(tabela)
        print(f"\nProcessando {tabela}: {total_rows_sqlite} linhas, {len(cols_sqlite)} colunas")

        # 4A) Leitura em chunks e escrita no Postgres
        sql = f'SELECT * FROM "{tabela}"'
        chunk_iter = pd.read_sql_query(sql, con_sqlite, chunksize=CHUNK)

        created = False
        total_written = 0
        for i, pdf in enumerate(chunk_iter):
            # primeira passada: cria/replace; demais: append
            if_exists = "replace" if not created else "append"
            pdf.to_sql(
                name=tabela,             # o SQLAlchemy coloca aspas corretas
                con=engine,
                schema="raw",
                if_exists=if_exists,
                index=False,
                method="multi",          # insere batches multi-values
                chunksize=10_000         # controla tamanho do batch
            )
            total_written += len(pdf)
            created = True
            print(f"   -> chunk {i}: +{len(pdf)} (acum {total_written})")

        # se não veio chunk (tabela vazia), cria estrutura vazia
        if not created:
            pd.DataFrame(columns=cols_sqlite).to_sql(
                name=tabela, con=engine, schema="raw", if_exists="replace", index=False
            )
            print("   -> tabela vazia; criada estrutura no Postgres")

        # 4B) Exporta Parquet e confere (linhas/colunas)
        out_dir = os.path.join(parquet_root, safe_dir(tabela))
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        # para não estourar RAM, também escreve parquet em partes via chunks
        chunk_iter = pd.read_sql_query(sql, con_sqlite, chunksize=CHUNK)
        wrote_parquet_any = False
        part = 0
        for pdf in chunk_iter:
            # pyarrow salva múltiplos arquivos .parquet numa mesma pasta
            pdf.to_parquet(os.path.join(out_dir, f"part-{part:05}.parquet"), index=False)
            wrote_parquet_any = True
            part += 1

        if not wrote_parquet_any:
            # parquet vazio com apenas schema
            pd.DataFrame(columns=cols_sqlite).to_parquet(os.path.join(out_dir, "part-00000.parquet"), index=False)

        # valida parquet agregando de volta
        parts = glob.glob(os.path.join(out_dir, "part-*.parquet"))
        # concat por streaming (sem carregar tudo de uma vez)
        total_parq_rows = 0
        parq_cols = None
        for p in parts:
            dfp = pd.read_parquet(p)
            total_parq_rows += len(dfp)
            if parq_cols is None:
                parq_cols = list(dfp.columns)

        if total_parq_rows != total_rows_sqlite or len(parq_cols or []) != len(cols_sqlite):
            raise RuntimeError(
                f"Verificação Parquet falhou: linhas parquet={total_parq_rows} colunas={len(parq_cols or [])} "
                f"vs sqlite={total_rows_sqlite}/{len(cols_sqlite)}"
            )

        print(f"✅ Concluído {tabela}: Postgres(raw) {total_written} linhas | Parquet OK em {out_dir}")
        ok.append((tabela, total_written, len(cols_sqlite)))

    except Exception as e:
        print(f"❌ Falha em {tabela}: {e}")
        fail.append((tabela, str(e)))

# =========================
# 5) Resumo e limpeza do .sqlite
# =========================
print("\n=== RESUMO ===")
for t, n, c in ok:
    print(f"OK  -> {t}: {n} linhas, {c} colunas")
for t, err in fail:
    print(f"FAIL-> {t}: {err}")

# apaga o sqlite **apenas** se todas passaram
try:
    if not fail:
        os.remove(sqlite_path)
        print(f"\n🧹 .sqlite removido: {sqlite_path}")
    else:
        print("\nℹ️  Mantive o .sqlite para depuração (há falhas).")
except Exception as e:
    print(f"⚠️  Não pude remover o .sqlite: {e}")

cur_sqlite.close()
con_sqlite.close()
