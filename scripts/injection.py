import pandas as pd
import oracledb
from dotenv import load_dotenv
import os

load_dotenv()  # charge le .env automatiquement

conn = oracledb.connect(
    user=os.getenv("ORACLE_USER"),
    password=os.getenv("ORACLE_PASSWORD"),
    dsn=os.getenv("ORACLE_DSN")
)

cursor = conn.cursor()

# Lire le CSV
df = pd.read_csv("data/raw/creditcard.csv")
df.columns = [c.upper() for c in df.columns]  # Oracle préfère les majuscules

# Injection par batch (rapide)
rows = [tuple(row) for row in df.itertuples(index=False)]

cursor.executemany("""
    INSERT INTO transactions (
        time_seconds, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10,
        v11, v12, v13, v14, v15, v16, v17, v18, v19, v20,
        v21, v22, v23, v24, v25, v26, v27, v28, amount, class
    ) VALUES (
        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11,
        :12, :13, :14, :15, :16, :17, :18, :19, :20, :21,
        :22, :23, :24, :25, :26, :27, :28, :29, :30, :31
    )
""", rows)

conn.commit()
print(f"{len(rows)} lignes injectées avec succès.")
cursor.close()
conn.close()