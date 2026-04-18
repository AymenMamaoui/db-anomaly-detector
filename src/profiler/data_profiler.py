import oracledb
import pandas as pd
from dotenv import load_dotenv
import os
import json

load_dotenv()

class DataProfiler:

    def __init__(self, schema: dict):
        self.conn = oracledb.connect(
            user=os.getenv("ORACLE_USER"),
            password=os.getenv("ORACLE_PASSWORD"),
            dsn=os.getenv("ORACLE_DSN")
        )
        self.cursor = self.conn.cursor()
        self.schema = schema

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        """Charge une table Oracle dans un DataFrame Pandas."""
        self.cursor.execute(f"SELECT * FROM {table_name}")
        columns = [col[0] for col in self.cursor.description]
        rows = self.cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)

    def compute_outlier_ratio(self, series: pd.Series) -> float:
        """Calcule le % de valeurs aberrantes via la méthode IQR."""
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outliers = series[(series < lower) | (series > upper)]
        return round(len(outliers) / len(series), 4) if len(series) > 0 else 0.0

    def profile_column(self, series: pd.Series, col_type: str) -> dict:
        """Calcule les statistiques d'une colonne."""
        total = len(series)
        null_count = series.isna().sum()

        profile = {
            "total_rows":   total,
            "null_count":   int(null_count),
            "null_ratio":   round(null_count / total, 4) if total > 0 else 0.0,
            "unique_count": int(series.nunique()),
            "unique_ratio": round(series.nunique() / total, 4) if total > 0 else 0.0,
        }

        # Stats numériques uniquement
        if col_type == "NUMBER":
            numeric = pd.to_numeric(series, errors="coerce").dropna()
            if len(numeric) > 0:
                profile.update({
                    "mean":          round(float(numeric.mean()), 4),
                    "std":           round(float(numeric.std()), 4),
                    "min":           round(float(numeric.min()), 4),
                    "max":           round(float(numeric.max()), 4),
                    "outlier_ratio": self.compute_outlier_ratio(numeric)
                })

        return profile

    def profile(self) -> dict:
        """Profile toutes les tables et colonnes du schéma."""
        full_profile = {}

        for table_name, table_info in self.schema.items():
            print(f"Profiling table : {table_name}...")
            df = self.fetch_table(table_name)
            table_profile = {}

            for col in table_info["columns"]:
                col_name = col["name"]
                col_type = col["type"]
                series = df[col_name]
                table_profile[col_name] = self.profile_column(series, col_type)

            full_profile[table_name] = table_profile

        return full_profile

    def save(self, profile: dict, path: str = "data/processed/profile.json"):
        """Sauvegarde le profil dans data/processed/."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(profile, f, indent=2)
        print(f"Profil sauvegardé : {path}")

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.parser.schema_parser import SchemaParser

    # 1. Parser le schéma
    parser_instance = SchemaParser()
    schema = parser_instance.parse()
    parser_instance.close()

    # 2. Profiler les données
    profiler = DataProfiler(schema)
    profile = profiler.profile()
    profiler.save(profile)
    profiler.close()

    print(json.dumps(profile, indent=2))