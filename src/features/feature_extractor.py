# src/features/feature_extractor.py

import json
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# Colonnes à exclure — identifiants et labels
EXCLUDE_COLUMNS = {"TRANSACTION_ID", "CLASS"}

class FeatureExtractor:

    def __init__(self, profile_path: str = "data/processed/profile.json"):
        with open(profile_path, "r") as f:
            self.profile = json.load(f)

    def extract(self) -> pd.DataFrame:
        """
        Transforme le profil JSON en matrice de features.
        Retourne un DataFrame où chaque ligne = une colonne de la DB.
        """
        rows = []

        for table_name, columns in self.profile.items():
            for col_name, stats in columns.items():

                # Exclure les colonnes non pertinentes
                if col_name in EXCLUDE_COLUMNS:
                    continue

                row = {
                    "table":         table_name,
                    "column":        col_name,
                    "null_ratio":    stats.get("null_ratio",    0.0),
                    "unique_ratio":  stats.get("unique_ratio",  0.0),
                    "mean":          stats.get("mean",          0.0),
                    "std":           stats.get("std",           0.0),
                    "min":           stats.get("min",           0.0),
                    "max":           stats.get("max",           0.0),
                    "outlier_ratio": stats.get("outlier_ratio", 0.0),
                }
                rows.append(row)

        df = pd.DataFrame(rows)
        return df

    def get_matrix(self, df: pd.DataFrame) -> np.ndarray:
        """
        Retourne uniquement les valeurs numériques
        sous forme de matrice numpy — prête pour le modèle.
        """
        feature_cols = [
            "null_ratio",
            "unique_ratio",
            "mean",
            "std",
            "min",
            "max",
            "outlier_ratio"
        ]
        return df[feature_cols].values

    def save(self, df: pd.DataFrame, path: str = "data/processed/features.csv"):
        """Sauvegarde la matrice de features en CSV."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)
        print(f"Features sauvegardées : {path}")


if __name__ == "__main__":
    extractor = FeatureExtractor()

    # 1. Extraire les features
    df = extractor.extract()

    # 2. Afficher la matrice
    print(df.to_string())
    print(f"\nShape : {df.shape}  ({df.shape[0]} colonnes × {df.shape[1]-2} features)")

    # 3. Vérifier la matrice numpy
    matrix = extractor.get_matrix(df)
    print(f"Matrix numpy shape : {matrix.shape}")

    # 4. Sauvegarder
    extractor.save(df)