import pandas as pd
import numpy as np
import json
import os
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib

FEATURE_COLS = [
    "null_ratio",
    "unique_ratio",
    "mean",
    "std",
    "min",
    "max",
    "outlier_ratio"
]

class AnomalyDetector:

    def __init__(self, contamination: float = 0.1, n_estimators: int = 100, random_state: int = 42):
        """
        contamination : proportion estimée d'anomalies dans les données.
                        0.1 = on s'attend à ~10% de colonnes anormales.
        n_estimators  : nombre d'arbres dans la forêt.
        """
        self.scaler = StandardScaler()
        self.model  = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=random_state
        )

    def fit_predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Entraîne le modèle et prédit les anomalies.
        Retourne le DataFrame enrichi avec les scores et labels.
        """
        X = df[FEATURE_COLS].values

        X_scaled = self.scaler.fit_transform(X)


        labels = self.model.fit_predict(X_scaled)

        raw_scores = self.model.score_samples(X_scaled)


        anomaly_scores = 1 - (raw_scores - raw_scores.min()) / (raw_scores.max() - raw_scores.min())


        result = df.copy()
        result["anomaly_label"] = labels          # -1 ou 1
        result["anomaly_score"] = np.round(anomaly_scores, 4)
        result["is_anomaly"]    = labels == -1    # True/False

        return result

    def save_model(self, path: str = "data/processed/model.pkl"):
        """Sauvegarde le modèle entraîné."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump({"model": self.model, "scaler": self.scaler}, path)
        print(f"Modèle sauvegardé : {path}")

    def save_results(self, result: pd.DataFrame, path: str = "data/processed/anomaly_results.json"):
        """Sauvegarde les résultats en JSON."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        output = []
        for _, row in result.iterrows():
            output.append({
                "table":         row["table"],
                "column":        row["column"],
                "anomaly_score": row["anomaly_score"],
                "is_anomaly":    bool(row["is_anomaly"]),
                "features": {
                    col: round(row[col], 4) for col in FEATURE_COLS
                }
            })
        with open(path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"Résultats sauvegardés : {path}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from src.features.feature_extractor import FeatureExtractor

    extractor = FeatureExtractor()
    df = extractor.extract()
    print(f"Features chargées : {df.shape}")

    detector = AnomalyDetector(contamination=0.1)
    result = detector.fit_predict(df)

    print("\n=== Anomalies détectées ===\n")
    display = result[["table", "column", "anomaly_score", "is_anomaly"]]\
                .sort_values("anomaly_score", ascending=False)
    print(display.to_string(index=False))

    n_anomalies = result["is_anomaly"].sum()
    print(f"\nTotal colonnes analysées : {len(result)}")
    print(f"Anomalies détectées      : {n_anomalies}")

    detector.save_model()
    detector.save_results(result)