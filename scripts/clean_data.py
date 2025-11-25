# scripts/clean_data.py

import os
import pandas as pd
from pathlib import Path

from config import RAW_CSV_PATH, CLEAN_CSV_PATH


def load_raw_data(path: str) -> pd.DataFrame:
    print(f"Chargement du fichier brut : {path}")
    return pd.read_csv(path)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie listings_Paris.csv (Inside Airbnb).
    On garde seulement les colonnes nécessaires aux questions de l'examen.
    """

    cols = [
        "id",
        "name",
        "host_id",
        "host_name",
        "host_since",
        "host_is_superhost",
        "property_type",
        "room_type",
        "last_scraped",
        "number_of_reviews",
        "instant_bookable",
        "calculated_host_listings_count",
        "price",
    ]

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans le CSV : {missing}")

    df = df[cols].copy()

    # ID et host_id en string
    df["id"] = df["id"].astype(str)
    df["host_id"] = df["host_id"].astype(str)

    # Dates
    df["last_scraped"] = pd.to_datetime(df["last_scraped"], errors="coerce")
    df["host_since"] = pd.to_datetime(df["host_since"], errors="coerce")

    # t/f -> bool
    def tf_to_bool(x):
        if isinstance(x, str):
            x = x.strip().lower()
            if x == "t":
                return True
            if x == "f":
                return False
        return None

    df["host_is_superhost"] = df["host_is_superhost"].apply(tf_to_bool)
    df["instant_bookable"] = df["instant_bookable"].apply(tf_to_bool)

    # number_of_reviews
    df["number_of_reviews"] = df["number_of_reviews"].fillna(0).astype(int)

    # calculated_host_listings_count
    df["calculated_host_listings_count"] = (
        df["calculated_host_listings_count"].fillna(0).astype(int)
    )

    # prix : enlever $, , etc.
    def clean_price(val):
        if isinstance(val, str):
            val = val.replace("$", "").replace(",", "")
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    df["price"] = df["price"].apply(clean_price)

    # Supprimer les lignes sans id ou host_id
    df = df.dropna(subset=["id", "host_id"])

    print("Aperçu après nettoyage :")
    print(df.head())

    return df


def save_clean_data(df: pd.DataFrame, path: str) -> None:
    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Fichier nettoyé enregistré dans : {path}")


def main():
    df_raw = load_raw_data(RAW_CSV_PATH)
    print(f"Shape brut : {df_raw.shape}")

    df_clean = clean_dataframe(df_raw)
    print(f"Shape nettoyé : {df_clean.shape}")

    save_clean_data(df_clean, CLEAN_CSV_PATH)


if __name__ == "__main__":
    main()
