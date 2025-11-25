# scripts/migrate_to_mongo.py

import pandas as pd
from pymongo import MongoClient

from config import CLEAN_CSV_PATH, MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME


def load_clean_data(path: str) -> pd.DataFrame:
    print(f"Chargement du fichier nettoyé : {path}")
    return pd.read_csv(path, parse_dates=["last_scraped", "host_since"])


def connect_mongo():
    print(f"Connexion à MongoDB : {MONGO_URI}")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    col = db[MONGO_COLLECTION_NAME]
    return client, col


def df_to_documents(df: pd.DataFrame):
    """
    Transforme chaque ligne en document MongoDB.
    Gère correctement les dates manquantes (NaT -> None).
    """
    docs = []

    for _, row in df.iterrows():
        # Gestion des dates : NaT -> None, sinon datetime natif Python
        last_scraped = row["last_scraped"]
        if pd.isna(last_scraped):
            last_scraped = None
        else:
            last_scraped = last_scraped.to_pydatetime()

        host_since = row["host_since"]
        if pd.isna(host_since):
            host_since = None
        else:
            host_since = host_since.to_pydatetime()

        doc = {
            "_id": row["id"],
            "name": row["name"],
            "property_type": row["property_type"],
            "room_type": row["room_type"],
            "last_scraped": last_scraped,
            "number_of_reviews": int(row["number_of_reviews"]),
            "instant_bookable": row["instant_bookable"],
            "price": row["price"],
            "host": {
                "host_id": row["host_id"],
                "host_name": row["host_name"],
                "host_since": host_since,
                "is_superhost": row["host_is_superhost"],
                "calculated_host_listings_count": int(
                    row["calculated_host_listings_count"]
                ),
            },
            "city": "Paris",
        }
        docs.append(doc)

    return docs



def main():
    df = load_clean_data(CLEAN_CSV_PATH)
    client, col = connect_mongo()

    print("Suppression de la collection existante (si elle existe)...")
    col.drop()

    docs = df_to_documents(df)
    print(f"Insertion de {len(docs)} documents...")
    result = col.insert_many(docs)
    print(f"{len(result.inserted_ids)} documents insérés.")

    client.close()
    print("Migration terminée.")


if __name__ == "__main__":
    main()
