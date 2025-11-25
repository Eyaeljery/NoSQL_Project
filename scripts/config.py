# scripts/config.py

from pathlib import Path

# Chemins construits à partir de la racine du projet
PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "listings_Paris.csv"
CLEAN_CSV_PATH = PROJECT_ROOT / "data" / "processed" / "airbnb_listings_clean.csv"

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB_NAME = "airbnb_paris"
MONGO_COLLECTION_NAME = "listings"
