# scripts/queries.py

from datetime import datetime
from pymongo import MongoClient

from config import MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME


def get_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    col = db[MONGO_COLLECTION_NAME]
    return client, col


def q1_listings_per_property_type(col):
    """
    1. How many listings are there for each type of property?
    """
    pipeline = [
        {"$group": {"_id": "$property_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    return list(col.aggregate(pipeline))


def q2_listings_on_date_paris(col):
    """
    2. How many listings were made on June 12, 2024, for the city of Paris?
       On suppose que 'last_scraped' = date de prise en compte.
    """
    target_date = datetime(2024, 6, 12)
    query = {"city": "Paris", "last_scraped": target_date}
    return col.count_documents(query)


def q3_top5_by_reviews(col):
    """
    3. Top 5 annonces par nombre de reviews.
    """
    cursor = (
        col.find({}, {"name": 1, "number_of_reviews": 1})
        .sort("number_of_reviews", -1)
        .limit(5)
    )
    return list(cursor)


def q4_total_unique_hosts(col):
    """
    4. Total nombre d'hôtes uniques.
    """
    host_ids = col.distinct("host.host_id")
    return len(host_ids)


def q5_instant_bookable_stats(col):
    """
    5. Nombre d'annonces instant-bookable + proportion.
    """
    total = col.count_documents({})
    instant = col.count_documents({"instant_bookable": True})
    proportion = instant / total if total > 0 else 0
    return {"total": total, "instant": instant, "proportion": proportion}


def q6_hosts_with_more_than_100_listings(col):
    """
    6. Hôtes avec plus de 100 annonces sur la plateforme.
    """
    pipeline = [
        {
            "$group": {
                "_id": "$host.host_id",
                "host_name": {"$first": "$host.host_name"},
                "count_listings": {"$sum": 1},
            }
        },
        {"$match": {"count_listings": {"$gt": 100}}},
    ]
    results = list(col.aggregate(pipeline))

    total_hosts = q4_total_unique_hosts(col)
    for r in results:
        r["percentage_of_hosts"] = (
            r["count_listings"] / total_hosts if total_hosts > 0 else 0
        )

    return results


def q7_superhosts_stats(col):
    """
    7. Nombre de superhosts uniques + pourcentage.
    """
    super_host_ids = col.distinct("host.host_id", {"host.is_superhost": True})
    total_hosts = q4_total_unique_hosts(col)

    count_super = len(super_host_ids)
    proportion = count_super / total_hosts if total_hosts > 0 else 0

    return {
        "unique_superhosts": count_super,
        "total_hosts": total_hosts,
        "proportion": proportion,
    }


def main():
    client, col = get_collection()

    print("Q1 - Listings per property type:")
    print(q1_listings_per_property_type(col))

    print("\nQ2 - Listings on 2024-06-12 in Paris:")
    print(q2_listings_on_date_paris(col))

    print("\nQ3 - Top 5 listings by number of reviews:")
    print(q3_top5_by_reviews(col))

    print("\nQ4 - Total unique hosts:")
    print(q4_total_unique_hosts(col))

    print("\nQ5 - Instant-bookable stats:")
    print(q5_instant_bookable_stats(col))

    print("\nQ6 - Hosts with more than 100 listings:")
    print(q6_hosts_with_more_than_100_listings(col))

    print("\nQ7 - Superhosts stats:")
    print(q7_superhosts_stats(col))

    client.close()


if __name__ == "__main__":
    main()
