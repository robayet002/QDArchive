import requests

ZENODO_API_URL = "https://zenodo.org/api/records"


def search_one_query(query, size=25, pages=2):
    records = []
    seen_ids = set()

    for page in range(1, pages + 1):
        params = {
            "q": query,
            "size": size,
            "page": page,
        }

        try:
            response = requests.get(ZENODO_API_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            hits = data.get("hits", {}).get("hits", [])

            for record in hits:
                record_id = str(record.get("id", ""))
                if record_id and record_id not in seen_ids:
                    seen_ids.add(record_id)
                    records.append(record)

        except requests.RequestException as error:
            print(f"Zenodo search failed for query '{query}' on page {page}: {error}")

    return records


def search_zenodo(size=25, pages=2):
    queries = ["qdpx", "qpdx", "qualitative"]
    all_records = []
    seen_ids = set()

    for query in queries:
        results = search_one_query(query=query, size=size, pages=pages)
        for record in results:
            record_id = str(record.get("id", ""))
            if record_id and record_id not in seen_ids:
                seen_ids.add(record_id)
                all_records.append(record)

    return all_records