import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.datafirst.uct.ac.za"
SEARCH_URL = "https://www.datafirst.uct.ac.za/dataportal/index.php/catalog/"

# List of search queries
SEARCH_QUERIES = [

    "study time000000000000000000",
    
]

def search_sada(rows=None, per_page=15, max_pages=100):

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    datasets = []
    seen_ids = set()

    for query in SEARCH_QUERIES:

        print(f"SADA query: {query}")

        for page in range(1, max_pages + 1):

            params = {
                "page": page,
                "sk": query,
                "ps": per_page
            }

            r = requests.get(SEARCH_URL, params=params, headers=headers, timeout=30)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")
            page_count = 0

            for link in soup.select("a[href]"):

                href = (link.get("href") or "").strip()
                title = " ".join(link.get_text(" ", strip=True).split())

                if not href or not title:
                    continue

                full_url = urljoin(BASE_URL, href)

                if "/catalog/" not in full_url:
                    continue

                match = re.search(r"/catalog/(\d+)", full_url)
                if not match:
                    continue

                record_id = match.group(1)

                if record_id in seen_ids:
                    continue

                seen_ids.add(record_id)
                page_count += 1

                datasets.append({
                    "id": record_id,
                    "title": title,
                    "url": full_url,
                    "year": "",
                    "description": ""
                })

                if rows is not None and len(datasets) >= rows:
                    return datasets

            print(f"  page {page}: {page_count} new records")

            if page_count == 0:
                break

    return datasets