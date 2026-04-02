import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

BASE_URL = "https://dataverse.no"
SEARCH_URL = "https://dataverse.no/dataverse/root/"

SEARCH_QUERIES = [
    "student",
]


def _extract_persistent_id(url: str) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    persistent_id = qs.get("persistentId", [""])[0].strip()
    if persistent_id:
        return persistent_id

    match = re.search(r"[?&]id=([^&]+)", url)
    if match:
        return match.group(1).strip()

    return ""


def _clean_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _extract_year_from_text(text: str) -> str:
    if not text:
        return ""

    m = re.search(r"\b(19|20)\d{2}\b", text)
    return m.group(0) if m else ""


def _fetch_dataset_metadata(session: requests.Session, dataset_url: str) -> dict:
    """
    Opens a dataset page and tries to extract a better title/description/year.
    This is optional enrichment so main.py gets nicer metadata.
    """
    meta = {
        "title": "",
        "description": "",
        "year": "",
    }

    try:
        r = session.get(dataset_url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"    could not open dataset page {dataset_url}: {e}")
        return meta

    soup = BeautifulSoup(r.text, "html.parser")

    # Better title from page heading if available
    heading = soup.select_one("h1") or soup.select_one("title")
    if heading:
        meta["title"] = _clean_text(heading.get_text(" ", strip=True))

    # Try common description areas
    desc_candidates = [
        soup.select_one('meta[name="description"]'),
        soup.select_one('meta[property="og:description"]'),
        soup.select_one(".dataset-description"),
        soup.select_one("#datasetDescription"),
    ]

    for item in desc_candidates:
        if not item:
            continue

        if item.name == "meta":
            desc = (item.get("content") or "").strip()
        else:
            desc = _clean_text(item.get_text(" ", strip=True))

        if desc:
            meta["description"] = desc
            break

    # Try to infer year from page text
    page_text = soup.get_text(" ", strip=True)
    meta["year"] = _extract_year_from_text(page_text)

    return meta


def _extract_file_records_from_dataset(session: requests.Session, dataset_url: str, dataset_title: str, dataset_description: str, dataset_year: str) -> list[dict]:
    """
    From a dataset page, extract file.xhtml links.
    These file pages contain the 'Access File' button that your main.py
    can already follow and download from.
    """
    records = []
    seen_file_ids = set()

    try:
        r = session.get(dataset_url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"    failed to open dataset page {dataset_url}: {e}")
        return records

    soup = BeautifulSoup(r.text, "html.parser")

    for link in soup.select('a[href*="/file.xhtml"]'):
        href = (link.get("href") or "").strip()
        if not href:
            continue

        file_page_url = urljoin(BASE_URL, href)
        file_pid = _extract_persistent_id(file_page_url)

        if not file_pid:
            continue

        if file_pid in seen_file_ids:
            continue
        seen_file_ids.add(file_pid)

        link_text = _clean_text(link.get_text(" ", strip=True))

        # Prefer visible file title if present, else fall back to dataset title
        title = link_text or dataset_title or file_pid

        records.append({
            "id": file_pid,
            "title": title,
            "url": file_page_url,          # main.py will open this page
            "year": dataset_year,
            "description": dataset_description,
        })

    return records


def search_dv(rows=None, per_page=10, max_pages=100):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    results = []
    seen_dataset_ids = set()
    seen_file_ids_global = set()

    session = requests.Session()
    session.headers.update(headers)

    for query in SEARCH_QUERIES:
        print(f"DataverseNO query: {query}")

        for page in range(1, max_pages + 1):
            params = {
                "q": query,
                "page": page,
                "sort": "score",
                "order": "desc",
                "types": "dataverses:datasets:files"
            }

            r = session.get(SEARCH_URL, params=params, timeout=30)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")

            dataset_links = []
            seen_dataset_urls_on_page = set()

            for link in soup.select('a[href*="/dataset.xhtml"]'):
                href = (link.get("href") or "").strip()
                title = _clean_text(link.get_text(" ", strip=True))

                if not href:
                    continue

                full_url = urljoin(BASE_URL, href)

                if "/dataset.xhtml" not in full_url:
                    continue

                if full_url in seen_dataset_urls_on_page:
                    continue
                seen_dataset_urls_on_page.add(full_url)

                persistent_id = _extract_persistent_id(full_url)
                if not persistent_id:
                    continue

                dataset_links.append({
                    "id": persistent_id,
                    "title": title,
                    "url": full_url,
                })

            if not dataset_links:
                print(f"  page {page}: 0 dataset records")
                break

            page_file_count = 0
            new_dataset_count = 0

            for dataset in dataset_links:
                dataset_id = dataset["id"]
                dataset_url = dataset["url"]
                dataset_title = dataset["title"]

                if dataset_id in seen_dataset_ids:
                    continue

                seen_dataset_ids.add(dataset_id)
                new_dataset_count += 1

                # get better metadata from dataset page
                meta = _fetch_dataset_metadata(session, dataset_url)

                better_title = meta["title"] or dataset_title or dataset_id
                better_description = meta["description"] or ""
                better_year = meta["year"] or ""

                file_records = _extract_file_records_from_dataset(
                    session=session,
                    dataset_url=dataset_url,
                    dataset_title=better_title,
                    dataset_description=better_description,
                    dataset_year=better_year,
                )

                for rec in file_records:
                    if rec["id"] in seen_file_ids_global:
                        continue

                    seen_file_ids_global.add(rec["id"])
                    results.append(rec)
                    page_file_count += 1

                    if rows is not None and len(results) >= rows:
                        return results

            print(f"  page {page}: {new_dataset_count} new datasets, {page_file_count} file pages found")

            # if this page added no new datasets, stop
            if new_dataset_count == 0:
                break

    return results
