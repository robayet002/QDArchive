import re
import requests
from pathlib import Path


def sanitize_filename(filename: str) -> str:
    filename = str(filename).strip()
    filename = re.sub(r'[<>:"/\\|?*]+', "_", filename)
    filename = re.sub(r"\s+", " ", filename).strip()
    filename = filename.rstrip(". ")
    return filename[:150] or "unknown"


def download_file(url: str, destination: Path) -> bool:
    headers = {
        "User-Agent": "Seeding-QDArchive/1.0"
    }

    try:
        response = requests.get(url, headers=headers, stream=True, timeout=60)
        response.raise_for_status()

        destination.parent.mkdir(parents=True, exist_ok=True)

        with open(destination, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        return True

    except requests.RequestException as error:
        print(f"Download failed for {url}: {error}")
        return False