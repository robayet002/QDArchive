import re
import requests
from pathlib import Path


import os
import re
import unicodedata
from pathlib import Path

def sanitize_filename(name: str, max_length: int = 100) -> str:
    if not name:
        return "downloaded_file"

    name = str(name).strip()

    # Try to repair common mojibake like â€œ â€™ â€
    try:
        repaired = name.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        if repaired and repaired.count("â") < name.count("â"):
            name = repaired
    except Exception:
        pass

    # Normalize unicode
    name = unicodedata.normalize("NFKC", name)

    # Remove non-printable/control chars
    name = "".join(ch for ch in name if ch.isprintable())

    # Remove Windows-invalid filename chars
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", name)

    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    # Remove trailing spaces/dots
    name = name.rstrip(" .")

    if not name:
        name = "downloaded_file"

    # Preserve extension if possible while shortening
    suffix = Path(name).suffix
    stem = Path(name).stem

    if len(name) > max_length:
        keep = max_length - len(suffix)
        stem = stem[:max(1, keep)].rstrip(" ._")
        name = f"{stem}{suffix}"

    return name or "downloaded_file"


def download_file(url, destination):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        with requests.get(url, headers=headers, stream=True, timeout=60) as response:
            response.raise_for_status()

            with open(destination, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)

        return True
    except Exception as e:
        print(f"Download failed: {url} -> {e}")
        return False
