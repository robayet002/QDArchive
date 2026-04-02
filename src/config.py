from pathlib import Path

# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Data folders
DATA_DIR = BASE_DIR / "data"
DOWNLOAD_DIR = DATA_DIR / "downloads"
DATABASE_DIR = DATA_DIR / "database"
EXPORT_DIR = DATABASE_DIR / "exports"

# Database file
DB_PATH = DATABASE_DIR / "qdarchive_part1.db"

# Create directories if they don't exist
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
DATABASE_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# QDA project file extensions
QDA_EXTENSIONS = {
    ".qdpx",
    ".qpdx",
    ".qda",
    ".atlproj",
    ".nvp",
    ".nvpx"
}

# Open license keywords
OPEN_LICENSE_KEYWORDS = {
    "cc-by",
    "cc-by-4.0",
    "cc-by-sa",
    "cc-by-sa-4.0",
    "cc0",
    "cc0-1.0",
    "creative commons",
    "open"
}