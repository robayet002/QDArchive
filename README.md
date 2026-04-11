# QDArchive Seeding — Part 1: Data Acquisition

**Student ID:** 23277555  
**Course:** Seeding QDArchive  
**Professor:** Dirk Riehle, FAU Erlangen-Nürnberg  
**Semester:** Winter 2025/26 + Summer 2026  

---

## Overview

This repository contains the pipeline for **Part 1 (Data Acquisition)** of the Seeding QDArchive project.

The goal is to collect qualitative research projects — especially those containing QDA files (`.qdpx`, `.nvp`, `.mx24`, etc.) — from two assigned repositories, download all available files, and record structured metadata in a SQLite database named `23135689-seeding.db`.

**Assigned repositories:**

| ID | Name      | URL |
|----|------     |-----|
| 1  | Sada      | https://www.datafirst.uct.ac.za |
| 2  | Dataverse | https://dataverse.no |

---

## Repository Structure

```
QDA_Project/
├── 23277555seeding.db         ← SQLite database (committed to repo root)
├── main.py                     ← Pipeline entry point
├── requirements.txt
├── .gitignore
├── README.md
│
├── db/
│   ├── schema.sql              ← Table definitions (6 tables)
│   └── database.py             ← DB connection + insert helpers
│
├── pipeline/
│   └── downloader.py           ← File downloader with failure classification
│
├── scrapers/
│   ├── ihsn_scraper.py         ← IHSN NADA REST API scraper
│   └── sikt_scraper.py         ← Sikt/NSD via CESSDA catalogue scraper
│
├── export/
│   └── export_csv.py           ← Export all tables to CSV
│
├── scripts/
│   └── retry_failed.py         ← Retry FAILED_SERVER_UNRESPONSIVE downloads
│
└── data/                       ← Downloaded files (NOT committed — see FAUbox link below)
    ├── ihsn/
    │   └── {project_id}/
    │       └── files...
    └── sikt/
        └── NSD{id}/
            └── files...
```

---

## Database Schema

The database (`23135689-seeding.db`) contains six tables:

### REPOSITORIES
Seed table of known repositories.

| Column | Type    | Notes |
|--------|---------|-------|
| id     | INTEGER | Primary key |
| name   | TEXT    | Short name e.g. `ihsn` |
| url    | TEXT    | Top-level URL |

### PROJECTS
One row per qualitative research project found.

| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | Primary key |
| query_string | TEXT | Search query that found this project |
| repository_id | INTEGER | FK → REPOSITORIES |
| repository_url | TEXT | e.g. `https://catalog.ihsn.org` |
| project_url | TEXT | Full URL to the project page |
| version | TEXT | Version string if any |
| title | TEXT | Project title |
| description | TEXT | Abstract/description |
| language | TEXT | BCP 47 language tag |
| doi | TEXT | DOI URL |
| upload_date | TEXT | Date of upload |
| download_date | TEXT | Timestamp of our download |
| download_repository_folder | TEXT | e.g. `ihsn` |
| download_project_folder | TEXT | e.g. `13286` |
| download_version_folder | TEXT | If versioned |
| download_method | TEXT | `SCRAPING` or `API-CALL` |

### FILES
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | Primary key |
| project_id | INTEGER | FK → PROJECTS |
| file_name | TEXT | Filename on disk |
| file_type | TEXT | Extension (lowercase, no dot) |
| status | TEXT | `SUCCEEDED`, `FAILED_SERVER_UNRESPONSIVE`, `FAILED_LOGIN_REQUIRED`, `FAILED_TOO_LARGE` |

### KEYWORDS
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | Primary key |
| project_id | INTEGER | FK → PROJECTS |
| keyword | TEXT | Original keyword string from source |

### PERSON_ROLE
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | Primary key |
| project_id | INTEGER | FK → PROJECTS |
| name | TEXT | Person's name |
| role | TEXT | `AUTHOR`, `UPLOADER`, `OWNER`, `OTHER`, `UNKNOWN` |

### LICENSES
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | Primary key |
| project_id | INTEGER | FK → PROJECTS |
| license | TEXT | e.g. `CC BY 4.0`, `CC0`, original string if unmapped |

---

