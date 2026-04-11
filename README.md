# QDArchive Seeding — Part 1: Data Acquisition

**Student ID:** 23135689  
**Course:** Seeding QDArchive — Applied Software Engineering Seminar/Project  
**Professor:** Dirk Riehle, FAU Erlangen-Nürnberg  
**Semester:** Winter 2025/26 + Summer 2026  
**Part 1 Deadline:** April 17, 2026  

---

## Overview

This repository contains the pipeline for **Part 1 (Data Acquisition)** of the Seeding QDArchive project.

The goal is to collect qualitative research projects — especially those containing QDA files (`.qdpx`, `.nvp`, `.mx24`, etc.) — from two assigned repositories, download all available files, and record structured metadata in a SQLite database named `23135689-seeding.db`.

**Assigned repositories:**

| ID | Name | URL |
|----|------|-----|
| 1  | IHSN | https://catalog.ihsn.org |
| 2  | Sikt | https://sikt.no/en/find-data |

---

## Repository Structure

```
QDA_Project/
├── 23135689-seeding.db         ← SQLite database (committed to repo root)
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

## Setup and Running

### 1. Clone the repository
```bash
git clone https://github.com/tosiful/QDA_Project-
cd QDA_Project-
```

### 2. Create and activate a virtual environment (Windows)
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the full pipeline
```bash
python main.py
```

This will:
- Initialise `23135689-seeding.db`
- Scrape IHSN via their public NADA REST API
- Scrape Sikt via the CESSDA Data Catalogue API
- Download all available project files into `data/`
- Print database statistics
- Export all tables to `export_csv/`

### 5. Run a single scraper only
```bash
python main.py --ihsn-only
python main.py --sikt-only
```

### 6. Retry failed downloads
```bash
python scripts/retry_failed.py
```

### 7. View stats
```bash
python main.py --stats
```

### 8. Export CSVs manually
```bash
python main.py --export
```

---

## Download Methods

| Repository | Method | Reason |
|-----------|--------|--------|
| IHSN | `API-CALL` | IHSN runs a public NADA REST API at `catalog.ihsn.org/index.php/api/` |
| Sikt | `SCRAPING` | Surveybanken is a JS SPA; we reverse-engineer CESSDA OAI-PMH + NSD HTML pages |

---

## File Download Notes

- Files larger than **500 MB** are recorded as `FAILED_TOO_LARGE` and skipped.
- Files returning HTTP 401/403 are recorded as `FAILED_LOGIN_REQUIRED`.
- Connection errors / timeouts / 5xx responses are `FAILED_SERVER_UNRESPONSIVE`.
- **Primary rule:** downloaded data is stored as-is; no modification.

---

## Data Folder

The `data/` folder is **not committed to GitHub** (see `.gitignore`).  
It is uploaded to FAUbox/Google Drive — link to be added here after submission.

Structure:
```
data/
  ihsn/{project_id}/file1.pdf, file2.docx, ...
  sikt/NSD{id}/file1.pdf, ...
```

---

## Technical Challenges (Data — Not Programming)

> This section reports data quality and structural challenges encountered during
> acquisition, as requested by Prof. Riehle.

### 1. IHSN: Mostly Quantitative Microdata, Not Qualitative QDA Files
The IHSN catalog (12,826 projects) is dominated by large-scale quantitative
household surveys (census microdata, living-standards surveys). Genuine qualitative
research projects — especially those containing `.qdpx` or other QDA analysis files
— are rare or absent. Keyword searches for `"qualitative"` and `"interview"` still
return projects that are *about* qualitative topics but contain only `.pdf`
questionnaires or restricted SPSS `.sav` files, not interview transcripts or analysis
files. This is a fundamental mismatch between the repository's primary content type
and QDArchive's target data.

### 2. IHSN: Most Files Are Login-Restricted (Licensed Access)
The majority of IHSN projects are marked `Licensed` or `Enclave` access.
Even when the metadata is publicly visible through the API, the actual data files
cannot be downloaded without registering, submitting a data request, and obtaining
approval. This means `FILES.status` is systematically `FAILED_LOGIN_REQUIRED` for
most projects. The public API exposes metadata but not the actual microdata.

### 3. Sikt (Surveybanken): No Public Bulk Search API
Sikt's Surveybanken is a modern React/JavaScript SPA. There is no documented public
REST API for bulk search or metadata harvest. All visible API calls from the browser
use internal endpoints with session tokens. To work around this, the scraper uses the
**CESSDA Data Catalogue** (which aggregates Sikt/NSD metadata) as an intermediary.
However, this introduces a dependency on CESSDA's index freshness and completeness.
Some recently deposited Sikt datasets may not yet be indexed by CESSDA.

### 4. Sikt: Most Datasets Require Ordering (Not Free Download)
Like IHSN, most Sikt datasets are access-restricted. They must be formally "ordered"
through the Sikt web interface — a manual process requiring a user account and data
usage agreement. Only a small subset is marked as freely downloadable. Files for
restricted datasets are recorded as `FAILED_LOGIN_REQUIRED`.

### 5. Keyword Inconsistency Across Both Repositories
Keywords are stored in raw form as per the professor's primary rule (do not change
data during acquisition). However, there is significant inconsistency:
- Some projects use comma-separated keyword strings instead of individual terms
  (e.g. `"interlanguage pragmatics, EFL learners, scoping review"`)
- Capitalisation varies (`EFL learners` vs `efl-learners`)
- Some keywords are in Norwegian (Sikt), others in English
These will be addressed during Part 2 classification.

### 6. License Information Often Missing or Ambiguous
Many IHSN projects do not include a machine-readable license field in the API
response. Instead, access conditions are described in free text (e.g.
`"Licensed files are accessible only to registered users"`) which does not map
cleanly to the CC/ODbL taxonomy. These are stored as-is per the primary rule.

### 7. Multiple Versions and Duplicate Projects
IHSN projects frequently have multiple versions (e.g. `v01_M`, `v7.5_A_IPUMS`),
each as a separate catalog entry with its own ID. These are stored as separate
projects since version information is part of the IDNO string. Deduplication
is a Part 2 task.

---

## Submission Checklist

- [x] `23135689-seeding.db` in repo root
- [x] All 6 tables created with correct schema and enum constraints
- [x] `download_method` is `API-CALL` (IHSN) or `SCRAPING` (Sikt)
- [x] `download_repository_folder` and `download_project_folder` populated
- [x] Files recorded with correct `DOWNLOAD_RESULT` enum values
- [x] Persons recorded with `PERSON_ROLE` enum values
- [x] Licenses stored as original strings (with normalisation where unambiguous)
- [x] README with Technical Challenges section
- [ ] `data/` folder uploaded to FAUbox/Google Drive (link TBD)
- [ ] Submission form filled in with GitHub link + data folder link
- [ ] Git tag `part-1-release` created

---

## Creating the Git Tag

After the final commit:
```bash
git add .
git commit -m "Part 1: Data acquisition complete"
git tag part-1-release
git push origin main --tags
```

---

## Contact

Student: 23135689  
Course forum: https://myc.uni1.de/course/view.php?id=23  
Professor: dirk.riehle@fau.de
