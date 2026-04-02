import sqlite3
from config import DB_PATH


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL,
            source_url TEXT,
            context_repository TEXT,
            record_id TEXT,
            title TEXT,
            author TEXT,
            year TEXT,
            uploader_name TEXT,
            uploader_email TEXT,
            doi TEXT,
            description TEXT,
            license TEXT,
            qda_file_url TEXT,
            file_url TEXT,
            local_directory TEXT,
            local_filename TEXT,
            local_file_path TEXT,
            file_type TEXT,
            is_qda_file INTEGER,
            download_status TEXT,
            downloaded_at TEXT,
            UNIQUE(source_name, record_id, file_url)
        )
    """)

    conn.commit()
    conn.close()

    print("Database initialized successfully.")