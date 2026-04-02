from datetime import datetime
from database import get_connection


def insert_metadata(metadata: dict):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO projects (
            source_name,
            source_url,
            context_repository,
            record_id,
            title,
            author,
            year,
            uploader_name,
            uploader_email,
            doi,
            description,
            license,
            qda_file_url,
            file_url,
            local_directory,
            local_filename,
            local_file_path,
            file_type,
            is_qda_file,
            download_status,
            downloaded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        metadata.get("source_name"),
        metadata.get("source_url"),
        metadata.get("context_repository"),
        metadata.get("record_id"),
        metadata.get("title"),
        metadata.get("author"),
        metadata.get("year"),
        metadata.get("uploader_name"),
        metadata.get("uploader_email"),
        metadata.get("doi"),
        metadata.get("description"),
        metadata.get("license"),
        metadata.get("qda_file_url"),
        metadata.get("file_url"),
        metadata.get("local_directory"),
        metadata.get("local_filename"),
        metadata.get("local_file_path"),
        metadata.get("file_type"),
        metadata.get("is_qda_file"),
        metadata.get("download_status"),
        metadata.get("downloaded_at", datetime.utcnow().isoformat())
    ))

    conn.commit()
    conn.close()