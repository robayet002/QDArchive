import csv
import sqlite3
from config import DB_PATH, EXPORT_DIR


def export_projects_to_csv():
    output_file = EXPORT_DIR / "projects.csv"

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM projects")
    rows = cursor.fetchall()

    column_names = [description[0] for description in cursor.description]

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(column_names)
        writer.writerows(rows)

    conn.close()
    print(f"CSV exported to: {output_file}")


if __name__ == "__main__":
    export_projects_to_csv()