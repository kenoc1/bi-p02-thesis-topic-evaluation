import datetime
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Database connection parameters
db_params = {
    'dbname': 'vault',
    'user': 'admin',
    'password': 'admin123',
    'host': 'localhost'
}

def read_json(file_path):
    if file_path.endswith('.json'):
        return pd.read_json(file_path)
    else:
        raise ValueError("Unsupported file format")

def instert_into_topics(data, entry_date, conn):
    cursor = conn.cursor()
    for key, value in data['topic_id'].items():  # Iterate based on topic_id
            # Prepare data from JSON file
            topic_id = value
            title = data['titel'].get(key, None)
            thesis_type = data['abschlussarbeitstyp'].get(key, None)
            # Add other fields based on your JSON structure
            department = data.get('studiengaenge', {}).get(key, None)
            work_type = data.get('art_der_arbeit', {}).get(key, None)
            contact = data.get('ansprechpartner', {}).get(key, None)
            created_date = data.get('erstellt', {}).get(key, None)
            url_topic_details = data.get('url_topic_details', {}).get(key, None)
            status = data.get('status', {}).get(key, None)

            # SQL statement for insertion
            insert_sql = """
            INSERT INTO topics (topic_id, entry_date, title, thesis_type, department, work_type, contact, created_date, url_topic_details, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (topic_id) DO NOTHING;  -- handles duplicate entries
            """
            # Execute SQL command
            try:
                cursor.execute(insert_sql, (topic_id, entry_date, title, thesis_type, department, work_type, contact, created_date, url_topic_details, status))
                conn.commit()
            except Exception as e:
                print(f"Error: {e}")
                conn.rollback()
            finally:
                cursor.close()

def insert_into_topics_additional(data, entry_date, conn):
    cursor = conn.cursor()
    for key, value in data['topic_id'].items():  # Iterate based on topic_id
        # Prepare data from JSON file
        topic_id = value
        title = data['titel'].get(key, None)
        description = data.get('Beschreibung', {}).get(key, None)
        home_institution = data.get('heimateinrichtung', {}).get(key, None)
        work_type = data.get('art_der_Arbeit', {}).get(key, None)
        author = data.get('Autor', {}).get(key, None)
        thesis_type = data.get('abschlussarbeitstyp', {}).get(key, None)
        status = data.get('status', {}).get(key, None)
        created_date = data.get('erstellt', {}).get(key, None)
        task = data.get('aufgabenstellung', {}).get(key, None)
        completed_on = data.get('abgeschlossen_am', {}).get(key, None)
        requirements = data.get('voraussetzung', {}).get(key, None)
        problem_statement = data.get('kurzdarstellung_der_problematik', {}).get(key, None)
        measurements = data.get('messungen_der_schulterstabilit\u00e4t', {}).get(key, None)
        literature = data.get('literatur', {}).get(key, None)
        other_details = data.get('sonstiges', {}).get(key, None)
        
        # SQL statement for insertion
        insert_sql = """
        INSERT INTO topics_additional (topic_id, entry_date, title, description, home_institution, work_type, author, thesis_type, status, created_date, task, completed_on, requirements, problem_statement, measurements, literature, other_details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (topic_id) DO NOTHING;  -- handles duplicate entries
        """
        # Execute SQL command
        try:
            cursor.execute(insert_sql, (topic_id, entry_date, title, description, home_institution, work_type, author, thesis_type, status, created_date, task, completed_on, requirements, problem_statement, measurements, literature, other_details))
            conn.commit()
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()
        finally:
            cursor.close()

def main():
    directory = 'data-uol-thesis-topics'

    conn = psycopg2.connect(**db_params)
    for root, dirs, files in os.walk(directory):
        for inside_dir in dirs:
            folder_date_string = inside_dir.split('_')[0]  # Cut the folder string from '_'
            for file in os.listdir(os.path.join(root, inside_dir)):
                if file.startswith('db-topics.json'):
                    entry_date = datetime.strptime(folder_date_string, "%Y%m%d").date()
                    file_path = os.path.join(root, inside_dir, file)
                    data = read_json(file_path)
                    instert_into_topics(data, entry_date, conn)
    for root, dirs, files in os.walk(directory):
        for inside_dir in dirs:
            folder_date_string = inside_dir.split('_')[0]  # Cut the folder string from '_'
            for file in os.listdir(os.path.join(root, inside_dir)):
                if file.startswith('db-thesis-additional.json'):
                    entry_date = datetime.strptime(folder_date_string, "%Y%m%d").date()
                    file_path = os.path.join(root, inside_dir, file)
                    data = read_json(file_path)
                    insert_into_topics_additional(data, entry_date, conn)
    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
