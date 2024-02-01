from datetime import datetime
import os
import re
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
duplicates_no_differences = []

def read_json(file_path):
    if file_path.endswith('.json'):
        return pd.read_json(file_path)
    else:
        raise ValueError("Unsupported file format")

def insert_into_topics(data, load_date, end_load_date, conn):
    cursor = conn.cursor()
    for key, value in data['topic_id'].items():  # Iterate based on topic_id
        # Prepare data from JSON file
        topic_id = value
        title = data['titel'].get(key, None)
        thesis_type = data['abschlussarbeitstyp'].get(key, None)
        department_string = data.get('studiengaenge', {}).get(key, None)                    
        work_type = data.get('art_der_arbeit', {}).get(key, None)
        contact = data.get('ansprechpartner', {}).get(key, None)
        created_date = datetime.strptime(data.get('erstellt', {}).get(key, None), '%d.%m.%Y').date()
        url_topic_details = data.get('url_topic_details', {}).get(key, None)
        status = data.get('status', {}).get(key, None)

        # SQL statement for fetching existing record
        fetch_sql = """
        SELECT topic_id
        FROM s_topic WHERE (LEVENSHTEIN(title, %s) <= 5 AND load_date = %s) OR (topic_id = %s AND load_date = %s)
        """
        cursor.execute(fetch_sql, (title, load_date.strftime('%m/%d/%Y'), topic_id, load_date.strftime('%m/%d/%Y')))
        existing_record = cursor.fetchone()

        # If record does not exist, insert it
        if existing_record is None:
            # h_topic
            insert_sql_h_topic = """
            INSERT INTO h_topic (topic_id, load_date, record_source)
            VALUES (%s, %s, %s)
            """
            cursor.execute(insert_sql_h_topic, (topic_id, load_date.strftime('%m/%d/%Y'), 'importer.py'))

            # s_topic
            insert_sql_s_topic = """
            INSERT INTO s_topic (topic_id, load_date, end_date, title, thesis_type, work_type, contact, created_at, url_topic_details, status, record_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql_s_topic, (topic_id, load_date.strftime('%m/%d/%Y'), end_load_date.strftime('%m/%d/%Y'), title, thesis_type, work_type, contact, created_date.strftime('%m/%d/%Y'), url_topic_details, status, 'importer.py'))

            # h_department and l_topic_department
            if department_string is not None:
                department_string = department_string.replace("Zugeordnete Studiengänge", "")
                split_departments = re.split('(Master|Bachelor|Zwei-Fächer-Bachelor|Fach-Bachelor|Staatsexamen)', department_string)

                if split_departments[0] == "Keine Studiengänge zugeordnet":
                        departments = split_departments;
                else:
                    split_departments = [i for i in split_departments if i]
                    departments = [split_departments[i] + split_departments[i + 1] for i in range(0, len(split_departments), 2)]
                for department in departments:                
                    department = department.strip()
                    if department:
                        # h_department
                        cursor.execute("""
                            INSERT INTO h_department (load_date, record_source)
                            SELECT %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM s_department WHERE name = %s
                            )
                            RETURNING department_id
                        """, (load_date.strftime('%m/%d/%Y'), 'importer.py', department))
                        result = cursor.fetchone()
                        if result is None:
                            cursor.execute("SELECT department_id FROM s_department WHERE LEVENSHTEIN(name, %s) <= 2", (department,))
                            department_id = cursor.fetchone()[0]
                        else:
                            department_id = result[0]
                            # s_department
                            insert_sql_s_department = """
                                INSERT INTO s_department (department_id, load_date, end_date, name)
                                VALUES (%s, %s, %s, %s)
                            """
                            cursor.execute(insert_sql_s_department, (department_id, load_date.strftime('%m/%d/%Y'), end_load_date.strftime('%m/%d/%Y'), department))

                        # l_topic_department
                        insert_sql_l_topic_department = """
                        INSERT INTO l_topic_department (topic_id, department_id, load_date, record_source)
                        VALUES (%s, %s, %s, %s)
                        """
                        cursor.execute(insert_sql_l_topic_department, (topic_id, department_id, load_date.strftime('%m/%d/%Y'), 'importer.py'))

        conn.commit()

def update_s_topics(data, load_date, conn):
    cursor = conn.cursor()
    for key, value in data['topic_id'].items():  # Iterate based on topic_id
        # Prepare data from JSON file
        topic_id = value
        title = data['titel'].get(key, None)
        description = data.get('beschreibung', {}).get(key, None)
        home_institution = data.get('heimateinrichtung', {}).get(key, None)
        work_type = data.get('art_der_arbeit', {}).get(key, None)
        author = data.get('autor', {}).get(key, None)
        thesis_type = data.get('abschlussarbeitstyp', {}).get(key, None)
        status = data.get('status', {}).get(key, None)
        created_date = datetime.strptime(data.get('erstellt', {}).get(key, None), '%d.%m.%Y').date()
        task = data.get('aufgabenstellung', {}).get(key, None)
        completed_on = None
        if data.get('abgeschlossen_am', {}).get(key, None) is not None:
            completed_on = datetime.strptime(data.get('abgeschlossen_am', {}).get(key), '%d.%m.%Y').date()
        requirements = data.get('voraussetzung', {}).get(key, None)

        # SQL statement for updating existing record
        update_sql = """
        UPDATE s_topic 
        SET title = %s, description = %s, home_institution = %s, work_type = %s, author = %s, thesis_type = %s, 
            status = %s, task = %s, completed_on = %s, requirements = %s, created_at = %s
        WHERE LEVENSHTEIN(title, %s) <= 5 AND load_date = %s
        """
        cursor.execute(update_sql, (title, description, home_institution, work_type, author, thesis_type, status, task, completed_on, requirements, created_date.strftime('%m/%d/%Y'), title, load_date))

        conn.commit()
    cursor.close()

def main():
    directory = 'data-uol-thesis-topics'
    end_load_date = datetime.strptime('19.12.2023', '%d.%m.%Y')

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    # For levensthein distance
    cursor.execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;")
    conn.commit()
    cursor.close()
    for root, dirs, files in os.walk(directory):
       for inside_dir in dirs:
           folder_date_string = inside_dir.split('_')[0]  # Cut the folder string from '_'
           for file in os.listdir(os.path.join(root, inside_dir)):
               if file.startswith('db-topics.json'):
                   load_date = datetime.strptime(folder_date_string, "%Y%m%d").date()
                   file_path = os.path.join(root, inside_dir, file)
                   data = read_json(file_path)
                   insert_into_topics(data, load_date, end_load_date, conn)
    for root, dirs, files in os.walk(directory):
        for inside_dir in dirs:
            folder_date_string = inside_dir.split('_')[0]  # Cut the folder string from '_'
            for file in os.listdir(os.path.join(root, inside_dir)):
                if file.startswith('db-topics-additional.json'):
                    load_date = datetime.strptime(folder_date_string, "%Y%m%d").date()
                    file_path = os.path.join(root, inside_dir, file)
                    data = read_json(file_path)
                    update_s_topics(data, load_date, conn)
    # Close the connection
    conn.close()
    
    # Print data items with no differences but duplicated
    print("Data items with no differences but duplicated:")
    for item in duplicates_no_differences:
        print(item)

if __name__ == "__main__":
    main()
