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

def instert_into_topics(data, entry_date, conn):
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

        # SQL statement for insertion
        insert_sql = """
        INSERT INTO topics (topic_id, entry_date, title, thesis_type, work_type, contact, created_date, url_topic_details, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # SQL statement for fetching existing record
        fetch_sql = """
        SELECT topic_id, entry_date, title, thesis_type, work_type, contact, created_date, url_topic_details, status 
        FROM topics WHERE topic_id = %s AND entry_date = %s
        """
        cursor.execute(fetch_sql, (topic_id, entry_date.strftime('%m/%d/%Y')))
        existing_record = cursor.fetchone()
        
        # If record does not exist, insert it
        if existing_record is None:
            # Execute SQL command
            try:
                cursor.execute(insert_sql, (topic_id, entry_date.strftime('%m/%d/%Y'), title, thesis_type, work_type, contact, created_date.strftime('%m/%d/%Y'), url_topic_details, status))
                
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
                            cursor.execute("""
                                INSERT INTO department (name)
                                SELECT %s
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM department WHERE name = %s
                                )
                            """, (department, department))
                            cursor.execute("SELECT id FROM department WHERE name = %s", (department,))
                            department_id = cursor.fetchone()[0]
                            cursor.execute("""
                                INSERT INTO topic_department (topic_id, department_id, entry_date)
                                VALUES (%s, %s, %s)
                            """, (topic_id, department_id, entry_date))
                conn.commit()
            except Exception as e:
                print(f"Error: {e}")
                conn.rollback()
        else:
            created_date_db_format = created_date.strftime('%Y-%m-%d')
            new_data = (topic_id, datetime.strptime(entry_date.strftime('%Y-%m-%d'), '%Y-%m-%d').date(), title, thesis_type, work_type, contact, datetime.strptime(created_date_db_format, '%Y-%m-%d').date(), url_topic_details, status)                
            differences_found = False
            for i, (new, existing) in enumerate(zip(new_data, existing_record)):
                if new != existing:
                    if not differences_found:
                        print()
                        print("topics")
                        print(f"Data item: {new_data[0]}")
                        differences_found = True
                    print(f"Difference in column {i}: {existing} -> {new}")
            if not differences_found:
                duplicates_no_differences.append(new_data)
        
    cursor.close()

def insert_into_topics_additional(data, entry_date, conn):
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
        
        
        # SQL statement for insertion
        insert_sql = """
        INSERT INTO topics_additional (topic_id, entry_date, title, description, home_institution, work_type, author, thesis_type, status, created_date, task, completed_on, requirements)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # SQL statement for fetching existing record
        fetch_sql = "SELECT * FROM topics_additional WHERE topic_id = %s AND entry_date = %s"
        cursor.execute(fetch_sql, (topic_id, entry_date.strftime('%m/%d/%Y')))
        existing_record = cursor.fetchone()
        # If record does not exist, insert it
        if existing_record is None:
            # Execute SQL command
            try:
                cursor.execute(insert_sql, (topic_id, entry_date.strftime('%m/%d/%Y'), title, description, home_institution, work_type, author, thesis_type, status, created_date.strftime('%m/%d/%Y'), task, completed_on, requirements))
                conn.commit()
            except Exception as e:
                print(f"Error: {e}")
                conn.rollback()
        else:
            new_data = (topic_id, entry_date, title, description, home_institution, work_type, author, thesis_type, status, created_date, task, completed_on, requirements)
            differences_found = False
            for i, (new, existing) in enumerate(zip(new_data, existing_record)):
                if new != existing:
                    if not differences_found:
                        print()
                        print(f"topics_additional")
                        print(f"Data item: {new_data[0]}")
                        differences_found = True
                    print(f"Difference in column {i}: {existing} -> {new}")
            if not differences_found:
                duplicates_no_differences.append(new_data)
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
                if file.startswith('db-topics-additional.json'):
                    entry_date = datetime.strptime(folder_date_string, "%Y%m%d").date()
                    file_path = os.path.join(root, inside_dir, file)
                    data = read_json(file_path)
                    insert_into_topics_additional(data, entry_date, conn)
    # Close the connection
    conn.close()
    
    # Print data items with no differences but duplicated
    print("Data items with no differences but duplicated:")
    for item in duplicates_no_differences:
        print(item)

if __name__ == "__main__":
    main()
