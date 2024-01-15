BEGIN;

DELETE FROM s_topic
WHERE load_date = '2023-05-24';

-- l_topic_department
DELETE FROM l_topic_department
WHERE load_date = '2023-05-24';

-- h_topic
DELETE FROM h_topic
WHERE load_date = '2023-05-24';

-- h_department
DELETE FROM h_department
WHERE load_date = '2023-05-24';

-- s_department
DELETE FROM s_department
WHERE load_date = '2023-05-24';

-- h_topic
INSERT INTO h_topic
SELECT topic_id, '2023-05-24'::timestamp, record_source
FROM h_topic
WHERE load_date = '2023-05-23';

-- h_department
INSERT INTO h_department
SELECT department_id, '2023-05-24'::timestamp, record_source
FROM h_department
WHERE load_date = '2023-05-23';

-- l_topic_department
INSERT INTO l_topic_department
SELECT link_id, topic_id, department_id, '2023-05-24'::timestamp, record_source
FROM l_topic_department
WHERE load_date = '2023-05-23';

-- s_topic
INSERT INTO s_topic
SELECT topic_id, '2023-05-24'::timestamp, end_date, title, description, home_institution, thesis_type, author, work_type, contact, url_topic_details, requirements, task, status, completed_on, created_at, record_source
FROM s_topic
WHERE load_date = '2023-05-23';

-- s_department
INSERT INTO s_department
SELECT department_id, '2023-05-24'::timestamp, end_date, name, record_source
FROM s_department
WHERE load_date = '2023-05-23';

COMMIT;