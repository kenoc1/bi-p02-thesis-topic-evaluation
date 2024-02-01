--- How many thesis topics are published in a week, in a month, in a year?
SELECT
    EXTRACT(YEAR FROM load_date) AS year,
    COUNT(DISTINCT topic_id) AS count
FROM h_topic
GROUP BY year
ORDER BY year;

SELECT
    TO_CHAR(load_date, 'Month') AS month,
    COUNT(DISTINCT topic_id) AS count
FROM h_topic
GROUP BY TO_CHAR(load_date, 'Month')
ORDER BY TO_DATE(TO_CHAR(load_date, 'Month'), 'Month');

SELECT
    EXTRACT(WEEK FROM load_date) AS week_number,
    COUNT(DISTINCT topic_id) AS count
FROM h_topic
GROUP BY week_number
ORDER BY week_number;

--- Which supervisor has the most thesis topics to offer?
SELECT contact, COUNT(DISTINCT topic_id) as topic_count
FROM s_topic
GROUP BY contact
ORDER BY topic_count DESC
LIMIT 1;

--- Which department has the most thesis topics to offer?
SELECT d.name AS department_name, COUNT(DISTINCT td.topic_id) AS topic_count
FROM s_department d
         JOIN l_topic_department td ON d.department_id = td.department_id AND d.load_date = td.load_date
GROUP BY d.name
ORDER BY topic_count DESC
LIMIT 1;

--- How many unique thesis topics are "removed from the list" in a week, in a month, in a year?
-- weekly
SELECT
    DATE_TRUNC('week', st.load_date) AS week,
    COUNT(DISTINCT st.topic_id) AS removed_topics
FROM
    s_topic st
        LEFT JOIN
    s_topic st_future ON st.topic_id = st_future.topic_id AND st_future.load_date > st.load_date
WHERE
    st_future.topic_id IS NULL
    AND st.load_date < '2023-12-18' -- incomplete week
GROUP BY
    week
ORDER BY
    week;

-- monthtly
SELECT
    TO_CHAR(DATE_TRUNC('month', st.load_date), 'Month') AS month,
    COUNT(DISTINCT st.topic_id) AS removed_topics
FROM
    s_topic st
        LEFT JOIN
    s_topic st_future ON st.topic_id = st_future.topic_id AND st_future.load_date > st.load_date
WHERE
    st_future.topic_id IS NULL
    AND st.load_date < '2023-12-18' -- incomplete week
GROUP BY
    DATE_TRUNC('month', st.load_date)
ORDER BY
    MIN(st.load_date);

SELECT
    TO_CHAR(DATE_TRUNC('month', st.load_date), 'YYYY-Month') AS year_month,
    COUNT(DISTINCT st.topic_id) AS removed_topics
FROM
    s_topic st
        LEFT JOIN
    s_topic st_future ON st.topic_id = st_future.topic_id AND st_future.load_date > st.load_date
WHERE
    st_future.topic_id IS NULL
    AND st.load_date < '2023-12-18' -- incomplete week
GROUP BY
    DATE_TRUNC('month', st.load_date)
ORDER BY
    DATE_TRUNC('month', st.load_date);

-- yearly
SELECT
    DATE_TRUNC('year', st.load_date) AS year,
    COUNT(DISTINCT st.topic_id) AS removed_topics
FROM
    s_topic st
        LEFT JOIN
    s_topic st_future ON st.topic_id = st_future.topic_id AND st_future.load_date > st.load_date
WHERE
    st_future.topic_id IS NULL
    AND st.load_date < '2023-12-18' -- incomplete week
GROUP BY
    year
ORDER BY
    year;

--- Create 1 task/business question of your own and answer this question: Which Thesis Type is Most Common?
SELECT thesis_type, COUNT(distinct topic_id) as count
FROM s_topic
GROUP BY thesis_type
ORDER BY count DESC
LIMIT 1;

--- Unique thesis topics published every month
SELECT
    date_trunc('month', load_date) AS month,
    COUNT(DISTINCT topic_id) AS unique_topics
FROM
    h_topic
GROUP BY
    month
ORDER BY
    month;

--- Average thesis topics for each department
SELECT
    d.name AS department,
    AVG(sub.topic_count) AS average_topics
FROM
    s_department d
        JOIN
    (SELECT
         td.department_id,
         t.load_date,
         COUNT(DISTINCT t.topic_id) AS topic_count
     FROM
         h_topic t
             JOIN
         l_topic_department td ON t.topic_id = td.topic_id AND t.load_date = td.load_date
     GROUP BY
         td.department_id, t.load_date) sub ON d.department_id = sub.department_id AND d.load_date = sub.load_date
GROUP BY
    d.name
ORDER BY
    average_topics DESC;

--- Create 1 KPI of your own, describe how to calculate it and put it on the dashboard:
--- This KPI gives an overview of the distribution of thesis topics across different thesis types.
SELECT
    thesis_type,
    COUNT(DISTINCT topic_id) AS topic_count
FROM
    s_topic
GROUP BY
    thesis_type;