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

--- How many thesis topics are "removed from the list" in a week, in a month, in a year?
-- weekly
WITH daily_counts AS (
    SELECT
        load_date,
        COUNT(topic_id) AS topic_count
    FROM
        h_topic
    GROUP BY
        load_date
),
     count_changes AS (
         SELECT
             load_date,
             topic_count - LAG(topic_count) OVER (ORDER BY load_date) AS change
         FROM
             daily_counts
     )
SELECT
    date_trunc('week', load_date) AS week,
    SUM(CASE WHEN change < 0 THEN -change ELSE 0 END) AS removed_topics
FROM
    count_changes
GROUP BY
    week
ORDER BY
    week;

-- monthtly
WITH daily_counts AS (
    SELECT
        load_date,
        COUNT(topic_id) AS topic_count
    FROM
        h_topic
    GROUP BY
        load_date
),
     count_changes AS (
         SELECT
             load_date,
             topic_count - LAG(topic_count) OVER (ORDER BY load_date) AS change
         FROM
             daily_counts
     )
SELECT
    date_trunc('month', load_date) AS month,
    SUM(CASE WHEN change < 0 THEN -change ELSE 0 END) AS removed_topics
FROM
    count_changes
GROUP BY
    month
ORDER BY
    month;

-- yearly
WITH daily_counts AS (
    SELECT
        load_date,
        COUNT(topic_id) AS topic_count
    FROM
        h_topic
    GROUP BY
        load_date
),
     count_changes AS (
         SELECT
             load_date,
             topic_count - LAG(topic_count) OVER (ORDER BY load_date) AS change
         FROM
             daily_counts
     )
SELECT
    date_trunc('year', load_date) AS year,
    SUM(CASE WHEN change < 0 THEN -change ELSE 0 END) AS removed_topics
FROM
    count_changes
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