--- How many thesis topics are published in a week, in a month, in a year?
SELECT
    EXTRACT(YEAR FROM entry_date) AS year,
    COUNT(DISTINCT topic_id) AS count
FROM topics
GROUP BY year
ORDER BY year;

SELECT
    EXTRACT(MONTH FROM entry_date) AS month,
    COUNT(DISTINCT topic_id) AS count
FROM topics
GROUP BY month
ORDER BY month;

SELECT
    EXTRACT(WEEK FROM entry_date) AS week_number,
    COUNT(DISTINCT topic_id) AS count
FROM topics
GROUP BY week_number
ORDER BY week_number;

--- Which supervisor has the most thesis topics to offer?
SELECT contact, COUNT(DISTINCT topic_id) as topic_count
FROM topics
GROUP BY contact
ORDER BY topic_count DESC
LIMIT 1;


--- Which department has the most thesis topics to offer?
SELECT d.name AS department_name, COUNT(DISTINCT td.topic_id) AS topic_count
FROM department d
         JOIN topic_department td ON d.id = td.department_id
GROUP BY d.name
ORDER BY topic_count DESC
LIMIT 1;

--- How many thesis topics are "removed from the list" in a week, in a month, in a year?
WITH daily_counts AS (
    SELECT
        entry_date,
        COUNT(topic_id) AS topic_count
    FROM
        topics
    GROUP BY
        entry_date
),
     count_changes AS (
         SELECT
             entry_date,
             topic_count - LAG(topic_count) OVER (ORDER BY entry_date) AS change
         FROM
             daily_counts
     )
SELECT
    date_trunc('week', entry_date) AS week,
    SUM(CASE WHEN change < 0 THEN -change ELSE 0 END) AS removed_topics
FROM
    count_changes
GROUP BY
    week
ORDER BY
    week;

WITH daily_counts AS (
    SELECT
        entry_date,
        COUNT(topic_id) AS topic_count
    FROM
        topics
    GROUP BY
        entry_date
),
     count_changes AS (
         SELECT
             entry_date,
             topic_count - LAG(topic_count) OVER (ORDER BY entry_date) AS change
         FROM
             daily_counts
     )
SELECT
    date_trunc('month', entry_date) AS month,
    SUM(CASE WHEN change < 0 THEN -change ELSE 0 END) AS removed_topics
FROM
    count_changes
GROUP BY
    month
ORDER BY
    month;

WITH daily_counts AS (
    SELECT
        entry_date,
        COUNT(topic_id) AS topic_count
    FROM
        topics
    GROUP BY
        entry_date
),
     count_changes AS (
         SELECT
             entry_date,
             topic_count - LAG(topic_count) OVER (ORDER BY entry_date) AS change
         FROM
             daily_counts
     )
SELECT
    date_trunc('year', entry_date) AS year,
    SUM(CASE WHEN change < 0 THEN -change ELSE 0 END) AS removed_topics
FROM
    count_changes
GROUP BY
    year
ORDER BY
    year;

--- Create 1 task/business question of your own and answer this question: Which Thesis Type is Most Common?
SELECT thesis_type, COUNT(*) as count
FROM topics
GROUP BY thesis_type
ORDER BY count DESC
LIMIT 1;


--- Unique thesis topics published every month
SELECT
    date_trunc('month', entry_date) AS month,
    COUNT(DISTINCT topic_id) AS unique_topics
FROM
    topics
GROUP BY
    month
ORDER BY
    month;


--- Average thesis topics for each department
SELECT
    d.name AS department,
    AVG(t.topic_count) AS average_topics
FROM
    department d
        JOIN
    (SELECT
         td.department_id,
         COUNT(DISTINCT t.topic_id) AS topic_count
     FROM
         topics t
             JOIN
         topic_department td ON t.topic_id = td.topic_id AND t.entry_date = td.entry_date
     GROUP BY
         td.department_id, date_trunc('month', t.entry_date)) t ON d.id = t.department_id
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
    topics
GROUP BY
    thesis_type;
