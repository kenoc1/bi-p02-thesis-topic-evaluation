CREATE TABLE IF NOT EXISTS department (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS topics (
    topic_id VARCHAR(255) NOT NULL,
    title VARCHAR(255),
    thesis_type VARCHAR(50),
    work_type VARCHAR(255),
    contact VARCHAR(255),
    created_date DATE,
    url_topic_details TEXT,
    status VARCHAR(50),
    entry_date DATE NOT NULL,
    PRIMARY KEY (topic_id, entry_date)
);

CREATE TABLE IF NOT EXISTS topics_additional (
    topic_id VARCHAR(255) NOT NULL,
    entry_date DATE NOT NULL,
    title VARCHAR(255),
    description TEXT,
    home_institution VARCHAR(255),
    work_type VARCHAR(255),
    author VARCHAR(255),
    thesis_type VARCHAR(255),
    status VARCHAR(50),
    created_date DATE,
    task TEXT,
    completed_on DATE,
    requirements TEXT,
    PRIMARY KEY (topic_id, entry_date)
);

CREATE TABLE IF NOT EXISTS topic_department (
    topic_id VARCHAR(255),
    department_id INTEGER,
    entry_date DATE NOT NULL,
    FOREIGN KEY (topic_id, entry_date) REFERENCES topics(topic_id, entry_date),
    FOREIGN KEY (department_id) REFERENCES department(id),
    PRIMARY KEY (topic_id, department_id, entry_date)
);