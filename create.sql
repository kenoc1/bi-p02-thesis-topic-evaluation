-- Hubs
CREATE TABLE IF NOT EXISTS h_topic (
    topic_id VARCHAR(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    record_source VARCHAR(255),
    PRIMARY KEY (topic_id, load_date)
);

CREATE TABLE IF NOT EXISTS h_department (
    department_id SERIAL NOT NULL,
    load_date TIMESTAMP NOT NULL,
    record_source VARCHAR(255),
    PRIMARY KEY (department_id)
);

-- Links
CREATE TABLE IF NOT EXISTS l_topic_department (
    link_id SERIAL PRIMARY KEY,
    topic_id VARCHAR(255) NOT NULL,
    department_id INTEGER NOT NULL,
    load_date TIMESTAMP NOT NULL,
    record_source VARCHAR(255),
    FOREIGN KEY (topic_id, load_date) REFERENCES h_topic(topic_id, load_date),
    FOREIGN KEY (department_id) REFERENCES h_department(department_id)
);

-- Satellites
CREATE TABLE IF NOT EXISTS s_topic (
    topic_id VARCHAR(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    title VARCHAR(255),
    description TEXT,
    home_institution VARCHAR(255),
    thesis_type VARCHAR(50),
    author VARCHAR(255),
    work_type VARCHAR(255),
    contact VARCHAR(255),
    url_topic_details TEXT,
    requirements TEXT,
    task TEXT,
    status VARCHAR(50),
    completed_on Date,
    created_at TIMESTAMP,
    record_source VARCHAR(255),
    PRIMARY KEY (topic_id, load_date),
    FOREIGN KEY (topic_id, load_date) REFERENCES h_topic(topic_id, load_date)
);

CREATE TABLE IF NOT EXISTS s_department (
    department_id INTEGER NOT NULL,
    load_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    name TEXT NOT NULL,
    record_source VARCHAR(255),
    PRIMARY KEY (department_id, load_date),
    FOREIGN KEY (department_id) REFERENCES h_department(department_id)
);