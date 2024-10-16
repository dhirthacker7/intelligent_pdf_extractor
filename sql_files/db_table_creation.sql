CREATE DATABASE assignment_2;

use assignment_2;

CREATE TABLE gaia_validation_dataset (
    task_id VARCHAR(255) NOT NULL,
    Question TEXT NOT NULL,
    Level INT NOT NULL,
    final_answer TEXT NOT NULL,
    file_name VARCHAR(255),
    file_path VARCHAR(255),
    annotator_metadata TEXT NOT NULL,
    PRIMARY KEY (task_id)
);

use assignment_2;

CREATE TABLE gaia_test_dataset (
    task_id VARCHAR(255) NOT NULL,
    Question TEXT NOT NULL,
    Level INT NOT NULL,
    final_answer TEXT NOT NULL,
    file_name VARCHAR(255),
    file_path VARCHAR(255),
    annotator_metadata TEXT NOT NULL,
    PRIMARY KEY (task_id)
);

select * from gaia_test_dataset;

select count(*) from gaia_validation_dataset;

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,   -- Primary key with auto-increment
    username VARCHAR(255) NOT NULL UNIQUE, -- Username field, unique and not null
    password VARCHAR(255) NOT NULL,        -- Password field, not null
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp for user creation
);

use assignment_2;

select * from users;





