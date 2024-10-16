CREATE TABLE gaia_merged_pdf AS
SELECT 
    task_id, 
    Question, 
    Level, 
    final_answer, 
    file_name, 
    file_path, 
    annotator_metadata
FROM 
    gaia_validation_dataset
WHERE 
    file_name LIKE '%.pdf'

UNION ALL

SELECT 
    task_id, 
    Question, 
    Level, 
    final_answer, 
    file_name, 
    file_path, 
    annotator_metadata
FROM 
    gaia_test_dataset
WHERE 
    file_name LIKE '%.pdf';
    
ALTER TABLE gaia_merged_pdf ADD COLUMN extracted_text TEXT;

select * from gaia_merged_pdf;
    
ALTER TABLE gaia_merged_pdf ADD COLUMN extracted_text TEXT;
ALTER TABLE gaia_merged_pdf DROP COLUMN extracted_text;    

ALTER TABLE gaia_merged_pdf ADD COLUMN ibm_extracted_text TEXT;

select * from gaia_merged_pdf;

ALTER TABLE gaia_merged_pdf ADD COLUMN `extracted_text` VARCHAR(255);

select Question from gaia_merged_pdf where file_name='7c215d46-91c7-424e-9f22-37d43ab73ea6.pdf';

SELECT task_id FROM gaia_merged_pdf;

select * from users;

