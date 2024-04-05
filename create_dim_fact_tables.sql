#create scripts for fact and dim tables fact_pupil_attendance,dim_pupil,dim_date,dim_academy,dim_grade
#create log table which contains log details for each run for data loading to fact and dim tables

CREATE TABLE `new-globe-419310.de_assignment.dim_pupil`  (
    pupil_id INT64,
    first_name STRING,
    middle_name STRING,
    last_name STRING,
    mail_id STRING,
    dob DATE,
    load_date DATE
   
);

CREATE TABLE `new-globe-419310.de_assignment.dim_date` (
    snapshot_date DATE,
    year INT64,
    month INT64,
    quarter INT64,
    load_date DATE
);

CREATE TABLE `new-globe-419310.de_assignment.dim_academy`(
    academy_id INT64,
    academy_name STRING,
    academy_phone_number INT64,
    academy_address STRING,
    load_date DATE
    
);


CREATE TABLE `new-globe-419310.de_assignment.dim_grade`(
    grade_id INT64,
    grade_name STRING,
    load_date DATE
    
);

CREATE TABLE `new-globe-419310.de_assignment.log_table`(
    table_name STRING,
    load_date DATE
    
);

CREATE TABLE `new-globe-419310.de_assignment.fact_pupil_attendance`
(
    pupil_id INT64,
    snapshot_date date,
    grade_name STRING,
    academy_name STRING,
    stream STRING,
    attendance STRING,
    status STRING,
    load_date DATE
)
PARTITION BY snapshot_date