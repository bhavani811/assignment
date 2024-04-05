# Pupil dataset assignment solution

step 1: create bucket new globe with two sub folders pupildata and pupiattendance which will recieve daily attendance files from external sources
step 2: Invoke "pupil_pipeline" airflow dag which contains the task's to load latest files from the subfolders to temp tables in biqguery 
(ie new-globe-419310.de_assignment.temp_pupil_data​,new-globe-419310.de_assignment.temp_pupil_attdendance)
step 3: Data to the temp_pupil_data, temp_pupil_attdendance from bucket to bq table are written in append mode
step 4: load_fact_table,load_dim_tables tasks are invoked from the airflow dah to call the stored procedure to load the fact and dim tables 
step 5 : Data cleansing such as removing duplicates, standaraize of grade names, stream,acaddemy name etc are done in the load_fact_table,load_dim_tables stored procedure
step 6 : An entry to log table (log_table) is added after the every load of the data to fact and dim tables to handle the incremental loads based on the log table entry 
step 7 : Use of Merge clause to identlty delta data and merge data to target tables.
step 8 : Airflow dag is assumed to be scheduled at 6PM every day 
