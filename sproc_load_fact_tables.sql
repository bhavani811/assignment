CREATE OR REPLACE PROCEDURE `new-globe-419310.de_assignment.load_fct_table`()
BEGIN
# check for the log table for the last loaded date for the fact table for loading the delta data 
DECLARE fct_last_laoded_date DATE DEFAULT DATE('1970-01-01');
SET fct_last_laoded_date=(SELECT max(load_date) FROM `new-globe-419310.de_assignment.log_table` WHERE table_name='fact_pupil_attendance');

#for the inital run when there is no log in the log table
if fct_last_laoded_date IS NULL
then
set fct_last_laoded_date='1970-01-01';
end if;


#fecth only the delta records based on the last loaded date in the log table for fact tables
CREATE TEMP TABLE temp_pupil AS(
SELECT
    * 
FROM
   `new-globe-419310.de_assignment.temp_pupil_data`
    WHERE SnapshotDate > fct_last_laoded_date
);

CREATE TEMP TABLE temp_pupil_attdendance AS(
SELECT
    * 
FROM
  `new-globe-419310.de_assignment.temp_pupil_attdendance`
    WHERE date > fct_last_laoded_date
);


###########use merge statement to identity delta records and load to fact table
MERGE `new-globe-419310.de_assignment.fact_pupil_attendance` as tgt
USING
(
    SELECT 
       PupilID AS pupil_id ,
      SnapshotDate AS snapshot_date ,
    GradeName AS grade_name  ,
    AcademyName AS academy_name ,
    stream ,
    attendance ,
    status ,
    CURRENT_DATE() as load_date 
    FROM 
    (
        SELECT 
         temp_pupil.PupilID ,
        temp_pupil.SnapshotDate ,
        LOWER(REGEXP_REPLACE(temp_pupil.GradeName,' ',''))  as  GradeName,
        temp_pupil.AcademyName ,
        UPPER(temp_pupil.stream) as stream ,
        temp_pupil_attdendance.attendance ,
        temp_pupil.Status ,
        FROM 
        temp_pupil 
        LEFT JOIN
        temp_pupil_attdendance
        on
        (
        temp_pupil.PupilID = temp_pupil_attdendance.PupilID
        and 
         temp_pupil.SnapshotDate = temp_pupil_attdendance.date
        )
        
    )
    
) as src
on 
(
     src.pupil_id = tgt.pupil_id
      and  src.snapshot_date =tgt.snapshot_date
       and  src.grade_name = tgt.grade_name
       and  src.academy_name = tgt.academy_name
       and  src.stream = tgt.stream
)

WHEN MATCHED THEN
  UPDATE SET 
        tgt.pupil_id = src.pupil_id,
        tgt.snapshot_date =src.snapshot_date,
        tgt.grade_name = src.grade_name,
        tgt.academy_name = src.academy_name,
        tgt.stream = src.stream

WHEN NOT MATCHED THEN
  INSERT (pupil_id, snapshot_date,   grade_name ,academy_name,stream,attendance,status, load_date) 
VALUES(src.pupil_id, src.snapshot_date,   src.grade_name ,src.academy_name,src.stream,src.attendance,src.status, src.load_date)
;

##########################add and entry to log table after the fact table loading #####################################
INSERT INTO `new-globe-419310.de_assignment.log_table` values ("fact_pupil_attendance",CURRENT_DATE());


END;