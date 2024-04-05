CREATE OR REPLACE PROCEDURE `new-globe-419310.de_assignment.load_dim_tables`()
BEGIN

# check for the log table for the last loaded date for the dim tables for loading the delta data 
DECLARE fct_last_laoded_date DATE DEFAULT DATE('1970-01-01');
SET fct_last_laoded_date=(SELECT max(load_date) FROM `new-globe-419310.de_assignment.log_table` WHERE table_name='dim_table_loads');

if fct_last_laoded_date IS NULL
then
set fct_last_laoded_date='1970-01-01';
end if;

#fecth only the delta records based on the last loaded date in the log table for dim tables
CREATE TEMP TABLE temp_pupil AS(
SELECT
    * 
FROM
   `new-globe-419310.de_assignment.temp_pupil_data`
    WHERE SnapshotDate > fct_last_laoded_date
    QUALIFY ROW_NUMBER() OVER(PARTITION BY SnapshotDate,PupilID,AcademyName,GradeId,Stream order by SnapshotDate desc)=1
);

##########################load dim pupil table #####################################
MERGE `new-globe-419310.de_assignment.dim_pupil` as tgt
USING
(
    SELECT 

      PupilID AS pupil_id ,
      FirstName AS first_name ,
      MiddleName AS middle_name ,
      LastName AS last_name ,
      'defaultmail@gmail.com' as mail_id ,
      cast('2000-01-01' as date) as dob ,
      CURRENT_DATE() as load_date
    FROM 
        temp_pupil 
    WHERE true
    qualify row_number() over(partition by PupilID order by SnapshotDate desc)=1

    )
    as src
on 
(
     src.pupil_id = tgt.pupil_id
)

WHEN MATCHED THEN
  UPDATE SET 
        tgt.pupil_id = src.pupil_id,
        tgt.first_name =src.first_name,
        tgt.middle_name = src.middle_name,
        tgt.last_name = src.last_name,
        tgt.mail_id = src.mail_id,
        tgt.dob = src.dob

WHEN NOT MATCHED THEN
  INSERT ( pupil_id ,
    first_name ,
    middle_name ,
    last_name ,
    mail_id ,
    dob ,
    load_date) 
VALUES(  src.pupil_id ,
     src.first_name ,
     src.middle_name ,
     src.last_name ,
     src.mail_id ,
     src.dob ,
     src.load_date)
;


##########################load dim grade table #####################################

MERGE `new-globe-419310.de_assignment.dim_grade` as tgt
USING
(
    SELECT 

      GradeId AS grade_id ,
      LOWER(REGEXP_REPLACE(GradeName,' ',''))AS grade_name ,
      CURRENT_DATE() as load_date
    FROM 
        temp_pupil 
    WHERE true
    qualify row_number() over(partition by grade_id order by SnapshotDate desc)=1

    )
    as src
on 
(
     src.grade_id = tgt.grade_id
)

WHEN MATCHED THEN
  UPDATE SET 
        tgt.grade_name = src.grade_name
    
WHEN NOT MATCHED THEN
  INSERT ( grade_id ,
    grade_name ,
    load_date) 
VALUES(  src.grade_id ,
     src.grade_name ,
     src.load_date)
;


##########################load dim academy table #####################################
TRUNCATE TABLE `new-globe-419310.de_assignment.dim_academy`;

INSERT INTO `new-globe-419310.de_assignment.dim_academy`(
    academy_id ,
    academy_name ,
    academy_phone_number ,
    academy_address ,
    load_date
)
SELECT  
    ROW_NUMBER() OVER(ORDER BY AcademyName ) AS academy_id ,
    AcademyName as academy_name,
     cast("+31617178465" as int64) as  academy_phone_number,
    "Amsterdam naritaweg 51" as academy_address,
    CURRENT_DATE() as load_date
FROM temp_pupil
 WHERE TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY AcademyName)=1;

##########################load dim date table #####################################

TRUNCATE TABLE `new-globe-419310.de_assignment.dim_date`;
INSERT INTO  `new-globe-419310.de_assignment.dim_date` (
    snapshot_date	 ,
    year ,
    month ,
    quarter ,
    load_date 
)
SELECT DISTINCT 
       SnapshotDate as snapshot_date	 ,
        EXTRACT(YEAR FROM SnapshotDate	 ) as year ,
        EXTRACT(MONTH FROM SnapshotDate) as month ,
        cast(FORMAT_DATE('%Q', SnapshotDate	) as int64) as quarter ,
        CURRENT_DATE() as load_date
FROM temp_pupil;


##########################add and entry to log table after the dim tables loading #####################################
INSERT INTO `new-globe-419310.de_assignment.log_table` values ("dim_table_loads",CURRENT_DATE());

END;