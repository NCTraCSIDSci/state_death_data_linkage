-- Databricks notebook source

/*
This file contains the finalized version of the linkage code that has been employed in a scheduled pipeline. 
This code takes patient data from an EHR table and state death data files and links these records. 
Positive matches are written to an output table.

Authors:
Peter J. Leese and John P. Powers

Copyright and licensing:
© 2024, The University of North Carolina at Chapel Hill. Permission is granted to use in accordance with the MIT license. 

The code is licensed under the open-source MIT license.
*/

-- MAGIC %md
-- MAGIC ## Data Loading

-- COMMAND ----------

-- load patient data table
CREATE OR REPLACE TEMPORARY VIEW patient
USING parquet
OPTIONS (path [path]);

-- load state death data tables
CREATE OR REPLACE TEMPORARY VIEW death_2010
USING parquet
OPTIONS (path [path]);

CREATE OR REPLACE TEMPORARY VIEW death_2014
USING parquet
OPTIONS (path [path]);

CREATE OR REPLACE TEMPORARY VIEW death_2020
USING parquet
OPTIONS (path [path]);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Cleaning

-- COMMAND ----------

-- parse 2010 death file, clean and standardize columns

drop view if exists clean_2010;

create temporary view clean_2010 as
select distinct
filename,
'Y' as state_final,
to_date(death_date, 'MMddyyyy') as death_date,
case when sex=1 then 'M'
     when sex=2 then 'F' end as sex,
to_date(date_of_birth, 'MMddyyyy') as dob,
split(decedents_firstname,' ')[0] as fname,
substring(decedents_middlename,1,1) as mname,
split(decedents_lastname,' ')[0] as lname,
case when social_security_number='000000000' then null
     when social_security_number is null then null
     else social_security_number end as ssn,
case when split(residence_address, ' ')[0] rlike '^[0-9]*$' then split(residence_address, ' ')[0]
     else null end as house_number,
split(residence_address,' ')[1] as street,
residence_zip_code as zipcode,
manual_underlying_causeofdeathcode as underlying_cause_of_death,
first_mention_cause_of_death as cod1,
second_mention_cause_of_death as cod2,
third_mention_cause_of_death as cod3,
fourth_mention_cause_of_death as cod4,
fifth_mention_cause_of_death as cod5,
sixth_mention_cause_of_death as cod6,
seventh_mention_cause_of_death as cod7,
eighth_mention_cause_of_death as cod8,
ninth_mention_cause_of_death as cod9,
tenth_mention_cause_of_death as cod10,
eleventh_mention_cause_of_death as cod11,
twelfth_mention_cause_of_death as cod12,
thirteenth_mention_cause_of_death as cod13,
fourteenth_mention_cause_of_death as cod14,
fifteenth_mention_cause_of_death as cod15,
sixteenth_mention_cause_of_death as cod16,
seventeenth_mention_cause_of_death as cod17,
eighteenth_mention_cause_of_death as cod18,
nineteenth_mention_cause_of_death as cod19,
twentieth_mention_cause_of_death as cod20
from death_2010


-- COMMAND ----------

-- parse 2014 death file, clean and standardize columns

drop view if exists clean_2014;

create temporary view clean_2014 as
select distinct
filename,
'Y' as state_final,
concat(DOD_YR, '-', DOD_MO, '-', DOD_DY) as  death_date,
sex,
 concat(DOB_YR, '-', DOB_MO, '-', DOB_DY) as dob,
split(GNAME,' ')[0] as fname,
substring(MNAME,1,1) as mname,
split(LNAME,' ')[0] as lname,
case when ssn='000000000' then null
     when ssn is null then null
     else ssn end as ssn,
addrnum as house_number,
split(addrname,' ')[0] as street,
zipcode,
acmecod as underlying_cause_of_death,
cod1,
cod2,
cod3,
cod4,
cod5,
cod6,
cod7,
cod8,
cod9,
cod10,
cod11,
cod12,
cod13,
cod14,
cod15,
cod16,
cod17,
cod18,
cod19,
cod20
from death_2014

-- COMMAND ----------

-- parse 2020 death file, clean and standardize columns

drop view if exists clean_2020;

create temporary view clean_2020 as
select distinct
filename,
case when substr(filename,1,5)='FINAL' then 'Y'
     else 'N' end as state_final,
concat(DOD_YR, '-', DOD_MO, '-', DOD_DY) as  death_date,
sex,
 concat(DOB_YR, '-', DOB_MO, '-', DOB_DY) as dob,
split(GNAME,' ')[0] as fname,
substring(MNAME,1,1) as mname,
split(LNAME,' ')[0] as lname,
case when ssn='000000000' then null
     when ssn is null then null
     else ssn end as ssn,
stnum_d as house_number,
split(stname_r,' ')[0] as street,
substring(zip9_d,1,5) as zipcode,
acme_uc as underlying_cause_of_death,
split(rac,' ')[0] as cod1,
split(rac,' ')[1] as cod2,
split(rac,' ')[2] as cod3,
split(rac,' ')[3] as cod4,
split(rac,' ')[4] as cod5,
split(rac,' ')[5] as cod6,
split(rac,' ')[6] as cod7,
split(rac,' ')[7] as cod8,
split(rac,' ')[8] as cod9,
split(rac,' ')[9] as cod10,
split(rac,' ')[10] as cod11,
split(rac,' ')[11] as cod12,
split(rac,' ')[12] as cod13,
split(rac,' ')[13] as cod14,
split(rac,' ')[14] as cod15,
split(rac,' ')[15] as cod16,
split(rac,' ')[16] as cod17,
split(rac,' ')[17] as cod18,
split(rac,' ')[18] as cod19,
split(rac,' ')[19] as cod20
from death_2020

-- COMMAND ----------

-- create new view of all cleaned death files unioned together

drop view if exists all_death;

create temporary view all_death as
select * from clean_2010
 union
select * from clean_2014
 union
select * from clean_2020

-- COMMAND ----------

-- clean and standardize patient data table

drop view if exists patient_clean;

create temporary view patient_clean as
select distinct mrn,
                id,
                last_name,
                first_name,
                split(upper(last_name),' ')[0] as ehr_lname,
                split(upper(first_name),' ')[0] as ehr_fname,
                substring(middle_name,1,1) as ehr_mname,
                case when sex=1 then 'F'
                     when sex=2 then 'M'
                     else '' end as ehr_sex,
                birth_date,
                to_date(birth_date) as ehr_dob,
                add_line_1,
                case when split(add_line_1, ' ')[0] rlike '^[0-9]*$' then split(add_line_1, ' ')[0]
                     else null end as ehr_house_number,
                upper(split(add_line_1,' ')[1]) as ehr_street,
                substring(zip,1,5) as ehr_zip,
                ssn,
                case when regexp_replace(ssn,'-','') in ('000000000','999999999','777777777','444444444','888888888','222222222','999990999','000009999','111111111','123456789','555555555','444556666','099999999','999999998','999990099','333333333','999999990','999999991','000000001','666666666','999991111') then null else regexp_replace(ssn,'-','') end as ehr_ssn,
                death_date as ehr_death_date,
                max_contact_dt,
                enc_type
from patient
                

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Linkage

-- COMMAND ----------

--perform first linkage approach [SSN is same, first name is a little different]

drop view if exists total_join_14;

create temporary view total_join_14 as
select z.*, (lname_score + fname_score + mname_score + dob_score + ssn_score + zip_score + house_score) as wms from (

select a.pat_mrn_id, a.pat_id, b.state_final, b.death_date, b.underlying_cause_of_death,
      b.cod1, b.cod2, b.cod3, b.cod4, b.cod5, b.cod6, b.cod7, b.cod8, b.cod9, b.cod10,
      b.cod11, b.cod12, b.cod13, b.cod14, b.cod15, b.cod16, b.cod17, b.cod18, b.cod19, b.cod20,
      a.ehr_lname, b.lname, levenshtein(a.ehr_lname, b.lname) as lname_dist,
      a.ehr_fname, b.fname, levenshtein(a.ehr_fname, b.fname) as fname_dist,
      a.ehr_mname, b.mname as mname,
      a.ehr_sex, b.sex,
      a.ehr_dob, b.dob, levenshtein(a.ehr_dob, b.dob) as dob_dist,
      a.ehr_ssn, b.ssn as ssn, levenshtein(a.ehr_ssn, b.ssn) as ssn_dist,
      a.ehr_zip, b.zipcode, levenshtein(a.ehr_zip, b.zipcode) as zip_dist,
      a.ehr_house_number, b.house_number, levenshtein(a.ehr_house_number, b.house_number) as house_dist,
      a.ehr_street, b.street,
              
      case when isnull(a.ehr_lname) then 0
            when a.ehr_lname = b.lname then 10
            when levenshtein(a.ehr_lname, b.lname) between 4 and 6 then 3
            when levenshtein(a.ehr_lname, b.lname) between 1 and 3 then 7
           else -5 end as lname_score,
           
       case when isnull(a.ehr_fname) then 0
            when a.ehr_fname = b.fname then 10
            when levenshtein(a.ehr_fname, b.fname) between 4 and 6 then 3
            when levenshtein(a.ehr_fname, b.fname) between 1 and 3 then 7
           else -5 end as fname_score,
           
         case when isnull(a.ehr_mname) or isnull(b.mname) then 0
              when a.ehr_mname = b.mname then 5
              else -5 end as mname_score,
              
          case when isnull(a.ehr_dob) or isnull(b.dob) then 0
               when a.ehr_dob = b.dob then 15
               when levenshtein(a.ehr_dob, b.dob) between 1 and 2 then 9
               when levenshtein(a.ehr_dob, b.dob) between 3 and 4 then 3
               else -5 end as dob_score,
               
           case when isnull(a.ehr_ssn) or isnull(b.ssn) then 0
                when a.ehr_ssn = b.ssn then 25
                when levenshtein(a.ehr_ssn, b.ssn) = 2 then 5
                when levenshtein(a.ehr_ssn, b.ssn) = 1 then 10
                else -5 end as ssn_score,
                
            case when isnull(a.ehr_zip) or isnull(b.zipcode) then 0
                 when a.ehr_zip = b.zipcode then 5
                 else 0 end as zip_score,
                 
             case when isnull(a.ehr_house_number) or isnull(b.house_number) then 0
                 when a.ehr_house_number = b.house_number then 10
                 else 0 end as house_score
           
      
      
from patient_clean a INNER JOIN all_death b on a.ehr_ssn = b.ssn
                            and a.ehr_lname not like ('INITIAL%')
                            and levenshtein(a.ehr_fname, b.fname) >2
) z

-- COMMAND ----------

--perform second linkage approach (sex is same, last 4 of SSN are same, last name is different)

drop view if exists total_join_17;

create temporary view total_join_17 as
select z.*, (lname_score + fname_score + mname_score + dob_score + ssn_score + zip_score + house_score) as wms from (

select a.pat_mrn_id, a.pat_id, b.state_final, b.death_date, b.underlying_cause_of_death,
      b.cod1, b.cod2, b.cod3, b.cod4, b.cod5, b.cod6, b.cod7, b.cod8, b.cod9, b.cod10,
      b.cod11, b.cod12, b.cod13, b.cod14, b.cod15, b.cod16, b.cod17, b.cod18, b.cod19, b.cod20,
      a.ehr_lname, b.lname, levenshtein(a.ehr_lname, b.lname) as lname_dist,
      a.ehr_fname, b.fname, levenshtein(a.ehr_fname, b.fname) as fname_dist,
      a.ehr_mname, b.mname as mname,
      a.ehr_sex, b.sex,
      a.ehr_dob, b.dob, levenshtein(a.ehr_dob, b.dob) as dob_dist,
      a.ehr_ssn, b.ssn as ssn, levenshtein(a.ehr_ssn, b.ssn) as ssn_dist,
      a.ehr_zip, b.zipcode, levenshtein(a.ehr_zip, b.zipcode) as zip_dist,
      a.ehr_house_number, b.house_number, levenshtein(a.ehr_house_number, b.house_number) as house_dist,
      a.ehr_street, b.street,
              
      case when isnull(a.ehr_lname) then 0
            when a.ehr_lname = b.lname then 10
            when levenshtein(a.ehr_lname, b.lname) between 4 and 6 then 3
            when levenshtein(a.ehr_lname, b.lname) between 1 and 3 then 7
           else -5 end as lname_score,
           
       case when isnull(a.ehr_fname) then 0
            when a.ehr_fname = b.fname then 10
            when levenshtein(a.ehr_fname, b.fname) between 4 and 6 then 3
            when levenshtein(a.ehr_fname, b.fname) between 1 and 3 then 7
           else -5 end as fname_score,
           
         case when isnull(a.ehr_mname) or isnull(b.mname) then 0
              when a.ehr_mname = b.mname then 5
              else -5 end as mname_score,
              
          case when isnull(a.ehr_dob) or isnull(b.dob) then 0
               when a.ehr_dob = b.dob then 15
               when levenshtein(a.ehr_dob, b.dob) between 1 and 2 then 9
               when levenshtein(a.ehr_dob, b.dob) between 3 and 4 then 3
               else -5 end as dob_score,
               
           case when isnull(a.ehr_ssn) or isnull(b.ssn) then 0
                when a.ehr_ssn = b.ssn then 25
                when levenshtein(a.ehr_ssn, b.ssn) = 2 then 5
                when levenshtein(a.ehr_ssn, b.ssn) = 1 then 10
                else -5 end as ssn_score,
                
            case when isnull(a.ehr_zip) or isnull(b.zipcode) then 0
                 when a.ehr_zip = b.zipcode then 5
                 else 0 end as zip_score,
                 
             case when isnull(a.ehr_house_number) or isnull(b.house_number) then 0
                 when a.ehr_house_number = b.house_number then 10
                 else 0 end as house_score
           
      
      
from patient_clean a INNER JOIN all_death b on a.ehr_sex = b.sex
                            and substr(a.ehr_ssn,6,9) = substr(b.ssn,6,9)
                            and substr(a.ehr_lname,1,4) != substr(b.lname,1,4)
) z

-- COMMAND ----------

--perform third linkage approach (sex is same, last 4 of SSN are same, last name is very similar)

drop view if exists total_join_18;

create temporary view total_join_18 as
select z.*, (lname_score + fname_score + mname_score + dob_score + ssn_score + zip_score + house_score) as wms from (

select a.pat_mrn_id, a.pat_id, b.state_final, b.death_date, b.underlying_cause_of_death,
      b.cod1, b.cod2, b.cod3, b.cod4, b.cod5, b.cod6, b.cod7, b.cod8, b.cod9, b.cod10,
      b.cod11, b.cod12, b.cod13, b.cod14, b.cod15, b.cod16, b.cod17, b.cod18, b.cod19, b.cod20,
      a.ehr_lname, b.lname, levenshtein(a.ehr_lname, b.lname) as lname_dist,
      a.ehr_fname, b.fname, levenshtein(a.ehr_fname, b.fname) as fname_dist,
      a.ehr_mname, b.mname as mname,
      a.ehr_sex, b.sex,
      a.ehr_dob, b.dob, levenshtein(a.ehr_dob, b.dob) as dob_dist,
      a.ehr_ssn, b.ssn as ssn, levenshtein(a.ehr_ssn, b.ssn) as ssn_dist,
      a.ehr_zip, b.zipcode, levenshtein(a.ehr_zip, b.zipcode) as zip_dist,
      a.ehr_house_number, b.house_number, levenshtein(a.ehr_house_number, b.house_number) as house_dist,
      a.ehr_street, b.street,
              
      case when isnull(a.ehr_lname) then 0
            when a.ehr_lname = b.lname then 10
            when levenshtein(a.ehr_lname, b.lname) between 4 and 6 then 3
            when levenshtein(a.ehr_lname, b.lname) between 1 and 3 then 7
           else -5 end as lname_score,
           
       case when isnull(a.ehr_fname) then 0
            when a.ehr_fname = b.fname then 10
            when levenshtein(a.ehr_fname, b.fname) between 4 and 6 then 3
            when levenshtein(a.ehr_fname, b.fname) between 1 and 3 then 7
           else -5 end as fname_score,
           
         case when isnull(a.ehr_mname) or isnull(b.mname) then 0
              when a.ehr_mname = b.mname then 5
              else -5 end as mname_score,
              
          case when isnull(a.ehr_dob) or isnull(b.dob) then 0
               when a.ehr_dob = b.dob then 15
               when levenshtein(a.ehr_dob, b.dob) between 1 and 2 then 9
               when levenshtein(a.ehr_dob, b.dob) between 3 and 4 then 3
               else -5 end as dob_score,
               
           case when isnull(a.ehr_ssn) or isnull(b.ssn) then 0
                when a.ehr_ssn = b.ssn then 25
                when levenshtein(a.ehr_ssn, b.ssn) = 2 then 5
                when levenshtein(a.ehr_ssn, b.ssn) = 1 then 10
                else -5 end as ssn_score,
                
            case when isnull(a.ehr_zip) or isnull(b.zipcode) then 0
                 when a.ehr_zip = b.zipcode then 5
                 else 0 end as zip_score,
                 
             case when isnull(a.ehr_house_number) or isnull(b.house_number) then 0
                 when a.ehr_house_number = b.house_number then 10
                 else 0 end as house_score
           
      
      
from patient_clean a INNER JOIN all_death b on a.ehr_sex = b.sex
                            and substr(a.ehr_ssn,6,9) = substr(b.ssn,6,9)
                            and substr(a.ehr_lname,1,4) = substr(b.lname,1,4)
) z

-- COMMAND ----------

--union all linkage results together
--and drop matches with weighted match scores < 30
--we consider these negative matches, so they don't need to proceed to ML classifier

drop view if exists total_linkage_results;

create temporary view total_linkage_results as
select * from total_join_14 where wms >= 30
 union
select * from total_join_17 where wms >= 30
 union
select * from total_join_18 where wms >= 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ML Classifier

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Load Code

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # import code
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC import joblib
-- MAGIC
-- MAGIC # PySpark
-- MAGIC import pyspark.sql.functions as F
-- MAGIC from pyspark.sql.types import IntegerType
-- MAGIC from pyspark.sql import Window
-- MAGIC
-- MAGIC # modeling
-- MAGIC from sklearn.pipeline import make_pipeline
-- MAGIC from sklearn.ensemble import HistGradientBoostingClassifier

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Deploy Model

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # unpickle saved, trained model
-- MAGIC pipeline = joblib.load('[path]/death_linkage_classifier.joblib')
-- MAGIC
-- MAGIC # broadcast model to all nodes
-- MAGIC model = sc.broadcast(pipeline)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # convert linkage results to spark df
-- MAGIC dataset = spark.sql('SELECT * FROM total_linkage_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # define function to run model's predict method on spark df
-- MAGIC @F.pandas_udf(IntegerType())
-- MAGIC def predict(*cols: pd.Series) -> pd.Series:
-- MAGIC         
-- MAGIC   # columns are passed as a tuple of pandas series.
-- MAGIC   # combine into a pandas df
-- MAGIC   X = pd.concat(cols, axis=1)
-- MAGIC
-- MAGIC   # make predictions
-- MAGIC   predictions = model.value.predict(X)
-- MAGIC
-- MAGIC   # return pandas series of predictions
-- MAGIC   return pd.Series(predictions)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC feature_cols = ['lname_dist', 'fname_dist', 'dob_dist', 'ssn_dist', 'zip_dist', 'house_dist', 
-- MAGIC                   'lname_score', 'fname_score', 'mname_score', 'dob_score', 'ssn_score', 'zip_score', 
-- MAGIC                   'house_score']
-- MAGIC
-- MAGIC # run the model on the dataset
-- MAGIC dataset = dataset.withColumn('match', predict(*feature_cols))
-- MAGIC
-- MAGIC # filter results to only positive matches
-- MAGIC matches = dataset.where(F.col('match') == 1)
-- MAGIC matches = matches.drop('match')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## in rare cases, the above pipeline yields multiple positive matches for a single patient
-- MAGIC
-- MAGIC # when wms differs, only retain the match with highest wms
-- MAGIC # manual inspection revealed the difference in wms was often substantial 
-- MAGIC # and the higher-wms match was clearly more likely the true match
-- MAGIC w = Window.partitionBy('mrn')
-- MAGIC matches = (
-- MAGIC   matches
-- MAGIC   .withColumn('max_wms', F.max('wms').over(w))
-- MAGIC   .where(F.col('wms') == F.col('max_wms'))
-- MAGIC   .drop('max_wms')
-- MAGIC )
-- MAGIC
-- MAGIC # of the remaining multiple match cases, these generally seem to result from 2-3 death records for
-- MAGIC # what appears to be the same individual (resulting in wms ties)
-- MAGIC # but usually only one of the records includes cod data
-- MAGIC # this code uses an indirect method to drop the match(es) without cod data when another match has cod data
-- MAGIC matches = matches.withColumn('cod_len', F.length(F.col('underlying_cause_of_death')))
-- MAGIC matches = (
-- MAGIC   matches
-- MAGIC   .withColumn('sum_cod_len', F.sum('cod_len').over(w))
-- MAGIC   .where((F.col('sum_cod_len') > 5) | (F.col('cod_len') == F.col('sum_cod_len')))
-- MAGIC   .drop('cod_len', 'sum_cod_len')
-- MAGIC )
-- MAGIC
-- MAGIC # any remaining multiple matches generally seem to result from slightly different data
-- MAGIC # in multiple death records for what appears to be the same individual
-- MAGIC # we cannot resolve which is the better match in these cases, so all such matches will remain in the output table

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC state_death_matches = matches
-- MAGIC
-- MAGIC spark.sql('drop table if exists state_death_matches')
-- MAGIC
-- MAGIC state_death_matches.write.saveAsTable("STATE_DEATH_MATCHES")
