/************************************************************
** This code loads the cerner dim files into hive cluster
*************************************************************/
CREATE DATABASE IF NOT EXISTS cerner;

use cerner;
set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


------------------------------------
--  diagnosis table creation
------------------------------------
DROP TABLE IF EXISTS diagnosis_txt PURGE;
CREATE TABLE diagnosis_txt (
        encounter_id string,
        diagnosis_id string,
        diagnosis_priority string,
        diagnosis_type_id string,
        present_on_admit_id string,
        third_party_ind string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION
  'hdfs://namenode:8020/da-data/public/diagnosis/txt'
TBLPROPERTIES(
    'external.table.purge'='true'
);

DROP TABLE IF EXISTS diagnosis PURGE;
CREATE EXTERNAL TABLE diagnosis (
        encounter_id string,
        diagnosis_id string,
        diagnosis_priority string,
        diagnosis_type_id string,
        present_on_admit_id string,
        third_party_ind string )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS ORC
LOCATION
  'hdfs://namenode:8020/da-data/public/diagnosis/orc'
TBLPROPERTIES(
    "orc.compress"="SNAPPY",
    "external.table.purge"="true"
);


alter table diagnosis_txt set tblproperties('EXTERNAL'='FALSE');
TRUNCATE TABLE diagnosis_txt;
alter table diagnosis_txt set tblproperties('EXTERNAL'='TRUE');

alter table diagnosis set tblproperties('EXTERNAL'='FALSE');
TRUNCATE TABLE diagnosis;
alter table diagnosis set tblproperties('EXTERNAL'='TRUE');

LOAD DATA LOCAL INPATH '/opt/data/input/_codes/HF_F_Diagnosis_2008_sample.dat' INTO Table diagnosis_txt;
INSERT OVERWRITE TABLE diagnosis SELECT * FROM diagnosis_txt;

