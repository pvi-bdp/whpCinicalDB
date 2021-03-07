/************************************************************
** This code loads the cerner dim files into hive cluster
*************************************************************/
CREATE DATABASE IF NOT EXISTS cerner;

use cerner;
set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

------------------------------------
--  HF_D_ADMISSION_SOURCE table creation
------------------------------------
DROP TABLE IF EXISTS d_admission_source_txt PURGE;
CREATE TABLE d_admission_source_txt (
    ADMISSION_SOURCE_ID string,
    ADMISSION_SOURCE_CODE string,
    ADMISSION_SOURCE_CODE_DESC string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION
  'hdfs://namenode:8020/da-data/cerner/d_admission_source/txt'
TBLPROPERTIES(
    'external.table.purge'='true'
);

DROP TABLE IF EXISTS d_admission_source PURGE;
CREATE EXTERNAL TABLE d_admission_source (
    ADMISSION_SOURCE_ID string,
    ADMISSION_SOURCE_CODE string,
    ADMISSION_SOURCE_CODE_DESC string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS ORC
LOCATION
  'hdfs://namenode:8020/da-data/cerner/d_admission_source/orc'
TBLPROPERTIES(
    "orc.compress"="SNAPPY",
    "external.table.purge"="true"
);


alter table d_admission_source_txt set tblproperties('EXTERNAL'='FALSE');
TRUNCATE TABLE d_admission_source_txt;
alter table d_admission_source_txt set tblproperties('EXTERNAL'='TRUE');

alter table d_admission_source set tblproperties('EXTERNAL'='FALSE');
TRUNCATE TABLE d_admission_source;
alter table d_admission_source set tblproperties('EXTERNAL'='TRUE');

LOAD DATA LOCAL INPATH '/opt/data/input/cerner/HF_D_ADMISSION_SOURCE.dat' INTO Table d_admission_source_txt;
INSERT OVERWRITE TABLE d_admission_source SELECT * FROM d_admission_source_txt;

DROP TABLE IF EXISTS d_admission_source_txt PURGE;

------------------------------------
--  HF_D_ADMISSION_TYPE table creation
------------------------------------
DROP TABLE IF EXISTS d_admission_type_txt PURGE;
CREATE TABLE d_admission_type_txt (
    ADMISSION_TYPE_ID string,
    ADMISSION_TYPE_CODE string,
    ADMISSION_TYPE_CODE_DESC string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION
  'hdfs://namenode:8020/da-data/cerner/d_admission_type/txt'
TBLPROPERTIES(
    'external.table.purge'='true'
);

DROP TABLE IF EXISTS d_admission_type PURGE;
CREATE EXTERNAL TABLE d_admission_type (
    ADMISSION_TYPE_ID string,
    ADMISSION_TYPE_CODE string,
    ADMISSION_TYPE_CODE_DESC string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS ORC
LOCATION
  'hdfs://namenode:8020/da-data/cerner/d_admission_type/orc'
TBLPROPERTIES(
    "orc.compress"="SNAPPY",
    "external.table.purge"="true"
);


alter table d_admission_type_txt set tblproperties('EXTERNAL'='FALSE');
TRUNCATE TABLE d_admission_type_txt;
alter table d_admission_type_txt set tblproperties('EXTERNAL'='TRUE');

alter table d_admission_type set tblproperties('EXTERNAL'='FALSE');
TRUNCATE TABLE d_admission_type;
alter table d_admission_type set tblproperties('EXTERNAL'='TRUE');

LOAD DATA LOCAL INPATH '/opt/data/input/cerner/HF_D_ADMISSION_TYPE.dat' INTO Table d_admission_type_txt;
INSERT OVERWRITE TABLE d_admission_type SELECT * FROM d_admission_type_txt;

DROP TABLE IF EXISTS d_admission_type_txt PURGE;



























































------------------------------------
--  diagnosis dimension table creation
------------------------------------
DROP TABLE IF EXISTS d_diagnosis_txt PURGE;
CREATE TABLE d_diagnosis_txt (
        diagnosis_id string,
        diagnosis_type string,
        diagnosis_code string,
        diagnosis_description string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION
  'hdfs://namenode:8020/da-data/cerner/d_diagnosis/txt'
TBLPROPERTIES(
    'external.table.purge'='true'
);

DROP TABLE IF EXISTS d_diagnosis PURGE;
CREATE EXTERNAL TABLE d_diagnosis (
        diagnosis_id string,
        diagnosis_type string,
        diagnosis_code string,
        diagnosis_description string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS ORC
LOCATION
  'hdfs://namenode:8020/da-data/cerner/d_diagnosis/orc'
TBLPROPERTIES(
    "orc.compress"="SNAPPY",
    "external.table.purge"="true"
);


alter table d_diagnosis_txt set tblproperties('EXTERNAL'='FALSE');
TRUNCATE TABLE d_diagnosis_txt;
alter table d_diagnosis_txt set tblproperties('EXTERNAL'='TRUE');

alter table d_diagnosis set tblproperties('EXTERNAL'='FALSE');
TRUNCATE TABLE d_diagnosis;
alter table d_diagnosis set tblproperties('EXTERNAL'='TRUE');

LOAD DATA LOCAL INPATH '/opt/data/input/cerner/HF_D_DIAGNOSIS.dat' INTO Table d_diagnosis_txt;
INSERT OVERWRITE TABLE d_diagnosis SELECT * FROM d_diagnosis_txt;

