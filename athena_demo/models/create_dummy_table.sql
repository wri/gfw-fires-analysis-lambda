CREATE EXTERNAL TABLE `fires`(
  `lat` double, 
  `lon` double, 
  `fire_type` string, 
  `fire_date` date)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://gfw2-data/alerts-tsv/temp/fires-athena-test'
TBLPROPERTIES (
  'transient_lastDdlTime'='1518213275')
