impala-shell -i HOSTRUNNINGIMPALAD:21000 <<eoj
create table sample_07p stored as parquet as select * from sample_07;
select * from dropme limit 5;
eoj
