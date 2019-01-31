

create table partition_test_partitioned(key string, value string) partitioned by (dt string);

insert overwrite table partition_test_partitioned partition(dt=100) select * from src1;
show table extended like partition_test_partitioned;
show table extended like partition_test_partitioned partition(dt=100);
select key from partition_test_partitioned where dt=100;
select key from partition_test_partitioned;

alter table partition_test_partitioned set fileformat rcfile;
insert overwrite table partition_test_partitioned partition(dt=101) select * from src1;
show table extended like partition_test_partitioned;
show table extended like partition_test_partitioned partition(dt=100);
show table extended like partition_test_partitioned partition(dt=101);
select key from partition_test_partitioned where dt=100;
select key from partition_test_partitioned where dt=101;
select key from partition_test_partitioned;

alter table partition_test_partitioned set fileformat Sequencefile;
insert overwrite table partition_test_partitioned partition(dt=102) select * from src1;
show table extended like partition_test_partitioned;
show table extended like partition_test_partitioned partition(dt=100);
show table extended like partition_test_partitioned partition(dt=101);
show table extended like partition_test_partitioned partition(dt=102);
select key from partition_test_partitioned where dt=100;
select key from partition_test_partitioned where dt=101;
select key from partition_test_partitioned where dt=102;
select key from partition_test_partitioned;

select key from partition_test_partitioned where dt >=100 and dt <= 102;

