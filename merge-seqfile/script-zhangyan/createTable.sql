create database if not exists mro
comment '数据采集及共享项目Hive数据库'
--location '/mro/mro.db'  -- 卓望CDH环境下hdfs路径 /user/zhangyan/mro/mro.db
location '/user/zhangyan/mro/mro.db'
--WITH DBPROPERTIES ('creator' = 'Zhang Yan', 'date' = '2019-05-29')
;

--create external table if not exists guiyang_101450_00_orc_snappy(
create external table if not exists mro.mro_dt_rec(
ENBID int, ECI int, CGI string, MmeUeS1apId string, MmeGroupId int, MmeCode int, TimeStampField string,
LteScEarfcn int, LteScPci int, LteScRSRP int, LteSceNBRxTxTimeDiff int, LteScTadv int,
LteScRSRQ int, LteScPUSCHPRBNum int, LteScPHR int, LteScAOA int, LteScSinrUL int,
LteScRI1 int, LteScRI2 int, LteScRI4 int, LteScRI8 int, LteScBSR int, LteScPDSCHPRBNum int,
LteNcEarfcn01 int, LteNcPci01 int, LteNcRSRP01 int, LteNcRSRQ01 int, TdsNcellUarfcn01 int,
TdsCellParameterId01 int, TdsPccpchRSCP01 int, GsmNcellNcc01 int, GsmNcellBcc01 int,
GsmNcellBcch01 int, GsmNcellCarrierRSSI01 int,
LteNcEarfcn02 int, LteNcPci02 int, LteNcRSRP02 int, LteNcRSRQ02 int, TdsNcellUarfcn02 int,
TdsCellParameterId02 int, TdsPccpchRSCP02 int, GsmNcellNcc02 int, GsmNcellBcc02 int,
GsmNcellBcch02 int, GsmNcellCarrierRSSI02 int,
LteNcEarfcn03 int, LteNcPci03 int, LteNcRSRP03 int, LteNcRSRQ03 int, TdsNcellUarfcn03 int,
TdsCellParameterId03 int, TdsPccpchRSCP03 int, GsmNcellNcc03 int, GsmNcellBcc03 int,
GsmNcellBcch03 int, GsmNcellCarrierRSSI03 int,
LteNcEarfcn04 int, LteNcPci04 int, LteNcRSRP04 int, LteNcRSRQ04 int, TdsNcellUarfcn04 int,
TdsCellParameterId04 int, TdsPccpchRSCP04 int, GsmNcellNcc04 int, GsmNcellBcc04 int,
GsmNcellBcch04 int, GsmNcellCarrierRSSI04 int,
LteNcEarfcn05 int, LteNcPci05 int, LteNcRSRP05 int, LteNcRSRQ05 int, TdsNcellUarfcn05 int,
TdsCellParameterId05 int, TdsPccpchRSCP05 int, GsmNcellNcc05 int, GsmNcellBcc05 int,
GsmNcellBcch05 int, GsmNcellCarrierRSSI05 int,
LteNcEarfcn06 int, LteNcPci06 int, LteNcRSRP06 int, LteNcRSRQ06 int, TdsNcellUarfcn06 int,
TdsCellParameterId06 int, TdsPccpchRSCP06 int, GsmNcellNcc06 int, GsmNcellBcc06 int,
GsmNcellBcch06 int, GsmNcellCarrierRSSI06 int,
LteNcEarfcn07 int, LteNcPci07 int, LteNcRSRP07 int, LteNcRSRQ07 int, TdsNcellUarfcn07 int,
TdsCellParameterId07 int, TdsPccpchRSCP07 int, GsmNcellNcc07 int, GsmNcellBcc07 int,
GsmNcellBcch07 int, GsmNcellCarrierRSSI07 int,
LteNcEarfcn08 int, LteNcPci08 int, LteNcRSRP08 int, LteNcRSRQ08 int, TdsNcellUarfcn08 int,
TdsCellParameterId08 int, TdsPccpchRSCP08 int, GsmNcellNcc08 int, GsmNcellBcc08 int,
GsmNcellBcch08 int, GsmNcellCarrierRSSI08 int,
LteNcEarfcn09 int, LteNcPci09 int, LteNcRSRP09 int, LteNcRSRQ09 int, TdsNcellUarfcn09 int,
TdsCellParameterId09 int, TdsPccpchRSCP09 int, GsmNcellNcc09 int, GsmNcellBcc09 int,
GsmNcellBcch09 int, GsmNcellCarrierRSSI09 int,
LteNcEarfcn10 int, LteNcPci10 int, LteNcRSRP10 int, LteNcRSRQ10 int, TdsNcellUarfcn10 int,
TdsCellParameterId10 int, TdsPccpchRSCP10 int, GsmNcellNcc10 int, GsmNcellBcc10 int,
GsmNcellBcch10 int, GsmNcellCarrierRSSI10 int,
LteNcEarfcn11 int, LteNcPci11 int, LteNcRSRP11 int, LteNcRSRQ11 int, TdsNcellUarfcn11 int,
TdsCellParameterId11 int, TdsPccpchRSCP11 int, GsmNcellNcc11 int, GsmNcellBcc11 int,
GsmNcellBcch11 int, GsmNcellCarrierRSSI11 int,
LteNcEarfcn12 int, LteNcPci12 int, LteNcRSRP12 int, LteNcRSRQ12 int, TdsNcellUarfcn12 int,
TdsCellParameterId12 int, TdsPccpchRSCP12 int, GsmNcellNcc12 int, GsmNcellBcc12 int,
GsmNcellBcch12 int, GsmNcellCarrierRSSI12 int,
LteScPlrULQci1 int, LteScPlrULQci2 int, LteScPlrULQci3 int, LteScPlrULQci4 int, LteScPlrULQci5 int,
LteScPlrULQci6 int, LteScPlrULQci7 int, LteScPlrULQci8 int, LteScPlrULQci9 int,
LteScPlrDLQci1 int, LteScPlrDLQci2 int, LteScPlrDLQci3 int, LteScPlrDLQci4 int, LteScPlrDLQci5 int,
LteScPlrDLQci6 int, LteScPlrDLQci7 int, LteScPlrDLQci8 int, LteScPlrDLQci9 int,
LteScRIP2 int, LteScRIP7 int
)
partitioned by (city String, logdate string, hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as ORC
--location '/zhangyantest/zhangyantest.db/guiyang_101450_00_orc_snappy'
location '/user/zhangyan/mro/mro_dt_rec'
--tblproperties("skip.header.line.count"="1")
tblproperties("orc.compression"="SNAPPY")
;

--ALTER TABLE guiyang_101450_00 ADD PARTITION(city='guiyang', logdate='20171108', hour='00') LOCATION '/zhangyantest/zhangyantest.db/guiyang_101450_00/city=guiyang/logdate=20171108/hour=00';
