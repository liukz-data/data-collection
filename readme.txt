这个工程的代码属于 设计院 吴博士的 数据采集与共享 项目中的两个模块。
这两个模块由 张岩 负责开发

1. out-seqfile模块：
功能：
  MRO解析结果（一个基站一个小时的数据）以sequence file形式写入hdfs。

运行方式：
该模块可以由上游的MRO解析程序进行调用，要把该模块封装为可被调用的代码。

输入参数：
	Hive path（hdfs:// host:port/com.com.mro/sf/city/date/hour）。地市为地市名全拼，日期格式yyyymmdd，小时取值0~23。
	Map(MRO key, MRO Sc, MRO Nc, MRO 2nd, Mro 3rd)

参照内容：
    Map中各部分成员的字段、类型

输出：成功/失败状态。并且可以返回异常信息

2. merge-seqfile模块：
功能：
sequence file汇总（一个地市一个小时的数据）汇聚为一个文件，存入以“地市_日期_小时”为分区的hive表中。
此程序封装为shell脚本可调用模式。任务执行状态输出至pg数据库。Seq file汇聚入hive表后，删除相应seq file。

Hive表名：mro_dt_rec。默认文件位置 hdfs://host:port/com.com.mro/mro_dt_rec。

运行方式：
  Shell脚本调用

  Shell脚本调用参数：
  	字段配置文件路径文件名。默认hdfs:// host:port/com.com.mro/property/mro_field.properties。
      此配置文件内容与工作项1中的List(字段，类型)保持一致。陈宏吉定义供张岩使用
  	日志输出接口信息文件路径文件名。默认hdfs:// host:port/com.com.mro/property/mro_task_log.properties。
      配置文件内容包括pg数据库接口信息。
      日志包括：时间戳、“地市+日期+小时”信息、任务执行状态，以拼接字符串形式，输出至pg数据库。
      数据库接口宏吉定义供张岩使用。

Shell脚本启动是通过定时查询解析完成状态文件内容。

3.generate-schemacls 模块：
功能：
   读取json格式的MRO数据日志字段信息，负责序列化和反序列化数据(实现Writable接口)
   根据此模块提供的FieldsMask类的allFields和allFieldsMask常量，
   可根据记录日志字段配置的json文件，out-seqfile模块可以动态地序列化日志数据；
   merge-seqfile可以动态的创建Hive表，以及合并数据时SparkSQL使用的schema类


4. 该工程内容结构介绍
1）datacollection/conf的代码：
配置文件，包括程序的配置文件 mromerge-conf.properties，以及测试时使用的CDH集群的 *-site.xml

2）datacollection/data：
测试用例数据、日志字段信息的json文件

3）datacollection/merge-seqfile/script的代码：
启动job用的shell代码: 创建表、合并数据
建表用的sql

4）datacollection/partial-fields：
只有部分日志字段需要输出时的测试用数据、json、建表SQL

