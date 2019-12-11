#!/usr/bin/env bash

# 将HDFS上的属于同一地市、同一日期、同一时点的SequenceFile的多个数据文件合并到最少一个文件，
# 导入Hive ORC表的一个分区中
# author:      刘科志
# create_time: 2019/11/25
# comment:  mro_process/bin/ 文件夹下脚本调度
# parameters:
#      无
# 运行命令：
#      sh merge-data-start-line.sh
###################################################################################
export LANG=en_US.UTF-8
deal_date=`date +'%Y%m%d'` # deal_date 为当前日期，即运行此shell的日期
# 获取当前shell所在的目录
cd `dirname $0`
currpath=`pwd`
# 配置文件mromerge-conf.properties路径
mromerge_conf=${currpath}/../conf/mromerge-conf-local.properties
if [[ $# -gt 1 ]]
then
  echo "使用方法：merge-data.sh [配置文件mromerge-conf.properties的路径]"
elif [[ $# -eq 1 ]]
then
  mromerge_conf=$1
fi

source ${mromerge_conf}
# 获取 hive_user 和 hive_keytab
#hive_user=`awk -F= '{if($1=="hive_user") print $2}' ${mromerge_conf}`
#hive_keytab=`awk -F= '{if($1=="hive_keytab") print $2}' ${mromerge_conf}`
# 运行 merge-data.sh 产生日志的路径
if [[ -z ${log_dir} ]]; then
  log_dir=${currpath}/../logs
fi
if [[ ! -d ${log_dir} ]]; then
  mkdir ${log_dir}
fi
log_file=${log_dir}/merge-data_${deal_date}.log
#########################################################################################################
echo "------------- `date +'%F %T'` 开始这一时间的合并数据程序运行 ------------------" >> ${log_file}
echo "`date +'%F %T'` [INFO] 获取Kerberos Hive principal" >>${log_file}
kinit -kt ${hive_keytab} ${hive_user} 2>>${log_file}
if [[ $? -ne 0 ]]
then
  echo "`date +'%F %T'` [FAILED] 获取Kerberos Hive principal失败，程序异常终止" >> ${log_file}
  echo "------------- `date +'%F %T'` 合并数据程序终止退出 ------------------" >> ${log_file}
  exit 1
fi
#########################################################################################################
# 1. 查询 Yarn 中 MergeSeqFiles 的application是否正在运行，是则正常退出
echo "------------- `date +'%F %T'` 查询 Yarn 中 MergeSeqFiles 的application是否正在运行 ------------------" >> ${log_file}
isRunning=`yarn application -appStates NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING --list | grep "MergeSeqFiles" 2>> ${log_file}`
if [[ -n ${isRunning} ]]
then
  appID=`echo ${isRunning} | cut -d" " -f1` 2>> ${log_file}
  echo "------------- `date +'%F %T'` 已经有合并数据的程序进程正在运行，ApplicationID为${appID}，本次运行终止 ------------------" >> ${log_file}
  exit 0
else
  echo "------------- `date +'%F %T'` Yarn 中没有 MergeSeqFiles 的application在运行 ------------------" >> ${log_file}
fi

# 3. 开始运行 com.merge.MergeSeqFiles 合并数据
echo "------------- `date +'%F %T'` 合并数据，开始 ------------------" >> ${log_file}
log_level=`awk -F= '{ if($1=="log_level") print $2 }' ${mromerge_conf}`
			 
spark-submit \
             --principal ${hive_user} --keytab ${hive_keytab} \
             --master yarn --deploy-mode client \
             --num-executors 4 \
             --conf spark.executor.memory=15g \
             --conf spark.executor.cores=32 \
             --conf spark.dynamicAllocation.enabled=false \
             --conf spark.yarn.executor.memoryOverhead=1g \
             --conf spark.network.timeout=300s \
             --queue root.dev \
             --conf spark.blacklist.enabled="false" \
             --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseG1GC -XX:ParallelGCThreads=16 -XX:ConcGCThreads=10 -XX:InitiatingHeapOccupancyPercent=40" \
             --jars ../libs/generate-schemacls-1.0.jar,../libs/postgresql-42.2.5.jar \
             --class com.merge.MergeSeqFilesCoalesceSerialLine \
             ../libs/merge-seqfile.jar ${mromerge_conf_hdfs} ${log_level} 1>> ${log_file} 2>> ${log_file}

echo "------------- `date +'%F %T'` 合并数据，结束 ------------------" >> ${log_file}
