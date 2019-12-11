#!/usr/bin/env bash

# 在初始化系统时，或者在修改了日志字段配置的JSON文件后，运行这个shell。
# 这个shell用于在CDH上创建MRO数据采集相关的数据库和表
# author:      张岩
# create_time: 2019/07/2
# comment:  执行方式：手动运行
# parameters:
#      无
# 运行命令：
#      sh create-table.sh
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
  echo "使用方法：create-table.sh [配置文件mromerge-conf.properties的路径]"
elif [[ $# -eq 1 ]]
then
  mromerge_conf=$1
fi

source ${mromerge_conf}

# 等待用户二次确认
echo "新建表的操作会删除CDH Hive数据库${dbname}中已存在的表${tb_name}以及表内原有的数据。输入 y 表示确定，其他表示放弃。"
read -p "确认新建表并删除原有数据吗？"
if [[ $REPLY -ne "y" ]]
then
  exit 0
fi
# 运行 merge-data.sh 产生日志的路径
if [[ -z ${log_dir} ]]; then
  log_dir=${currpath}/../log/
fi
if [[ ! -d ${log_dir} ]]; then
  mkdir ${log_dir}
fi
log_file=${log_dir}/create-table_${deal_date}.log
#########################################################################################################
echo "------------- `date +'%F %T'` 开始在CDH上创建MRO数据采集相关的数据库和表 ------------------" >> ${log_file}
if [[ -z ${log_level} ]]; then
  log_level="ERROR"
fi

spark-submit \
             --principal ${hive_user} --keytab ${hive_keytab} \
             --master yarn --deploy-mode client \
             --jars ../libs/generate-schemacls-1.0.jar \
             --class com.table.MroTable \
             ../libs/merge-seqfile.jar ${mromerge_conf_hdfs} ${log_level} 1>> ${log_file} 2>> ${log_file}
			 
echo "------------- `date +'%F %T'` 程序结束 ------------------" >> ${log_file}
