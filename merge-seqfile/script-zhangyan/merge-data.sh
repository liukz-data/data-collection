#!/usr/bin/env bash

# 将HDFS上的属于同一地市、同一日期、同一时点的SequenceFile的多个数据文件合并到最少一个文件，
# 导入Hive ORC表的一个分区中
# author:      张岩
# create_time: 2019/06/20
# comment:  执行方式：在 crontab 中创建定时任务
# parameters:
#      无
# 运行命令：
#      nohup ./script/merge-data.sh &
###################################################################################
export LANG=en_US.UTF-8
deal_date=`date +'%Y%m%d'` # deal_date 为当前日期，即运行此shell的日期
# 获取当前shell所在的目录
cd `dirname $0`
currpath=`pwd`
# 配置文件mromerge-conf.properties路径
mromerge_conf=${currpath}/../conf/mromerge-conf.properties
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
  log_dir=${currpath}/../log/
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
#########################################################################################################
# 2. 删除Hive ORC表mro_dt_rec中过期的分区以及里面的数据
# 获取表名
#dbname=`awk -F= '{if($1=="dbname") print $2}' ${mromerge_conf}`
#tb_name=`awk -F= '{if($1=="tb_name") print $2}' ${mromerge_conf}`
tb_name=${dbname}"."${tb_name}
# 获取表的数据文件存放路径
#tb_location=`awk -F= '{if($1=="tb_location") print $2}' ${mromerge_conf}`
# 获取 beeline 连接用户字符串
#beeline_user=`awk -F= '{if($1=="beeline_user") print $2}' ${mromerge_conf}`
################## 删除过期分区、过期数据 ##################
# 获取配置文件中数据的有效天数
#data_valid_days=`awk -F= '{if($1=="data_valid_days") print $2+0}' ${mromerge_conf}`
data_invalid_day=`date -d"-${data_valid_days}day" +'%Y%m%d'`
##################################################
echo "------------- `date +'%F %T'` 删除表${tb_name}中，日期在${data_invalid_day}以前的分区和数据，开始 ------------------" >> ${log_file}
# 获取Hive表 mro.mro_dt_rec 的所有分区，按照日志数据的日期进行排序，导入 临时文件del_part.tmp
beeline -u "${beeline_user};principal=${beeline_principal}" --silent=true --showHeader=false -e "show partitions ${tb_name}" | awk '{if($1=="|") print $2}' | cut -d/ -f1,2 | sort -u | sort -t"/" -k 2 1> /tmp/all_part.tmp 2>>${log_file}

beeline_result=`tail -1 ${log_file} | grep "No current connection"`
if [[ -n ${beeline_result} ]]; then
  echo "`date +'%F %T'` [FAILED] 通过Beeline与Hive连接失败，程序异常终止" >> ${log_file}
  echo "------------- `date +'%F %T'` 合并数据程序终止退出 ------------------" >> ${log_file}
  exit 1
fi

# ${data_invalid_day}以前的数据要被rm掉，分区要被drop掉
echo "`date +'%F %T'` [INFO] 删除表${tb_name}中，日期在${data_invalid_day}以前的数据" >>${log_file}
# 从 mro_dt_rec表的分区信息中找到 ${data_invalid_day}以前的数据
for item in $(cat /tmp/all_part.tmp); do
 # 获取所有分区中的日期
 itemdate=`echo ${item} | grep -o -E '[0-9]{8}'`
 if [[ ${itemdate} -lt ${data_invalid_day} ]]
 then
   # ${data_invalid_day}以前的数据，数据要被rm掉
   echo "`date +'%F %T'` [INFO] 分区 ${item} 及其数据要被移除" >>${log_file}
   hdfs dfs -rm -r ${tb_location}/${item} 1>>${log_file} 2>>${log_file}

   rm_result=`tail -1 ${log_file} | grep "Permission denied"`
   if [[ -n ${rm_result} ]]; then
     echo "`date +'%F %T'` [FAILED] 删除分区数据失败，权限不够，程序异常终止" >> ${log_file}
     echo "------------- `date +'%F %T'` 合并数据程序终止退出 ------------------" >> ${log_file}
     exit 1
   fi
 else
   break
 fi
done

echo "`date +'%F %T'` [INFO] 删除表${tb_name}中，日期在${data_invalid_day}以前的分区" >>${log_file}
echo "alter table ${tb_name} drop partition(logdate<'${data_invalid_day}');" >>${log_file}
beeline -u "${beeline_user};principal=${beeline_principal}" --silent=true --showHeader=false -e "alter table ${tb_name} drop partition(logdate<'${data_invalid_day}');" 2>>${log_file}

beeline_result=`tail -1 ${log_file} | grep "No current connection"`
if [[ -n ${beeline_result} ]]; then
  echo "`date +'%F %T'` [FAILED] 通过Beeline与Hive连接失败，程序异常终止" >> ${log_file}
  echo "------------- `date +'%F %T'` 合并数据程序终止退出 ------------------" >> ${log_file}
  exit 1
fi

rm -f /tmp/all_part.tmp 2>> ${log_file}
echo "------------- `date +'%F %T'` 删除表${tb_name}中，日期在${data_invalid_day}以前的分区和数据，结束 ------------------" >> ${log_file}

# 3. 开始运行 com.merge.MergeSeqFiles 合并数据
echo "------------- `date +'%F %T'` 合并数据，开始 ------------------" >> ${log_file}
log_level=`awk -F= '{ if($1=="log_level") print $2 }' ${mromerge_conf}`
#spark2-submit \
spark-submit \
             --principal ${hive_user} --keytab ${hive_keytab} \
             --master yarn --deploy-mode client \
             --conf spark.blacklist.enabled="false" \
             --jars ./generate-schemacls-1.0.jar,./postgresql-42.2.5.jar \
             --class com.merge.MergeSeqFiles \
             ./merge-seqfile-1.0.jar ${mromerge_conf_hdfs} ${log_level} 1>> ${log_file} 2>> ${log_file}
echo "------------- `date +'%F %T'` 合并数据，结束 ------------------" >> ${log_file}
