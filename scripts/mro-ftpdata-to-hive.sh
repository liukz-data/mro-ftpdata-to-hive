#!/usr/bin/env bash

# 将ftp上压缩数据入hive库表
# 导入Hive ORC表的一个分区中
# author:      刘科志
# create_time: 2020/01/03
# comment:  
# parameters:
#      无
# 运行命令：
#      sh mro-ftpdata-to-hive.sh 20181211
###################################################################################
export LANG=en_US.UTF-8
deal_date=`date +'%Y%m%d'` # deal_date 为当前日期，即运行此shell的日期
# 获取当前shell所在的目录
cd `dirname $0`
currpath=`pwd`
# 配置文件mromerge-conf.properties路径
mro_ftpdata_to_hive_conf=${currpath}/../conf/mro-ftpdata-to-hive-conf.properties
match_str=20181211

if [[ $# -lt 1 ]]
then
  echo "使用方法：mro-ftpdata-to-hive.sh [配置文件mro-ftpdata-to-hive-conf.properties的路径 match_str]"
  exit 0
elif [[ $# -eq 1 ]]
then
  match_str=$1
elif [[ $# -eq 2 ]]
then
  mro_ftpdata_to_hive_conf=$1
  match_str=$2
fi

source ${mro_ftpdata_to_hive_conf}
# 获取 hive_user 和 hive_keytab
# 运行 merge-data.sh 产生日志的路径
if [[ -z ${log_dir} ]]; then
  log_dir=${currpath}/../logs
fi
if [[ ! -d ${log_dir} ]]; then
  mkdir ${log_dir}
fi
log_file=${log_dir}/mro-ftpdata-to-hive-conf_${deal_date}.log
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
echo "------------- `date +'%F %T'` 查询 Yarn 中 FTPCompressedFileToHive 的application是否正在运行 ------------------" >> ${log_file}
isRunning=`yarn application -appStates NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING --list | grep "FTPCompressedFileToHive" 2>> ${log_file}`
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
spark-submit \
             --principal ${hive_user} --keytab ${hive_keytab} \
             --master yarn --deploy-mode client \
             --conf spark.driver.cores=3 \
             --conf spark.driver.memory=10g \
             --num-executors 8 \
             --conf spark.executor.memory=15g \
             --conf spark.executor.cores=12 \
             --conf spark.driver.memoryOverhead=1g \
             --conf spark.rpc.message.maxSize=500 \
             --conf spark.kryoserializer.buffer.max=512 \
             --conf spark.dynamicAllocation.enabled=false \
             --conf spark.yarn.executor.memoryOverhead=2g \
             --conf spark.network.timeout=300s \
             --conf spark.executor.heartbeatInterval=100s \
             --queue root.dev \
             --conf spark.blacklist.enabled="false" \
             --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseG1GC -XX:ParallelGCThreads=12 -XX:ConcGCThreads=5 -XX:InitiatingHeapOccupancyPercent=60" \
             --jars ../libs/postgresql-42.2.5.jar,../libs/fastjson-1.2.62.jar \
             --class com.cmdi.mro_ftpdata_to_hive.submit.FTPCompressedFileToHive \
             ../libs/mro-ftpdata-to-hive-1.0-SNAPSHOT.jar ${mro_ftpdata_to_hive_conf} ${match_str} 1>> ${log_file} 2>> ${log_file}

echo "------------- `date +'%F %T'` 合并数据，结束 ------------------" >> ${log_file}
