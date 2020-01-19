#!/usr/bin/env bash

#建表脚本
# author:      刘科志
# create_time: 2020/01/03
# comment:  
# parameters:
#      无
# 运行命令：
#      sh mro-ftpdata-to-hive-create-tb.sh
###################################################################################
export LANG=en_US.UTF-8
deal_date=`date +'%Y%m%d'` # deal_date 为当前日期，即运行此shell的日期
# 获取当前shell所在的目录
cd `dirname $0`
currpath=`pwd`
# 配置文件mromerge-conf.properties路径
mro_ftpdata_to_hive_conf=${currpath}/../conf/mro-ftpdata-to-hive-conf.properties

if [[ $# -eq 1 ]]
then
  mro_ftpdata_to_hive_conf=$1
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
log_file=${log_dir}/mro-ftpdata-to-hive-conf-create-tb_${deal_date}.log
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


# 3. 开始建表
echo "------------- `date +'%F %T'` 建表，开始 ------------------" >> ${log_file}
spark-submit \
             --principal ${hive_user} --keytab ${hive_keytab} \
             --master yarn --deploy-mode client \
             --jars ../libs/postgresql-42.2.5.jar,../libs/fastjson-1.2.62.jar \
             --class com.cmdi.mro_ftpdata_to_hive.submit.CreateMroTable \
             ../libs/mro-ftpdata-to-hive-1.0-SNAPSHOT.jar ${mro_ftpdata_to_hive_conf} 1>> ${log_file} 2>> ${log_file}

echo "------------- `date +'%F %T'` 建表，结束 ------------------" >> ${log_file}
