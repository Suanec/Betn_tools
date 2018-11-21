app_id=$1
if [ 'x' = 'x'${app_id} ]
then
    echo 'NEED APPLICATION_ID : yarn application id!!'
    exit 1
fi
HADOOP_CONF_DIR=/data0/rsync_data/control_center/ccConfs/yarn-setting/EMR-118-conf
cur_path=$(cd . && pwd)
cur_path=$( dirname $(readlink -f $0) )
log_path=${cur_path}/yarn-logs/${app_id}
user_name=$((export HADOOP_CONF_DIR=${HADOOP_CONF_DIR};yarn application  -status ${app_id} )| grep User | awk '{print $NF}')
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
export HADOOP_USER_NAME=${user_name}
(export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}; export HADOOP_USER_NAME=${user_name}; yarn logs -applicationId ${app_id} > ${log_path})
# cat ${log_path}
echo ${log_path}
