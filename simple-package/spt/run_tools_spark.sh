SPARK_BIN="spark-submit"
TARGET_JAR="spark-tools-1-8-0_2.11-0.1.0-SNAPSHOT.jar"
MASTER_URL="yarn"
DEPLOY_MODE="cluster"
NUM_EXECUTORS="20"
DRIVER_MEMORY="1500m"
EXECUTOR_CORES="1"
EXECUTOR_MEMORY="900m"
QUEUE_NAME="ml_cc"
CLASS_NAME="transData"
SRC_DIR="hdfs://10.87.49.220:8020/user/push_weibo/temp_cuiwei_push_log_singlegroup_distinct_dt/000026_0"
TAR_DIR="temp_cuiwei_push_log_singlegroup_distinct_dt/000026_0"
MAP_NUM=80
config="
   ${SPARK_BIN} \
   --master ${MASTER_URL} \
   --deploy-mode ${DEPLOY_MODE} \
   --num-executors ${NUM_EXECUTORS} \
   --driver-memory ${DRIVER_MEMORY} \
   --executor-cores ${EXECUTOR_CORES} \
   --executor-memory ${EXECUTOR_MEMORY} \
   --queue ${QUEUE_NAME} \
   --conf spark.ui.retainedJobs=2 \
   --conf spark.ui.retainedStages=2 \
   --conf spark.worker.ui.retainedExecutors=5 \
   --conf spark.worker.ui.retainedDrivers=5 \
   --conf spark.eventLog.enabled=false \
   --class ${CLASS_NAME} \
   ${SRC_DIR} ${TAR_DIR} ${MAP_NUM}
"

SPARK_BIN="spark-submit"
TARGET_JAR="spark-tools-1-8-0_2.11-0.1.0-SNAPSHOT.jar"
MASTER_URL="yarn"
DEPLOY_MODE="cluster"
NUM_EXECUTORS="14"
DRIVER_MEMORY="500m"
EXECUTOR_CORES="4"
EXECUTOR_MEMORY="7g"
QUEUE_NAME="ml_cc"
CLASS_NAME="trainLRm"
ITER_NUM="192"
APP_NAME="lr-from-tiramisu-dnn"

config="
   $SPARK_BIN \
   --master $MASTER_URL \
   --deploy-mode ${DEPLOY_MODE} \
   --num-executors ${NUM_EXECUTORS} \
   --driver-memory ${DRIVER_MEMORY} \
   --executor-cores ${EXECUTOR_CORES} \
   --executor-memory ${EXECUTOR_MEMORY} \
   --conf spark.ui.retainedJobs=2 \
   --conf spark.ui.retainedStages=2 \
   --conf spark.worker.ui.retainedExecutors=5 \
   --conf spark.worker.ui.retainedDrivers=5 \
   --conf spark.eventLog.enabled=false \
   --class ${CLASS_NAME} \
   $TARGET_JAR $ITER_NUM $APP_NAME
"

date1=`date +%F_%H-%M-%S`
$config

date2=`date +%F_%H-%M-%S`
echo date start : $date1
echo data size : $trainpath
echo $iterNum
echo $config
echo date ended : $date2
