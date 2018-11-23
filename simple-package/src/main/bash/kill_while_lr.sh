RUN_WHILE_LR_PID=$(ps -ef | grep 'sh run_while_lr.sh' | grep -v grep | awk '{print $2}' | tail -1  )
if [ x${RUN_WHILE_LR_PID} != x ] ; then
   PID_TREE=$(pstree ${RUN_WHILE_LR_PID} -p)
   PID_LIST=$(echo ${PID_TREE} | awk -F"[()]" '{for(i=0;i<=NF;i++)if($i~/[0-9]+/)print $i}' | grep -v '|')
   echo "need to kill list "
   echo ${PID_LIST}
   echo ${PID_LIST} | xargs kill -9
   YARN_ID=$(yarn application -appStates RUNNING -list | grep -v 'Tracking-URL\|Total' | awk '{print $1}')
   for ID in $YARN_ID
     do
       yarn application -kill ${ID}
     done
fi
exit

