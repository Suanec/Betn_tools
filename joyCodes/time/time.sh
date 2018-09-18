# START_DATE=`date +"%s"`
# scala time
# END_DATE=`date +"%s"`
# RUN_TIME=`expr ${END_DATE}-${START_DATE}`
# ((RUN_TIME=${END_DATE}-${START_DATE}))
# echo ${RUN_TIME}

IDX=${1:-100}
START_DATE=`date +"%s"`
# for i in `seq 1 $IDX`; do scala time; done
for i in `seq 1 $IDX`
do 
  scala time
done
END_DATE=`date +"%s"`
((RUN_TIME=((${END_DATE}-${START_DATE})/${IDX})))
RUN_TIME=$(echo "(${END_DATE}-${START_DATE})/${IDX}" | bc -l )
echo "RUN_TIME : "
echo ${RUN_TIME} | bc -l
