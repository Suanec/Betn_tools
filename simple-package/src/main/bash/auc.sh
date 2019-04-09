LOG_FILE=/data0/weibo_bigdata_pa/enzhao/ml-effi/dataflow-space/kspSpark/auc.log
function update_model_and_calc_auc(){
    pushd /data0/weibo_bigdata_pa/enzhao/ml-effi/dataflow-space/kspSpark/weips_v3.1/downloadModel 2>&1 > /dev/null
    sh save.sh ol-mpr-ftrl-fm-11w 101 2>&1 > /dev/null
    sh local_convert_to_weifm.sh /data0/weibo_bigdata_pa/enzhao/ml-effi/dataflow-space/kspSpark/weips_v3.1/downloadModel/ol-mpr-ftrl-fm-11w.101/angel/ 2>&1 > /dev/null
    popd 2>&1 > /dev/null
    python /data0/weibo_bigdata_pa/enzhao/ml-effi/dataflow-space/kspSpark/weips_v3.1/downloadModel/fm_suanec.py /data0/weibo_bigdata_pa/enzhao/ml-effi/dataflow-space/kspSpark/weips_v3.1/downloadModel/ol-mpr-ftrl-fm-11w.101/angel/weifm_text_model /data0/weibo_bigdata_pa/enzhao/ml-effi/dataflow-space/kspSpark/base/libsvm_sample 2>&1 | tail -1 >> ${LOG_FILE}
}
while true
do 
    update_model_and_calc_auc
    tail -1 ${LOG_FILE}
    sleep 300
done
# tailf auc.log
