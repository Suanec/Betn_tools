#!/usr/bin/env bash

RUN='odpscmd -e '

set -x
set -e

RAW_ODS_WLS_ENCODE_BHV_TABLE='hotweibo_ods_wls_encode_bhv'
RAW_BIGDATA_MDS_USER_BORNYEAR_MINING_TABLE='bigdata_mds_user_bornyear_mining'
RAW_BIGDATA_MDS_USER_GENDER_MINING_TABLE='bigdata_mds_user_gender_mining'
RAW_ODS_TBLOG_HOTMBLOG_EXPOSURE_STORAGE='ods_tblog_hotmblog_exposure_storage'
RAW_HOT_WEIBO_REAL_READ_EXPOSE_TABLE='hw_view_ods_tblog_real_read'
RAW_HW_LOCATION_TABLE='hw_xueli_location_0323'
RAW_HW_MONTH_USER_FREQUENCY_TABLE='bigdata_hotwb_month_recommend_mobile_user_2'
RAW_ODS_TBLOG_EXPO_TABLE='ods_tblog_expo'
RAW_HOT_MBLOG_USER_INTIMACY_PLAT='hot_mblog_user_intimacy_plat'
RAW_USER_LONG_INTEREST_MODIFIED_FOR_HOTMBLOG='user_long_interest_modified_for_hotmblog'
RAW_USERS_SHORT_INTEREST_IN_INTEREST_BASED_READING='users_short_interest_in_interest_based_reading'
RAW_HOT_MBLOG_RECOMMEND_MBLOG_INFO_TABLE='hot_mblog_recommend_mblog_info'
RAW_MDS_BHV_PUBBLOG_TABLE='mds_bhv_pubblog'
RAW_MDS_BHV_CMTBLOG_TABLE='mds_bhv_cmtblog'
RAW_MDS_BHV_LIKE_TABLE='mds_bhv_like'
RAW_MDS_BHV_ADDATTEN_TABLE='mds_bhv_addatten_14000008'

RAW_BIGDATA_VF_USER_DEVICE_TYPE_TABLE='bigdata_vf_user_device_type'
RAW_USER_RECALL_PREFERENCE_TABLE='user_recall_preference'
RAW_USER_RECALL_PREFERENCE_RESULT_TABLE='hw_user_multi_recall_ctr_itr'
RAW_HW_USER_AUTHOR_RELATIONSHIP_TABLE='hw_user_author_relationship'
RAW_HW_AUTHOR_MBLOG_EXPO_ACT_DAILY_TABLE='hw_author_mblog_expo_act_daily'
RAW_HW_AUTHOR_MBLOG_EXPO_ACT_DAILY_RESULT_TABLE='hw_author_month_ctr'
RAW_HW_VISIT_AUTHOR_PUB_NUM_MONTHLY_RXD_TABLE='hw_visit_author_pub_num_monthly_rxd'
RAW_HOTWB_MID_HEIGHT_TEMP_RXD_TABLE='hotwb_mid_height_temp_rxd'
RAW_MDS_BAS_USER_USAGEFREQ_TABLE='mds_bas_user_usagefreq'

HW_EXPO_ACTION_V3_TABLE='hw_expo_action_middle_v3'
HW_USER_BASE_FEATURE_V2_2_1_TABLE='hw_user_base_feature_v2_2_1'
HW_USER_BASE_FEATURE_V3_1_1_TABLE='hw_user_base_feature_v3_1_1'
HW_USER_BASE_FEATURE_PLUS_V3_2_1_TABLE='hw_user_base_feature_plus_v3_2_1'

function create_partition_table_without_row_format() {
    TABLE=$1
    FIELDS=$2
    PARTITIONS=$3
    ${RUN} "
        CREATE TABLE IF NOT EXISTS $TABLE ($FIELDS)
        PARTITIONED BY ($PARTITIONS); "
}

function create_partition() {
    TABLE=$1
    PARTITIONS=$2
    ${RUN} " ALTER TABLE $TABLE ADD IF NOT EXISTS PARTITION($PARTITIONS); "
}

function create_hw_expo_action_v3_table() {
    dt_name=$1
    fields='
        is_click string, actions string, isautoplay string,
        v_valid_play_duration string, v_object_duration string, v_duration string, v_replay_count string,
        v_click_count string, v_is_click string, v_pause_count string, v_video_orientation string,
        duration string,
        uid string, mid string, area_id string, author_id string, time string,
        network_type string, recommend_source string, category string,
        first_level_inte_weight string, second_level_inte_weight string,
        third_level_inte_weight string, effect_weight string, pic_weight string,
        article_weight string, video_weight string, obj_weight string,
        user_weight string, ret_num string, cmt_num string, like_num string,
        ret_num_recent string, cmt_num_recent string, like_num_recent string,
        expose_num string, act_num string, expose_num_recent string, act_num_recent string,
        article_read_num string, miaopai_view_num string, expo_ctr string, mblog_miaopai_num string,
        category_id string, enabledtriggers string, extend string, extend2 string, extend3 string
        '
    partitions='dt string'
    create_partition_table_without_row_format "$HW_EXPO_ACTION_V3_TABLE" "$fields" "$partitions"
    create_partition "$HW_EXPO_ACTION_V3_TABLE" "dt='$dt_name'"
}

function create_hw_user_base_feature_v3_1() {
    dt=$1
    table_fields='
            is_click string, actions string, isautoplay string, expo_time string, time_part string, network_type string, recommend_source string, recall_category string,
            recall_category_id string, real_duration string, exposure_position string, effect_weight string, request_area_id string, province_index string,

            v_valid_play_duration string, v_object_duration string, v_duration string, v_replay_count string, v_video_orientation string,

            uid string, user_frequency string, user_active_type string, user_born string, user_gender string, user_born_index string, user_gender_index string,
            user_minning_city_level string, user_minning_extra_area_id string, user_minning_city_name string, user_minning_city_tag string,
            user_minning_province_name string, user_minning_province_tag string, user_minning_city_weight string, user_location string,
            user_location_id string, user_area_id string, user_city_tag string, user_province_tag string, user_cold_start_tags string, user_long_first_tags string,
            user_long_second_tags string, user_long_third_tags string, user_short_first_tags string, user_short_second_tags string,
            user_short_third_tags string, user_merged_first_tags string, user_merged_second_tags string, user_merged_third_tags string,

            author_id string, author_class string, author_verified_type string, author_property string, author_type_index string, author_gender string,
            author_city string, author_province string, author_followers_num string, author_statuses_count string, author_sunshine_credit string,

            mid string, mblog_text_len string, mblog_level string, mblog_topic_num string, mblog_title_num string, mblog_miaopai_num string, mblog_link_num string,
            mblog_article_num string, mblog_pic_num string, mblog_gif_num string, mblog_long_pic_num string, mblog_panorama_num string, mblog_content_type string,
            mblog_picture_num_index string, mblog_ret_num string, mblog_cmt_num string, mblog_like_num string, mblog_ret_num_recent string,
            mblog_cmt_num_recent string, mblog_like_num_recent string, mblog_expose_num string, mblog_act_num string, mblog_expose_num_recent string,
            mblog_act_num_recent string, mblog_article_read_num string, mblog_miaopai_view_num string, mblog_total_read_num string, mblog_first_tags string,
            mblog_second_tags string, mblog_third_tags string, mblog_topic_tags string, mblog_keyword_tags string, mblog_area_tags string, mblog_first_max_tag string,
            mblog_second_max_tag string, mblog_third_max_tag string, mblog_topic_max_tag string, mblog_keyword_max_tag string, mblog_interact_num string,
            mblog_inter_act_num_recent string, mblog_hot_ret_num string, mblog_hot_cmt_num string, mblog_hot_like_num string, mblog_hot_ret_num_recent string,
            mblog_hot_cmt_num_recent string, mblog_hot_like_num_recent string, mblog_group_expo_num string, mblog_group_act_num string,
            mblog_group_interact_num string, mblog_group_ret_num string, mblog_group_cmt_num string, mblog_group_like_num string, mblog_group_expo_recent_num string,
            mblog_group_act_recent_num string, mblog_group_interact_recent_num string, mblog_group_ret_recent_num string, mblog_group_cmt_recent_num string,
            mblog_group_like_recent_num string, mblog_click_rate string, mblog_interact_rate string, mblog_group_click_rate string,
            mblog_group_interact_rate string, mblog_click_pic_num_norm string, mblog_click_video_num_norm string, mblog_click_single_page_num_norm string,
            mblog_click_follow_num_norm string, mblog_click_article_num_norm string, mblog_hot_ret_num_norm string, mblog_hot_cmt_num_norm string,
            mblog_hot_like_num_norm string, mblog_ret_num_norm string, mblog_cmt_num_norm string, mblog_like_num_norm string, mblog_hot_heat string,
            mblog_hot_heat_norm string, mblog_heat string, mblog_heat_norm string, mblog_click_pic_num string, mblog_click_video_num string,
            mblog_click_single_page_num string, mblog_click_follow_num string, mblog_click_article_num string, mblog_new_click_num string,
            mblog_click_num_rate string, mblog_group_click_num_rate string, mblog_click_pic_rate string, mblog_click_video_rate string,
            mblog_click_single_page_rate string, mblog_click_follow_rate string, mblog_click_article_rate string, mblog_hot_ret_rate string,
            mblog_hot_cmt_rate string, mblog_hot_like_rate string, mblog_group_click_pic_rate string, mblog_group_click_video_rate string,
            mblog_group_click_single_page_rate string, mblog_group_click_follow_rate string, mblog_group_click_article_rate string, mblog_real_expo_num string,
            mblog_real_group_expo_num string, mblog_real_click_rate string, mblog_real_interact_rate string, mblog_real_group_click_rate string,
            mblog_real_group_interact_rate string, mblog_real_click_pic_rate string, mblog_real_click_video_rate string, mblog_real_click_sing_page_rate string,
            mblog_real_click_follow_rate string, mblog_real_click_article_rate string, mblog_real_ret_rate string, mblog_real_cmt_rate string,
            mblog_real_like_rate string, mblog_real_group_click_pic_rate string, mblog_real_group_click_video_rate string,
            mblog_real_group_click_single_page_rate string, mblog_real_group_click_follow_rate string, mblog_real_group_click_article_rate string,
            mblog_real_read_duration string, mblog_real_read_uv string, mblog_read_duration_avg string, mblog_real_city_level_expo_num string,
            mblog_real_city_level_act_num string, mblog_real_city_level_interact_num string, mblog_real_city_level_click_rate string,
            mblog_real_city_level_interact_rate string, mblog_province_group_ctr string, mblog_province_group_click string, mblog_province_group_expo string,

            match_first_tag string, match_second_tag string, match_third_tag string, match_first_tag_v2 string, match_second_tag_v2 string, match_third_tag_v2 string,
            match_first_long_tag string, match_second_long_tag string, match_third_long_tag string, match_first_short_tag string, match_second_short_tag string,
            match_third_short_tag string, match_first_group_ctr string, match_second_group_ctr string, match_third_group_ctr string, match_first_group_v2_ctr string,
            match_second_group_v2_ctr string, match_third_group_v2_ctr string, match_first_group_long_ctr string, match_second_group_long_ctr string,
            match_third_group_long_ctr string, match_first_group_short_ctr string, match_second_group_short_ctr string, match_third_group_short_ctr string,
            match_first_level_inte_weight string, match_second_level_inte_weight string, match_third_level_inte_weight string, match_first_tag_user_value string,
            match_second_tag_user_value string, match_third_tag_user_Value string, match_first_tag_mblog_value string, match_second_tag_mblog_value string,
            match_third_tag_mblog_value string, match_first_tag_user_value_v2 string, match_second_tag_user_value_v2 string, match_third_tag_user_Value_v2 string,
            match_first_tag_mblog_value_v2 string, match_second_tag_mblog_value_v2 string, match_third_tag_mblog_value_v2 string, match_first_tag_user_long_value string,
            match_second_tag_user_long_value string, match_third_tag_user_long_Value string, match_first_tag_mblog_long_value string,
            match_second_tag_mblog_long_value string, match_third_tag_mblog_long_value string, match_first_tag_user_short_value string,
            match_second_tag_user_short_value string, match_third_tag_user_short_Value string, match_first_tag_mblog_short_value string,
            match_second_tag_mblog_short_value string, match_third_tag_mblog_short_value string, match_first_tag_ctrs string, match_second_tag_ctrs string,
            match_third_tag_ctrs string, match_user_author_intimacy string, is_match_location string,
            is_match_long_interest string, is_match_short_interest string, is_match_near_interest string, is_match_instant_interest string,

            mblog_follower_interact_rate string, mblog_total_interact_rate string,
            user_device string, user_device_type string, user_recall_type_ctr string, user_recall_type_itr string, recommend_source_ods string,
            match_relationship string, author_ctr string, author_pub_num string, mblog_height string, user_freq_all string
    '
    table_partitions='dt string'
    create_partition_table_without_row_format "$HW_USER_BASE_FEATURE_PLUS_V3_2_1_TABLE" "$table_fields" "$table_partitions"
    create_partition "$HW_USER_BASE_FEATURE_PLUS_V3_2_1_TABLE" "dt='$dt'"
}

function create_hw_user_base_feature_v3() {
    dt=$1
    table_fields='
        is_click string, actions string, isautoplay string, expo_time string, time_part string, network_type string,
        recommend_source string, recall_category string, recall_category_id string, real_duration string,
        exposure_position string, effect_weight string, request_area_id string, province_index string,

        v_valid_play_duration string, v_object_duration string, v_duration string, v_replay_count string, v_video_orientation string,

        uid string, user_frequency string, user_active_type string, user_born string, user_gender string,
        user_born_index string, user_gender_index string, user_minning_city_level string,
        user_minning_extra_area_id string, user_minning_city_name string, user_minning_city_tag string,
        user_minning_province_name string, user_minning_province_tag string, user_minning_city_weight string,
        user_location string, user_location_id string, user_area_id string, user_city_tag string,
        user_province_tag string, user_cold_start_tags string, user_long_first_tags string,
        user_long_second_tags string, user_long_third_tags string, user_short_first_tags string,
        user_short_second_tags string, user_short_third_tags string, user_merged_first_tags string,
        user_merged_second_tags string, user_merged_third_tags string,

        author_id string, author_class string, author_verified_type string, author_property string,
        author_type_index string, author_gender string, author_city string, author_province string,
        author_followers_num string, author_statuses_count string, author_sunshine_credit string,

        mid string, mblog_text_len string, mblog_level string, mblog_topic_num string, mblog_title_num string,
        mblog_miaopai_num string, mblog_link_num string, mblog_article_num string, mblog_pic_num string,
        mblog_gif_num string, mblog_long_pic_num string, mblog_panorama_num string, mblog_content_type string,
        mblog_picture_num_index string, mblog_ret_num string, mblog_cmt_num string, mblog_like_num string,
        mblog_ret_num_recent string, mblog_cmt_num_recent string, mblog_like_num_recent string,
        mblog_expose_num string, mblog_act_num string, mblog_expose_num_recent string, mblog_act_num_recent string,
        mblog_article_read_num string, mblog_miaopai_view_num string, mblog_total_read_num string,
        mblog_first_tags string, mblog_second_tags string, mblog_third_tags string, mblog_topic_tags string,
        mblog_keyword_tags string, mblog_area_tags string, mblog_first_max_tag string, mblog_second_max_tag string,
        mblog_third_max_tag string, mblog_topic_max_tag string, mblog_keyword_max_tag string, mblog_interact_num string,
        mblog_inter_act_num_recent string, mblog_hot_ret_num string, mblog_hot_cmt_num string, mblog_hot_like_num string,
        mblog_hot_ret_num_recent string, mblog_hot_cmt_num_recent string, mblog_hot_like_num_recent string,
        mblog_group_expo_num string, mblog_group_act_num string, mblog_group_interact_num string,
        mblog_group_ret_num string, mblog_group_cmt_num string, mblog_group_like_num string,
        mblog_group_expo_recent_num string, mblog_group_act_recent_num string, mblog_group_interact_recent_num string,
        mblog_group_ret_recent_num string, mblog_group_cmt_recent_num string, mblog_group_like_recent_num string,
        mblog_click_rate string, mblog_interact_rate string, mblog_group_click_rate string,
        mblog_group_interact_rate string, mblog_click_pic_num_norm string, mblog_click_video_num_norm string,
        mblog_click_single_page_num_norm string, mblog_click_follow_num_norm string, mblog_click_article_num_norm string,
        mblog_hot_ret_num_norm string, mblog_hot_cmt_num_norm string, mblog_hot_like_num_norm string,
        mblog_ret_num_norm string, mblog_cmt_num_norm string, mblog_like_num_norm string, mblog_hot_heat string,
        mblog_hot_heat_norm string, mblog_heat string, mblog_heat_norm string, mblog_click_pic_num string,
        mblog_click_video_num string, mblog_click_single_page_num string, mblog_click_follow_num string,
        mblog_click_article_num string,
        mblog_new_click_num string, mblog_click_num_rate string, mblog_group_click_num_rate string,
        mblog_click_pic_rate string, mblog_click_video_rate string, mblog_click_single_page_rate string,
        mblog_click_follow_rate string, mblog_click_article_rate string, mblog_hot_ret_rate string,
        mblog_hot_cmt_rate string, mblog_hot_like_rate string, mblog_group_click_pic_rate string,
        mblog_group_click_video_rate string, mblog_group_click_single_page_rate string,
        mblog_group_click_follow_rate string, mblog_group_click_article_rate string, mblog_real_expo_num string,
        mblog_real_group_expo_num string, mblog_real_click_rate string, mblog_real_interact_rate string,
        mblog_real_group_click_rate string, mblog_real_group_interact_rate string, mblog_real_click_pic_rate string,
        mblog_real_click_video_rate string, mblog_real_click_sing_page_rate string, mblog_real_click_follow_rate string,
        mblog_real_click_article_rate string, mblog_real_ret_rate string, mblog_real_cmt_rate string,
        mblog_real_like_rate string, mblog_real_group_click_pic_rate string, mblog_real_group_click_video_rate string,
        mblog_real_group_click_single_page_rate string, mblog_real_group_click_follow_rate string,
        mblog_real_group_click_article_rate string, mblog_real_read_duration string, mblog_real_read_uv string,
        mblog_read_duration_avg string, mblog_real_city_level_expo_num string, mblog_real_city_level_act_num string,
        mblog_real_city_level_interact_num string, mblog_real_city_level_click_rate string,
        mblog_real_city_level_interact_rate string, mblog_province_group_ctr string, mblog_province_group_click string,
        mblog_province_group_expo string,

        match_first_tag string, match_second_tag string, match_third_tag string, match_first_tag_v2 string,
        match_second_tag_v2 string, match_third_tag_v2 string, match_first_long_tag string, match_second_long_tag string,
        match_third_long_tag string, match_first_short_tag string, match_second_short_tag string, match_third_short_tag string,
        match_first_group_ctr string, match_second_group_ctr string, match_third_group_ctr string, match_first_group_v2_ctr string,
        match_second_group_v2_ctr string, match_third_group_v2_ctr string, match_first_group_long_ctr string,
        match_second_group_long_ctr string, match_third_group_long_ctr string, match_first_group_short_ctr string,
        match_second_group_short_ctr string, match_third_group_short_ctr string, match_first_level_inte_weight string,
        match_second_level_inte_weight string, match_third_level_inte_weight string, match_first_tag_user_value string,
        match_second_tag_user_value string, match_third_tag_user_Value string, match_first_tag_mblog_value string,
        match_second_tag_mblog_value string, match_third_tag_mblog_value string, match_first_tag_user_value_v2 string,
        match_second_tag_user_value_v2 string, match_third_tag_user_Value_v2 string, match_first_tag_mblog_value_v2 string,
        match_second_tag_mblog_value_v2 string, match_third_tag_mblog_value_v2 string, match_first_tag_user_long_value string,
        match_second_tag_user_long_value string, match_third_tag_user_long_Value string, match_first_tag_mblog_long_value string,
        match_second_tag_mblog_long_value string, match_third_tag_mblog_long_value string, match_first_tag_user_short_value string,
        match_second_tag_user_short_value string, match_third_tag_user_short_Value string, match_first_tag_mblog_short_value string,
        match_second_tag_mblog_short_value string, match_third_tag_mblog_short_value string, match_first_tag_ctrs string,
        match_second_tag_ctrs string, match_third_tag_ctrs string, match_user_author_intimacy string,
        is_match_location string, is_match_long_interest string, is_match_short_interest string, is_match_near_interest string,
        is_match_instant_interest string
    '
    table_partitions='dt string'
    create_partition_table_without_row_format "$HW_USER_BASE_FEATURE_V3_1_1_TABLE" "$table_fields" "$table_partitions"
    create_partition "$HW_USER_BASE_FEATURE_V3_1_1_TABLE" "dt='$dt'"
}

function gen_expo_act_data_v3() {
    dt_name=$1
    MAX_LIMIT='100'
    create_hw_expo_action_v3_table $dt_name
    ${RUN} "
        insert overwrite table
            $HW_EXPO_ACTION_V3_TABLE partition(dt='$dt_name')
        select
            case
                when ((T_ACTION.mid is not null) or (concat(if(T_ACTION.actions is null,'',T_ACTION.actions), if(C.action is not null,concat(',',C.action),''), if(D.action is not null,concat(',',D.action),''), if(E.action is not null,concat(',',E.action),''))<>'')) then 1
                else 0
            end as is_click,
            concat(if(T_ACTION.actions is null,'',T_ACTION.actions), if(C.action is not null,concat(',',C.action),''), if(D.action is not null,concat(',',D.action),''), if(E.action is not null,concat(',',E.action),''), if(F.action is not null,concat(',',F.action),'')) as actions,
            T_ACTION.isautoplay,
            T_ACTION.v_valid_play_duration, T_ACTION.v_object_duration, T_ACTION.v_duration, T_ACTION.v_replay_count,
            T_ACTION.v_click_count, T_ACTION.v_is_click, T_ACTION.v_pause_count, T_ACTION.v_video_orientation,
            T_REAL.duration,
            T_EXPO.uid, T_EXPO.id as mid, T_EXPO.area_id, T_EXPO.author_id, T_EXPO.time,
            T_EXPO.network_type, T_EXPO.recommend_source, T_EXPO.category,
            T_EXPO.first_level_inte_weight, T_EXPO.second_level_inte_weight,
            T_EXPO.third_level_inte_weight, T_EXPO.effect_weight, T_EXPO.pic_weight,
            T_EXPO.article_weight, T_EXPO.video_weight, T_EXPO.obj_weight,
            T_EXPO.user_weight, T_EXPO.ret_num, T_EXPO.cmt_num, T_EXPO.like_num,
            T_EXPO.ret_num_recent, T_EXPO.cmt_num_recent, T_EXPO.like_num_recent,
            T_EXPO.expose_num, T_EXPO.act_num, T_EXPO.expose_num_recent, T_EXPO.act_num_recent,
            T_EXPO.article_read_num, T_EXPO.miaopai_view_num, T_EXPO.ctr as expo_ctr, T_EXPO.mblog_miaopai_num,
            T_EXPO.category_id, T_EXPO.enabledtriggers, T_EXPO.extend, T_EXPO.extend2, T_EXPO.extend3
        FROM (
            SELECT * FROM
                $RAW_ODS_TBLOG_HOTMBLOG_EXPOSURE_STORAGE
            WHERE dt='$dt_name'
            and uid not in ('3655689037','')
            and recommend_source < 100
            and instr(category,'1042015:dujia')=0
            and instr(category,'yunying')=0
        ) T_EXPO

        left join (
            select
                tmp_limit_user.valid_uid
            from (
                select
                    uid as valid_uid, count(distinct uid, time) as m_cnt
                from $RAW_ODS_TBLOG_HOTMBLOG_EXPOSURE_STORAGE
                where category_id=''
                and length(uid)<=10
                and instr(category,'1042015:dujia')=0
                and instr(category,'yunying')=0
                and dt='$dt_name'
                and uid not in ('3655689037','')
                and recommend_source<100
                group by uid
            ) tmp_limit_user
            where tmp_limit_user.m_cnt<=$MAX_LIMIT
        ) T_LIMIT
        on T_EXPO.uid=T_LIMIT.valid_uid

        left join (
            select
                uid, itemid as mid,
                sum(if((read_duration is null or read_duration ='\\\N'),0.0,cast(read_duration as double))) as duration
            from $RAW_HOT_WEIBO_REAL_READ_EXPOSE_TABLE
            where dt='$dt_name'
            and read_duration>500
            and uid is not null
            and length(uid)<=10
            group by uid, itemid
        ) T_REAL
        on T_EXPO.uid=T_REAL.uid
        and T_EXPO.id=T_REAL.mid

        left join (
            SELECT
                uid, mid, isautoplay,
                wm_concat(',', if(action_code is null, 'null', action_code )) as actions,
                max(cast(valid_play_duration as bigint)) as v_valid_play_duration,
                max(cast(object_duration as bigint)) as v_object_duration,
                max(cast(duration as bigint)) as v_duration,
                max(cast(replay_count as bigint)) as v_replay_count,
                max(cast(click_count as bigint)) as v_click_count,
                max(cast(is_click as bigint)) as v_is_click,
                max(cast(pause_count as bigint)) as v_pause_count,
                wm_concat(',', video_orientation) as v_video_orientation
            FROM (
                SELECT
                    uid,
                    CASE WHEN (substr(target_id,1,1)='3' OR substr(target_id,1,1)='4') AND length(target_id)=16 THEN target_id
                        WHEN REGEXP_EXTRACT(extend, 'mid[:=]([0-9]+)', 1)<>'' THEN REGEXP_EXTRACT(extend, 'mid[:=]([0-9]+)', 1)
                    END as mid,
                    if (action in ('91','1423','7','41','54','55','127','128','749','3','381','658','659','4','336','932','933','6') or (action='799' and ((from_val>='1065000000' and split_part(split_part(extend,'valid_play_duration:', 2),'|', 1)>3000) or (from_val<'1065000000' and split_part(split_part(extend,'playduration:', 2),'|', 1)>3000))), action, 'invalid_action') as action_code,
                    if ((instr(extend, 'isautoplay:1')=0 or extend is null), 0, 1) as isautoplay,
                    if(REGEXP_EXTRACT(extend, 'valid_play_duration[:=]([0-9]+)', 1)<>'', REGEXP_EXTRACT(extend, 'valid_play_duration[:=]([0-9]+)', 1), '0') as valid_play_duration,
                    if(REGEXP_EXTRACT(extend, 'object_duration[:=]([0-9]+)', 1)<>'', REGEXP_EXTRACT(extend, 'object_duration[:=]([0-9]+)', 1), '0') as object_duration,
                    if(REGEXP_EXTRACT(extend, 'duration[:=]([0-9]+)', 1)<>'', REGEXP_EXTRACT(extend, 'duration[:=]([0-9]+)', 1), '0') as duration,
                    if(REGEXP_EXTRACT(extend, 'replay_count[:=]([0-9]+)', 1)<>'', REGEXP_EXTRACT(extend, 'replay_count[:=]([0-9]+)', 1), '0') as replay_count,
                    if(REGEXP_EXTRACT(extend, 'click_count[:=]([0-9]+)', 1)<>'', REGEXP_EXTRACT(extend, 'click_count[:=]([0-9]+)', 1), '0') as click_count,
                    if(REGEXP_EXTRACT(extend, 'is_click[:=]([0-9]+)', 1)<>'', REGEXP_EXTRACT(extend, 'is_click[:=]([0-9]+)', 1), '0') as is_click,
                    if(REGEXP_EXTRACT(extend, 'pause_count[:=]([0-9]+)', 1)<>'', REGEXP_EXTRACT(extend, 'pause_count[:=]([0-9]+)', 1), '0') as pause_count,
                    REGEXP_EXTRACT(extend, 'video_orientation[:=]([a-zA-Z]+)', 1) as video_orientation
                FROM $RAW_ODS_WLS_ENCODE_BHV_TABLE
                WHERE dt=$dt_name
            ) tmp_encode
            WHERE tmp_encode.mid is not null
            and action_code<>'invalid_action'
            group by uid, mid, isautoplay
        ) T_ACTION
        on T_EXPO.uid=T_ACTION.uid
        and T_EXPO.id=T_ACTION.mid

        left outer join (
            select C_tmp.uid, C_tmp.mid, C_tmp.action
            from (
                select uid, rootmid as mid, '3' as action,
                    row_number() over(partition by uid, rootmid order by unix_timestamp(time) ASC) as rid
                from $RAW_MDS_BHV_PUBBLOG_TABLE
                where dt='$dt_name' and is_transmit=1
            ) C_tmp
            where rid=1
        ) C
        on T_EXPO.uid=C.uid and T_EXPO.id=C.mid

        left outer join (
            select D_tmp.uid, D_tmp.mid, D_tmp.action
            from (
                select uid, mid, '4' as action,
                    row_number() over(partition by uid, mid order by unix_timestamp(time) ASC) as rid
                from $RAW_MDS_BHV_CMTBLOG_TABLE
                where dt='$dt_name'
            ) D_tmp
            where D_tmp.rid=1
        ) D
        on T_EXPO.uid=D.uid and T_EXPO.id=D.mid

        left outer join (
            select E_tmp.uid, E_tmp.mid, E_tmp.action
            from (
                select uid, object_id as mid, '6' as action,
                    row_number() over(partition by uid, object_id order by unix_timestamp(time) ASC) as rid
                from $RAW_MDS_BHV_LIKE_TABLE
                where dt='$dt_name' and mode=1 and length(object_id)=16
            ) E_tmp
            where E_tmp.rid=1
        ) E
        on T_EXPO.uid=E.uid and T_EXPO.id=E.mid

        left outer join (
            select
                uid, split(split(split(split(split(concat(',', extend),',spr=>')[1],',')[0],'\\;cardid:')[1],'\\;|\\,')[0],'_')[1] as mid, '91' as action
            from $RAW_MDS_BHV_ADDATTEN_TABLE
            where
                dt='$dt_name'
                and mode='12'
                and status='1'
                and substr(split(split(extend,'\\;fid:')[1],'\\;')[0],1,6)='102803'
                and substr(split(split(split(split(concat(',',extend),',spr=>')[1],',')[0],'\\;cardid:')[1],'\\;|\\,')[0],1,6)='102803'
                and length(split(split(split(split(split(concat(',',extend),',spr=>')[1],',')[0],'\\;cardid:')[1],'\\;|\\,')[0],'_')[1])=16
        ) F
        on T_EXPO.uid=F.uid and T_EXPO.id=F.mid

        where T_LIMIT.valid_uid is not null
        and T_REAL.uid is not null
        and T_REAL.mid is not null
    "
}

function add_base_feature_data_v3_1() {
    dt_name=$1
    month_before_dt=`date +%Y%m%d -d "$dt_name -30 days"`
    create_hw_user_base_feature_v3_1 "$dt_name"

    ##获取特征表最新分区
    big_vf_user_device_type_dt=`get_latest_partition "$RAW_BIGDATA_VF_USER_DEVICE_TYPE_TABLE" "$dt_name"`
    user_recall_preference_dt=`get_latest_partition "$RAW_USER_RECALL_PREFERENCE_RESULT_TABLE" "$dt_name"`
    hw_user_author_relationship_dt=`get_latest_partition "$RAW_HW_USER_AUTHOR_RELATIONSHIP_TABLE" "$dt_name"`
    hw_author_mblog_expo_act_daily_dt=`get_latest_partition "$RAW_HW_AUTHOR_MBLOG_EXPO_ACT_DAILY_RESULT_TABLE" "$dt_name"`
    hw_visit_author_pub_num_monthly_dt=`get_latest_partition "$RAW_HW_VISIT_AUTHOR_PUB_NUM_MONTHLY_RXD_TABLE" "$dt_name"`
    hotwb_mid_height_dt=`get_latest_partition "$RAW_HOTWB_MID_HEIGHT_TEMP_RXD_TABLE" "$dt_name"`
    mds_bas_user_usagefreq_dt=`get_latest_partition "$RAW_MDS_BAS_USER_USAGEFREQ_TABLE" "$dt_name"`

    odpscmd -e "
        set odps.sql.groupby.skewindata=true;
        set odps.stage.mapper.split.size=1024;
        set odps.stage.mapper.mem=4096;
        set odps.stage.reducer.mem=4096;
        set odps.stage.joiner.mem=4096;
        set odps.stage.mem=4096;

        insert overwrite table
            $HW_USER_BASE_FEATURE_PLUS_V3_2_1_TABLE partition (dt='$dt_name')
        select
            is_click, actions, isautoplay, expo_time, time_part, network_type, recommend_source, recall_category,
            recall_category_id, real_duration, exposure_position, effect_weight, request_area_id, province_index,

            v_valid_play_duration, v_object_duration, v_duration, v_replay_count, v_video_orientation,

            uid, user_frequency, user_active_type, user_born, user_gender, user_born_index, user_gender_index,
            user_minning_city_level, user_minning_extra_area_id, user_minning_city_name, user_minning_city_tag,
            user_minning_province_name, user_minning_province_tag, user_minning_city_weight, user_location,
            user_location_id, user_area_id, user_city_tag, user_province_tag, user_cold_start_tags, user_long_first_tags,
            user_long_second_tags, user_long_third_tags, user_short_first_tags, user_short_second_tags,
            user_short_third_tags, user_merged_first_tags, user_merged_second_tags, user_merged_third_tags,

            author_id, author_class, author_verified_type, author_property, author_type_index, author_gender,
            author_city, author_province, author_followers_num, author_statuses_count, author_sunshine_credit,

            mid, mblog_text_len, mblog_level, mblog_topic_num, mblog_title_num, mblog_miaopai_num, mblog_link_num,
            mblog_article_num, mblog_pic_num, mblog_gif_num, mblog_long_pic_num, mblog_panorama_num, mblog_content_type,
            mblog_picture_num_index, mblog_ret_num, mblog_cmt_num, mblog_like_num, mblog_ret_num_recent, mblog_cmt_num_recent,
            mblog_like_num_recent, mblog_expose_num, mblog_act_num, mblog_expose_num_recent, mblog_act_num_recent,
            mblog_article_read_num, mblog_miaopai_view_num, mblog_total_read_num, mblog_first_tags, mblog_second_tags,
            mblog_third_tags, mblog_topic_tags, mblog_keyword_tags, mblog_area_tags, mblog_first_max_tag, mblog_second_max_tag,
            mblog_third_max_tag, mblog_topic_max_tag, mblog_keyword_max_tag, mblog_interact_num, mblog_inter_act_num_recent,
            mblog_hot_ret_num, mblog_hot_cmt_num, mblog_hot_like_num, mblog_hot_ret_num_recent, mblog_hot_cmt_num_recent,
            mblog_hot_like_num_recent, mblog_group_expo_num, mblog_group_act_num, mblog_group_interact_num, mblog_group_ret_num,
            mblog_group_cmt_num, mblog_group_like_num, mblog_group_expo_recent_num, mblog_group_act_recent_num,
            mblog_group_interact_recent_num, mblog_group_ret_recent_num, mblog_group_cmt_recent_num, mblog_group_like_recent_num,
            mblog_click_rate, mblog_interact_rate, mblog_group_click_rate, mblog_group_interact_rate, mblog_click_pic_num_norm,
            mblog_click_video_num_norm, mblog_click_single_page_num_norm, mblog_click_follow_num_norm, mblog_click_article_num_norm,
            mblog_hot_ret_num_norm, mblog_hot_cmt_num_norm, mblog_hot_like_num_norm, mblog_ret_num_norm, mblog_cmt_num_norm,
            mblog_like_num_norm, mblog_hot_heat, mblog_hot_heat_norm, mblog_heat, mblog_heat_norm, mblog_click_pic_num,
            mblog_click_video_num, mblog_click_single_page_num, mblog_click_follow_num, mblog_click_article_num, mblog_new_click_num,
            mblog_click_num_rate, mblog_group_click_num_rate, mblog_click_pic_rate, mblog_click_video_rate, mblog_click_single_page_rate,
            mblog_click_follow_rate, mblog_click_article_rate, mblog_hot_ret_rate, mblog_hot_cmt_rate, mblog_hot_like_rate,
            mblog_group_click_pic_rate, mblog_group_click_video_rate, mblog_group_click_single_page_rate, mblog_group_click_follow_rate,
            mblog_group_click_article_rate, mblog_real_expo_num, mblog_real_group_expo_num, mblog_real_click_rate, mblog_real_interact_rate,
            mblog_real_group_click_rate, mblog_real_group_interact_rate, mblog_real_click_pic_rate, mblog_real_click_video_rate,
            mblog_real_click_sing_page_rate, mblog_real_click_follow_rate, mblog_real_click_article_rate, mblog_real_ret_rate,
            mblog_real_cmt_rate, mblog_real_like_rate, mblog_real_group_click_pic_rate, mblog_real_group_click_video_rate,
            mblog_real_group_click_single_page_rate, mblog_real_group_click_follow_rate, mblog_real_group_click_article_rate,
            mblog_real_read_duration, mblog_real_read_uv, mblog_read_duration_avg, mblog_real_city_level_expo_num,
            mblog_real_city_level_act_num, mblog_real_city_level_interact_num, mblog_real_city_level_click_rate,
            mblog_real_city_level_interact_rate, mblog_province_group_ctr, mblog_province_group_click, mblog_province_group_expo,

            match_first_tag, match_second_tag, match_third_tag, match_first_tag_v2, match_second_tag_v2, match_third_tag_v2,
            match_first_long_tag, match_second_long_tag, match_third_long_tag, match_first_short_tag, match_second_short_tag,
            match_third_short_tag, match_first_group_ctr, match_second_group_ctr, match_third_group_ctr, match_first_group_v2_ctr,
            match_second_group_v2_ctr, match_third_group_v2_ctr, match_first_group_long_ctr, match_second_group_long_ctr,
            match_third_group_long_ctr, match_first_group_short_ctr, match_second_group_short_ctr, match_third_group_short_ctr,
            match_first_level_inte_weight, match_second_level_inte_weight, match_third_level_inte_weight, match_first_tag_user_value,
            match_second_tag_user_value, match_third_tag_user_Value, match_first_tag_mblog_value, match_second_tag_mblog_value,
            match_third_tag_mblog_value, match_first_tag_user_value_v2, match_second_tag_user_value_v2, match_third_tag_user_Value_v2,
            match_first_tag_mblog_value_v2, match_second_tag_mblog_value_v2, match_third_tag_mblog_value_v2, match_first_tag_user_long_value,
            match_second_tag_user_long_value, match_third_tag_user_long_Value, match_first_tag_mblog_long_value,
            match_second_tag_mblog_long_value, match_third_tag_mblog_long_value, match_first_tag_user_short_value, match_second_tag_user_short_value,
            match_third_tag_user_short_Value, match_first_tag_mblog_short_value, match_second_tag_mblog_short_value,
            match_third_tag_mblog_short_value, match_first_tag_ctrs, match_second_tag_ctrs, match_third_tag_ctrs, match_user_author_intimacy,
            is_match_location, is_match_long_interest, is_match_short_interest, is_match_near_interest, is_match_instant_interest,

            if((mblog_heat='0' or author_followers_num='0'),'0',((mblog_heat_v/author_followers_num_v+pow(1.96,2)/(2*author_followers_num_v)-1.96*sqrt(mblog_heat_v/author_followers_num_v*(1-mblog_heat_v/author_followers_num_v)/author_followers_num_v+pow(1.96,2)/(4*pow(author_followers_num_v,2))))/(1+pow(1.96,2)/author_followers_num_v))) as mblog_follower_interact_rate,
            if((mblog_heat='0' or mblog_total_read_num='0'),'0',((mblog_heat_v/mblog_total_read_num_v+pow(1.96,2)/(2*mblog_total_read_num_v)-1.96*sqrt(mblog_heat_v/mblog_total_read_num_v*(1-mblog_heat_v/mblog_total_read_num_v)/mblog_total_read_num_v+pow(1.96,2)/(4*pow(mblog_total_read_num_v,2))))/(1+pow(1.96,2)/mblog_total_read_num_v))) as mblog_total_interact_rate,

            b_device as user_device, b_device_type as user_device_type, c_ctr as user_recall_type_ctr, c_itr as user_recall_type_itr,
            c_recommend_source as recommend_source_ods, d_relationship as match_relationship, e_ctr as author_ctr, f_pub_num as author_pub_num,
            g_height as mblog_height, h_lgn_all as user_freq_all

        from (
            select *,
                cast(mblog_heat as double) as mblog_heat_v,
                cast(author_followers_num as double) as author_followers_num_v,
                cast(mblog_total_read_num as double) as mblog_total_read_num_v,
                case
                    when recommend_source='16' then '3_1'
                    when recommend_source='20' then '28'
                    when recommend_source='21' then '20_1'
                    when recommend_source='23' then '20_2'
                    when recommend_source='24' then '27'
                    when recommend_source='25' then '20_3'
                    when recommend_source='26' then '24'
                    when recommend_source='37' then '23'
                    when recommend_source='53' then '15_1'
                    when recommend_source='54' then '15_2'
                    when recommend_source='56' then '15_1'
                    when recommend_source='57' then '24_2'
                    when recommend_source='58' then '25'
                    when recommend_source='59' then '24_1'
                    when recommend_source='60' then '26'
                    when recommend_source='72' then '16'
                    when recommend_source='81' then '10_1'
                    when recommend_source='82' then '10_2'
                    when recommend_source='83' then '10_2'
                    when recommend_source='91' then '9_1'
                    when recommend_source='92' then '9_2'
                else recommend_source end as recommend_source_ods
            from $HW_USER_BASE_FEATURE_V3_1_1_TABLE
            where dt=$dt_name
        ) A

        left join (
            select
                b_uid,
                b_device,
                b_device_type
            from (
                select
                    uid as b_uid,
                    device as b_device,
                    case
                        when instr(tolower(device), 'sonny')>0 then 'sonny'
                        when instr(tolower(device), 'moto')>0 then 'moto'
                        when instr(tolower(device), 'nokia')>0 then 'nokia'
                        when instr(tolower(device), 'nubia')>0 then 'nubia'
                        when instr(tolower(device), 'meitu')>0 then 'meitu'
                        when instr(tolower(device), 'htc')>0 then 'htc'
                        when instr(tolower(device), 'lenovo')>0 then 'lenovo'
                        when instr(tolower(device), 'gionee')>0 then 'gionee'
                        when instr(tolower(device), 'meizu')>0 then 'meizu'
                        when instr(tolower(device), 'samsung')>0 then 'samsung'
                        when instr(tolower(device), 'xiaomi')>0 then 'xiaomi'
                        when instr(tolower(device), 'huawei')>0 then 'huawei'
                        when instr(tolower(device), 'vivo')>0 then 'vivo'
                        when instr(tolower(device), 'oppo')>0 then 'oppo'
                        when instr(tolower(device), 'iphone')>0 then 'iphone'
                    else 'other' end as b_device_type,
                    ROW_NUMBER() OVER (partition by uid order by device) as rid
                from $RAW_BIGDATA_VF_USER_DEVICE_TYPE_TABLE
                where user_type='0'
                and dt=$big_vf_user_device_type_dt
            ) tmp_B
            where rid=1
        ) B
        on A.uid=B.b_uid

        left join (
            select
                c_uid,
                c_recommend_source,
                c_ctr,
                c_itr
            from (
                select
                    uid as c_uid,
                    case
                        when recommend_source='53' then '15_1'
                        when recommend_source='54' then '15_2'
                        when recommend_source='56' then '15_1'
                    else recommend_source end as c_recommend_source,
                    ctr as c_ctr,
                    itr as c_itr,
                    ROW_NUMBER() OVER (partition by uid order by recommend_source) as rid
                from $RAW_USER_RECALL_PREFERENCE_RESULT_TABLE
                where dt=$user_recall_preference_dt
            ) tmp_C
            where rid=1
        ) C
        on A.uid=C.c_uid and A.recommend_source_ods=C.c_recommend_source

        left join (
            select
                uid as d_uid,
                author_id as d_author_id,
                max(cast(relationship as bigint)) as d_relationship
            from $RAW_HW_USER_AUTHOR_RELATIONSHIP_TABLE
            where dt=$hw_user_author_relationship_dt
            group by uid, author_id
        ) D
        on A.uid=D.d_uid and A.author_id=D.d_author_id

        left join (
            select
                author as e_author_id,
                max(cast(ctr as double)) as e_ctr
            from $RAW_HW_AUTHOR_MBLOG_EXPO_ACT_DAILY_RESULT_TABLE
            where dt=$hw_author_mblog_expo_act_daily_dt
            group by author
        ) E
        on A.author_id=E.e_author_id

        left join (
            select
                uid as f_author_id,
                max(cast(pub_num as bigint)) as f_pub_num
            from $RAW_HW_VISIT_AUTHOR_PUB_NUM_MONTHLY_RXD_TABLE
            where dt=$hw_visit_author_pub_num_monthly_dt
            group by uid
        ) F
        on A.author_id=F.f_author_id

        left join (
            select
                mid as g_mid,
                max(cast(height as double)) as g_height
            from $RAW_HOTWB_MID_HEIGHT_TEMP_RXD_TABLE
            where dt>$month_before_dt and dt<=$hotwb_mid_height_dt
            group by mid
        ) G
        on A.mid=G.g_mid

        left join (
            select
                uid as h_uid,
                max(cast(lgn_all as bigint)) as h_lgn_all
            from $RAW_MDS_BAS_USER_USAGEFREQ_TABLE
            where dt=$mds_bas_user_usagefreq_dt
            group by uid
        ) H
        on A.uid=H.h_uid
    ;
    "
}

function gen_base_feature_data_v3() {
    dt_name=$1
    merge_part="dt=$dt_name"
    create_hw_user_base_feature_v3 "$dt_name"

    ##获取特征表最新分区
    reader_intimacy_table_dt=`get_latest_partition "$RAW_HOT_MBLOG_USER_INTIMACY_PLAT" "$dt_name"`
    user_long_interests_dt=`get_latest_partition "$RAW_USER_LONG_INTEREST_MODIFIED_FOR_HOTMBLOG" "$dt_name"`
    user_short_interests_dt=`get_latest_partition "$RAW_USERS_SHORT_INTEREST_IN_INTEREST_BASED_READING" "$dt_name"`
    month_before_dt=`date +%Y%m%d -d "$dt_name -30 days"`

    odpscmd -e "
        set odps.sql.groupby.skewindata=true;
        set odps.stage.mapper.split.size=1024;
        set odps.stage.mapper.mem=4096;
        set odps.stage.reducer.mem=4096;
        set odps.stage.joiner.mem=4096;
        set odps.stage.mem=4096;

        #add jar ../lib/hot_weibo_aliyun_udf_wb.jar -f;
        #add file ../conf/user_interest_cold_start_file -f;
        #add file ../conf/second_tag_index.conf -f;
        #add file ../conf/third_tag_object_prefix.conf -f;
        #create function udf_ali_hw_visit_expo_pos_explode as 'com.weibo.aliyun.udf.tools.UDFVisitExpoPosExtract' using 'hot_weibo_aliyun_udf_wb.jar' -f;
        #create function udf_user_raw_feature_processor_v3 as 'com.weibo.aliyun.udf.user_feed.UDFUserRawFeatureProcessorV3' using 'hot_weibo_aliyun_udf_wb.jar, user_interest_cold_start_file, second_tag_index.conf, third_tag_object_prefix.conf' -f;

        insert overwrite table
            $HW_USER_BASE_FEATURE_V3_1_1_TABLE partition($merge_part)
        select
            X.is_click, X.actions, X.isautoplay, X.expo_time, X.time_part, X.network_type, X.recommend_source, X.recall_category,
            X.recall_category_id, X.real_duration, X.exposure_position, X.effect_weight, X.request_area_id, X.province_index,

            X.v_valid_play_duration, X.v_object_duration, X.v_duration, X.v_replay_count, X.v_video_orientation,

            X.uid, X.user_frequency, X.user_active_type, X.user_born, X.user_gender, X.user_born_index, X.user_gender_index,
            X.user_minning_city_level, X.user_minning_extra_area_id, X.user_minning_city_name, X.user_minning_city_tag,
            X.user_minning_province_name, X.user_minning_province_tag, X.user_minning_city_weight, X.user_location,
            X.user_location_id, X.user_area_id, X.user_city_tag, X.user_province_tag, X.user_cold_start_tags, X.user_long_first_tags,
            X.user_long_second_tags, X.user_long_third_tags, X.user_short_first_tags, X.user_short_second_tags,
            X.user_short_third_tags, X.user_merged_first_tags, X.user_merged_second_tags, X.user_merged_third_tags,

            X.author_id, X.author_class, X.author_verified_type, X.author_property, X.author_type_index, X.author_gender,
            X.author_city, X.author_province, X.author_followers_num, X.author_statuses_count, X.author_sunshine_credit,

            X.mid, X.mblog_text_len, X.mblog_level, X.mblog_topic_num, X.mblog_title_num, X.mblog_miaopai_num, X.mblog_link_num,
            X.mblog_article_num, X.mblog_pic_num, X.mblog_gif_num, X.mblog_long_pic_num, X.mblog_panorama_num, X.mblog_content_type,
            X.mblog_picture_num_index, X.mblog_ret_num, X.mblog_cmt_num, X.mblog_like_num, X.mblog_ret_num_recent,
            X.mblog_cmt_num_recent, X.mblog_like_num_recent, X.mblog_expose_num, X.mblog_act_num, X.mblog_expose_num_recent,
            X.mblog_act_num_recent, X.mblog_article_read_num, X.mblog_miaopai_view_num, X.mblog_total_read_num, X.mblog_first_tags,
            X.mblog_second_tags, X.mblog_third_tags, X.mblog_topic_tags, X.mblog_keyword_tags, X.mblog_area_tags, X.mblog_first_max_tag,
            X.mblog_second_max_tag, X.mblog_third_max_tag, X.mblog_topic_max_tag, X.mblog_keyword_max_tag, X.mblog_interact_num,
            X.mblog_inter_act_num_recent, X.mblog_hot_ret_num, X.mblog_hot_cmt_num, X.mblog_hot_like_num, X.mblog_hot_ret_num_recent,
            X.mblog_hot_cmt_num_recent, X.mblog_hot_like_num_recent, X.mblog_group_expo_num, X.mblog_group_act_num,
            X.mblog_group_interact_num, X.mblog_group_ret_num, X.mblog_group_cmt_num, X.mblog_group_like_num, X.mblog_group_expo_recent_num,
            X.mblog_group_act_recent_num, X.mblog_group_interact_recent_num, X.mblog_group_ret_recent_num, X.mblog_group_cmt_recent_num,
            X.mblog_group_like_recent_num, X.mblog_click_rate, X.mblog_interact_rate, X.mblog_group_click_rate,
            X.mblog_group_interact_rate, X.mblog_click_pic_num_norm, X.mblog_click_video_num_norm, X.mblog_click_single_page_num_norm,
            X.mblog_click_follow_num_norm, X.mblog_click_article_num_norm, X.mblog_hot_ret_num_norm, X.mblog_hot_cmt_num_norm,
            X.mblog_hot_like_num_norm, X.mblog_ret_num_norm, X.mblog_cmt_num_norm, X.mblog_like_num_norm, X.mblog_hot_heat,
            X.mblog_hot_heat_norm, X.mblog_heat, X.mblog_heat_norm, X.mblog_click_pic_num, X.mblog_click_video_num,
            X.mblog_click_single_page_num, X.mblog_click_follow_num, X.mblog_click_article_num, X.mblog_new_click_num,
            X.mblog_click_num_rate, X.mblog_group_click_num_rate, X.mblog_click_pic_rate, X.mblog_click_video_rate,
            X.mblog_click_single_page_rate, X.mblog_click_follow_rate, X.mblog_click_article_rate, X.mblog_hot_ret_rate,
            X.mblog_hot_cmt_rate, X.mblog_hot_like_rate, X.mblog_group_click_pic_rate, X.mblog_group_click_video_rate,
            X.mblog_group_click_single_page_rate, X.mblog_group_click_follow_rate, X.mblog_group_click_article_rate, X.mblog_real_expo_num,
            X.mblog_real_group_expo_num, X.mblog_real_click_rate, X.mblog_real_interact_rate, X.mblog_real_group_click_rate,
            X.mblog_real_group_interact_rate, X.mblog_real_click_pic_rate, X.mblog_real_click_video_rate, X.mblog_real_click_sing_page_rate,
            X.mblog_real_click_follow_rate, X.mblog_real_click_article_rate, X.mblog_real_ret_rate, X.mblog_real_cmt_rate,
            X.mblog_real_like_rate, X.mblog_real_group_click_pic_rate, X.mblog_real_group_click_video_rate,
            X.mblog_real_group_click_single_page_rate, X.mblog_real_group_click_follow_rate, X.mblog_real_group_click_article_rate,
            X.mblog_real_read_duration, X.mblog_real_read_uv, X.mblog_read_duration_avg, X.mblog_real_city_level_expo_num,
            X.mblog_real_city_level_act_num, X.mblog_real_city_level_interact_num, X.mblog_real_city_level_click_rate,
            X.mblog_real_city_level_interact_rate, X.mblog_province_group_ctr, X.mblog_province_group_click, X.mblog_province_group_expo,

            X.match_first_tag, X.match_second_tag, X.match_third_tag, X.match_first_tag_v2, X.match_second_tag_v2, X.match_third_tag_v2,
            X.match_first_long_tag, X.match_second_long_tag, X.match_third_long_tag, X.match_first_short_tag, X.match_second_short_tag,
            X.match_third_short_tag, X.match_first_group_ctr, X.match_second_group_ctr, X.match_third_group_ctr, X.match_first_group_v2_ctr,
            X.match_second_group_v2_ctr, X.match_third_group_v2_ctr, X.match_first_group_long_ctr, X.match_second_group_long_ctr,
            X.match_third_group_long_ctr, X.match_first_group_short_ctr, X.match_second_group_short_ctr, X.match_third_group_short_ctr,
            X.match_first_level_inte_weight, X.match_second_level_inte_weight, X.match_third_level_inte_weight, X.match_first_tag_user_value,
            X.match_second_tag_user_value, X.match_third_tag_user_Value, X.match_first_tag_mblog_value, X.match_second_tag_mblog_value,
            X.match_third_tag_mblog_value, X.match_first_tag_user_value_v2, X.match_second_tag_user_value_v2, X.match_third_tag_user_Value_v2,
            X.match_first_tag_mblog_value_v2, X.match_second_tag_mblog_value_v2, X.match_third_tag_mblog_value_v2, X.match_first_tag_user_long_value,
            X.match_second_tag_user_long_value, X.match_third_tag_user_long_Value, X.match_first_tag_mblog_long_value,
            X.match_second_tag_mblog_long_value, X.match_third_tag_mblog_long_value, X.match_first_tag_user_short_value,
            X.match_second_tag_user_short_value, X.match_third_tag_user_short_Value, X.match_first_tag_mblog_short_value,
            X.match_second_tag_mblog_short_value, X.match_third_tag_mblog_short_value, X.match_first_tag_ctrs, X.match_second_tag_ctrs,
            X.match_third_tag_ctrs, X.match_user_author_intimacy, X.is_match_location,
            X.is_match_long_interest, X.is_match_short_interest, X.is_match_near_interest, X.is_match_instant_interest
        from (
            select
                ABC.*,
                intimacy_table.intimacy as user_intimacy,
                concat(long_table.f1,',',long_table.f2,',',long_table.f3) as user_long_interests,
                concat(short_table.interests1,',',short_table.interests2,',',short_table.interests3) as user_short_interests,
                born_table.age as u_born, gender_table.u_gender,
                frequency_table.u_frequency, location_table.*
            from (
                select AB.*,C.exposure_position,C.expo_time
                from (
                    select
                        A.*,B.*
                    from (
                        select k.*
                        from (
                            select a.u as valid_uid
                            from (
                                select uid as u,sum(is_click) as sum_click
                                from $HW_EXPO_ACTION_V3_TABLE
                                where dt='$dt_name' group by uid
                            )a
                            where a.sum_click>0
                        ) g

                        inner join(
                            select d.is_click,
                                   d.actions, d.isautoplay,
                                   d.duration as real_duration,
                                   d.v_valid_play_duration, d.v_object_duration, d.v_duration,
                                   d.v_replay_count, d.v_video_orientation,
                                   d.uid, d.mid, d.area_id, d.author_id, substr(d.time,1,10) as new_time,
                                   d.network_type, d.recommend_source, d.category as recall_category,
                                   d.first_level_inte_weight, d.second_level_inte_weight,
                                   d.third_level_inte_weight, d.effect_weight,
                                   d.ret_num, d.cmt_num, d.like_num,
                                   d.ret_num_recent, d.cmt_num_recent, d.like_num_recent,
                                   d.expose_num, d.act_num, d.expose_num_recent, d.act_num_recent,
                                   d.article_read_num, d.miaopai_view_num, d.expo_ctr,
                                   d.category_id, d.enabledtriggers, d.extend, d.extend2, d.extend3, d.dt
                            from(
                                select *
                                from (
                                    select *,ROW_NUMBER() OVER(PARTITION BY uid, mid
                                        ORDER BY concat(if(is_click=1,0,1),if(isautoplay IS NULL,0,isautoplay),time) ASC) as rank
                                    from $HW_EXPO_ACTION_V3_TABLE where dt='$dt_name'
                                )c
                                where c.rank=1
                            )d
                        ) k
                        on g.valid_uid=k.uid
                    ) A
    #热门微博推荐表
                    inner join (
                            select *
                            from (
                                select
                                    uid as author_id1, verified_type as author_verified_type, user_class as author_class,
                                    user_property as author_property, gender as author_gender,
                                    city as author_city, province as author_province, followers_num as author_followers_num,
                                    statuses_count as author_statuses_count, mid as recom_mid, text_len,
                                    content_tag, mblog_level, mblog_topic_num, mblog_gif_num, mblog_long_pic_num, pic_num,
                                    mblog_miaopai_num, mblog_link_num, mblog_article_num, mblog_title_num, dictionary,
                                    ROW_NUMBER() OVER (partition by mid order by expose_num desc) as nums
                                from $RAW_HOT_MBLOG_RECOMMEND_MBLOG_INFO_TABLE
                                where dt>='$month_before_dt' and dt<='$dt_name'
                            ) tmp
                            where tmp.nums = 1
                        ) B
                        on A.mid=B.recom_mid
                )AB
    #博文曝光日志
                inner join (
                    select
                        tmp_C.uid as expo_uid, mid as expo_mid, tmp_C.expo_time, min(expo_pos) as exposure_position
                    from (
                        select uid, mid_list, expo_time
                        from $RAW_ODS_TBLOG_EXPO_TABLE
                        where dt='$dt_name'
                        and length(uid)<=10
                        and uid is not null
                        and volume>0
                        and (split(split(extend2,'scenes=>')[1],',')[0]<>'1' or split(split(extend2,'scenes=>')[1],',')[0] is null)
                        and appid=6 and interface_id in (800,900)
                        and split(split(extend2,'containerid=>')[1],',')[0]='102803'
                        and if(extend2 like ',uicode=>',split(split(extend2,',uicode=>')[1],',')[0],split(split(extend2,'uicode=>')[1],',')[0])<>'10000511'
                        and (
                            (split(split(extend2,'full_containerid=>')[1],',')[0]='102803_ctg1_1760_-_ctg1_1760' and substr(split(split(extend2,'from=>')[1],',')[0],3,3) in ('530','540') and substr(split(split(extend2,'from=>')[1],',')[0],-4,1)='5')
                            or
                            (split(split(extend2,'full_containerid=>')[1],',')[0]='102803' and substr(split(split(extend2,'from=>')[1],',')[0],3,3)>='545' )
                            or
                            (split(split(extend2,'full_containerid=>')[1],',')[0]='102803_ctg1_1760_-_ctg1_1760' and substr(split(split(extend2,'from=>')[1],',')[0],3,3) ='540' and substr(split(split(extend2,'from=>')[1],',')[0],-4,1)='3')
                            )
                    ) tmp_C lateral view udf_ali_hw_visit_expo_pos_explode(tmp_C.mid_list) tmp as mid, expo_pos
                    group by uid, mid, expo_time
                ) C
                on AB.uid=C.expo_uid and AB.mid=C.expo_mid and AB.new_time=C.expo_time
            ) ABC
    #亲密度表
            left join (
                select *
                from (
                    select *, row_number() over(partition by uid order by intimacy desc) as rank
                    from $RAW_HOT_MBLOG_USER_INTIMACY_PLAT where dt = '$reader_intimacy_table_dt'
                ) it
                where it.rank=1
            ) intimacy_table
            on ABC.uid = intimacy_table.uid
    #长兴趣表
            left join (
                select
                    uid,
                    concat_ws(',',collect_list(f1)) as f1,
                    concat_ws(',',collect_list(f2)) as f2,
                    concat_ws(',',collect_list(f3)) as f3
                from $RAW_USER_LONG_INTEREST_MODIFIED_FOR_HOTMBLOG
                where dt = '$user_long_interests_dt'
                group by uid
            ) long_table
            on ABC.uid = long_table.uid
    #短兴趣表
            left join (
                select
                    uid,
                    concat_ws(',',collect_list(interests1)) as interests1,
                    concat_ws(',',collect_list(interests2)) as interests2,
                    concat_ws(',',collect_list(interests3)) as interests3
                from $RAW_USERS_SHORT_INTEREST_IN_INTEREST_BASED_READING
                where dt = '$user_short_interests_dt'
                group by uid
            ) short_table
            on ABC.uid = short_table.uid
    #频次表
            left join (
                select *
                from (
                    select *, frequency as u_frequency, row_number() over(partition by uid order by frequency desc) as rank
                    from $RAW_HW_MONTH_USER_FREQUENCY_TABLE where dt='$dt_name'
                ) ft
                where ft.rank=1
            ) frequency_table
            on ABC.uid = frequency_table.uid
    #常去地址表
            left join (
                select *
                from (
                    select
                        uid as loc_uid, level as user_city_level, area_id as user_extra_area_id,
                        city_name as user_city_name, city_tag as user_city_tag, province as user_province_name,
                        province_tag as user_province_tag, weight as user_city_weight,
                        row_number() over(partition by uid order by weight desc) as rank
                    from $RAW_HW_LOCATION_TABLE
                    where dt='$dt_name' and uid is not null and length(uid)<=10
                ) loct
                where loct.rank=1
            ) location_table
            on ABC.uid = location_table.loc_uid
    #年龄表
            left join (
                select *
                from (
                    select
                        *, row_number() over(partition by uid order by weight desc) as rank
                    from $RAW_BIGDATA_MDS_USER_BORNYEAR_MINING_TABLE
                ) bt
                where bt.rank=1)
            born_table
            on ABC.uid = born_table.uid
    #性别表
            left join (
                select *,
                    case when gt.gender='1' then 'm'
                        when gt.gender='2' then 'f'
                    else '' end as u_gender
                from (
                    select *, row_number() over(partition by uid order by weight desc) as row
                    from $RAW_BIGDATA_MDS_USER_GENDER_MINING_TABLE
                ) gt
                where gt.row=1
            ) gender_table
            on ABC.uid = gender_table.uid

        ) tmp_table lateral view udf_user_raw_feature_processor_v3(
            is_click, actions, isautoplay, expo_time, network_type, recommend_source, recall_category,
            real_duration, exposure_position, effect_weight,
            uid, u_frequency, u_born, u_gender, area_id, first_level_inte_weight, second_level_inte_weight,
            third_level_inte_weight, user_long_interests, user_short_interests, user_intimacy,
            user_city_level, user_extra_area_id, user_city_name, user_city_tag, user_province_name,
            user_province_tag, user_city_weight,
            author_id, author_verified_type, author_class, author_property, author_gender,
            author_city, author_province, author_followers_num, author_statuses_count,
            mid, ret_num, cmt_num, like_num, ret_num_recent, cmt_num_recent, like_num_recent, expose_num,
            act_num, expose_num_recent, act_num_recent, article_read_num, miaopai_view_num,
            text_len, content_tag, mblog_level, mblog_topic_num, mblog_gif_num, mblog_long_pic_num, pic_num,
            mblog_miaopai_num, mblog_link_num, mblog_article_num, mblog_title_num, dictionary,
            v_valid_play_duration, v_object_duration, v_duration, v_replay_count, v_video_orientation,
            extend, extend2, extend3, dt
        ) X as

            is_click, actions, isautoplay, expo_time, time_part, network_type, recommend_source, recall_category,
            recall_category_id, real_duration, exposure_position, effect_weight, request_area_id, province_index,

            v_valid_play_duration, v_object_duration, v_duration, v_replay_count, v_video_orientation,

            uid, user_frequency, user_active_type, user_born, user_gender, user_born_index, user_gender_index,
            user_minning_city_level, user_minning_extra_area_id, user_minning_city_name, user_minning_city_tag,
            user_minning_province_name, user_minning_province_tag, user_minning_city_weight, user_location,
            user_location_id, user_area_id, user_city_tag, user_province_tag, user_cold_start_tags, user_long_first_tags,
            user_long_second_tags, user_long_third_tags, user_short_first_tags, user_short_second_tags,
            user_short_third_tags, user_merged_first_tags, user_merged_second_tags, user_merged_third_tags,

            author_id, author_class, author_verified_type, author_property, author_type_index, author_gender,
            author_city, author_province, author_followers_num, author_statuses_count, author_sunshine_credit,

            mid, mblog_text_len, mblog_level, mblog_topic_num, mblog_title_num, mblog_miaopai_num, mblog_link_num,
            mblog_article_num, mblog_pic_num, mblog_gif_num, mblog_long_pic_num, mblog_panorama_num, mblog_content_type,
            mblog_picture_num_index, mblog_ret_num, mblog_cmt_num, mblog_like_num, mblog_ret_num_recent, mblog_cmt_num_recent,
            mblog_like_num_recent, mblog_expose_num, mblog_act_num, mblog_expose_num_recent, mblog_act_num_recent,
            mblog_article_read_num, mblog_miaopai_view_num, mblog_total_read_num, mblog_first_tags, mblog_second_tags,
            mblog_third_tags, mblog_topic_tags, mblog_keyword_tags, mblog_area_tags, mblog_first_max_tag,
            mblog_second_max_tag, mblog_third_max_tag, mblog_topic_max_tag, mblog_keyword_max_tag, mblog_interact_num,
            mblog_inter_act_num_recent, mblog_hot_ret_num, mblog_hot_cmt_num, mblog_hot_like_num, mblog_hot_ret_num_recent,
            mblog_hot_cmt_num_recent, mblog_hot_like_num_recent, mblog_group_expo_num, mblog_group_act_num,
            mblog_group_interact_num, mblog_group_ret_num, mblog_group_cmt_num, mblog_group_like_num,
            mblog_group_expo_recent_num, mblog_group_act_recent_num, mblog_group_interact_recent_num,
            mblog_group_ret_recent_num, mblog_group_cmt_recent_num, mblog_group_like_recent_num, mblog_click_rate,
            mblog_interact_rate, mblog_group_click_rate, mblog_group_interact_rate, mblog_click_pic_num_norm,
            mblog_click_video_num_norm, mblog_click_single_page_num_norm, mblog_click_follow_num_norm,
            mblog_click_article_num_norm, mblog_hot_ret_num_norm, mblog_hot_cmt_num_norm, mblog_hot_like_num_norm,
            mblog_ret_num_norm, mblog_cmt_num_norm, mblog_like_num_norm, mblog_hot_heat, mblog_hot_heat_norm,
            mblog_heat, mblog_heat_norm, mblog_click_pic_num, mblog_click_video_num, mblog_click_single_page_num,
            mblog_click_follow_num, mblog_click_article_num,
            mblog_new_click_num, mblog_click_num_rate, mblog_group_click_num_rate, mblog_click_pic_rate,
            mblog_click_video_rate, mblog_click_single_page_rate, mblog_click_follow_rate, mblog_click_article_rate,
            mblog_hot_ret_rate, mblog_hot_cmt_rate, mblog_hot_like_rate, mblog_group_click_pic_rate,
            mblog_group_click_video_rate, mblog_group_click_single_page_rate, mblog_group_click_follow_rate,
            mblog_group_click_article_rate, mblog_real_expo_num, mblog_real_group_expo_num, mblog_real_click_rate,
            mblog_real_interact_rate, mblog_real_group_click_rate, mblog_real_group_interact_rate, mblog_real_click_pic_rate,
            mblog_real_click_video_rate, mblog_real_click_sing_page_rate, mblog_real_click_follow_rate,
            mblog_real_click_article_rate, mblog_real_ret_rate, mblog_real_cmt_rate, mblog_real_like_rate,
            mblog_real_group_click_pic_rate, mblog_real_group_click_video_rate, mblog_real_group_click_single_page_rate,
            mblog_real_group_click_follow_rate, mblog_real_group_click_article_rate, mblog_real_read_duration,
            mblog_real_read_uv, mblog_read_duration_avg, mblog_real_city_level_expo_num, mblog_real_city_level_act_num,
            mblog_real_city_level_interact_num, mblog_real_city_level_click_rate, mblog_real_city_level_interact_rate,
            mblog_province_group_ctr, mblog_province_group_click, mblog_province_group_expo,

            match_first_tag, match_second_tag, match_third_tag, match_first_tag_v2, match_second_tag_v2, match_third_tag_v2,
            match_first_long_tag, match_second_long_tag, match_third_long_tag, match_first_short_tag, match_second_short_tag,
            match_third_short_tag, match_first_group_ctr, match_second_group_ctr, match_third_group_ctr, match_first_group_v2_ctr,
            match_second_group_v2_ctr, match_third_group_v2_ctr, match_first_group_long_ctr, match_second_group_long_ctr,
            match_third_group_long_ctr, match_first_group_short_ctr, match_second_group_short_ctr, match_third_group_short_ctr,
            match_first_level_inte_weight, match_second_level_inte_weight, match_third_level_inte_weight, match_first_tag_user_value,
            match_second_tag_user_value, match_third_tag_user_Value, match_first_tag_mblog_value, match_second_tag_mblog_value,
            match_third_tag_mblog_value, match_first_tag_user_value_v2, match_second_tag_user_value_v2, match_third_tag_user_Value_v2,
            match_first_tag_mblog_value_v2, match_second_tag_mblog_value_v2, match_third_tag_mblog_value_v2, match_first_tag_user_long_value,
            match_second_tag_user_long_value, match_third_tag_user_long_Value, match_first_tag_mblog_long_value,
            match_second_tag_mblog_long_value, match_third_tag_mblog_long_value, match_first_tag_user_short_value,
            match_second_tag_user_short_value, match_third_tag_user_short_Value, match_first_tag_mblog_short_value,
            match_second_tag_mblog_short_value, match_third_tag_mblog_short_value, match_first_tag_ctrs, match_second_tag_ctrs,
            match_third_tag_ctrs, match_user_author_intimacy,
            is_match_location, is_match_long_interest, is_match_short_interest, is_match_near_interest, is_match_instant_interest
    ;
    "
}

function get_latest_partition() {
    local table_name=$1
    local target_dt=$2
    local dt_max='20180301'
    local parts=(`odpscmd -e "show partitions $table_name"`)
    local parts_len=${#parts[@]}
    if [ $parts_len -le 0 ]; then
        echo "!!! ERROR: empty partitions, $table_name, $target_dt"
        exit
    fi
    local first=`echo ${parts[0]} | awk -F'=' '{print $NF}'`
    local parts_last=`expr ${#parts[@]} - 1`
    local last=`echo ${parts[$parts_last]} | awk -F'=' '{print $NF}'`
    if [ $target_dt -lt $first ]; then
        echo $first
    elif [ $target_dt -gt $last ]; then
        echo $last
    else
        for dt in ${parts[@]};
        do
            dt_r=`echo $dt | awk -F'=' '{print $NF}'`
            if [ $dt_r -gt $target_dt ];
            then
                break
            else
                dt_max=$dt_r
            fi
        done
        echo $dt_max
    fi
}

function gen_sample_data_by_day() {
    start_dt=$1
    cur_dt=$start_dt
    end_dt=$2
    while [ $cur_dt -lt $end_dt ]
    do
        gen_expo_act_data_v3 $cur_dt
        gen_base_feature_data_v3 $cur_dt
        add_base_feature_data_v3_1 $cur_dt
        cur_dt=`date +%Y%m%d -d "$cur_dt 1 days"`
    done
}

function main() {

    s_dt='20180426'
    e_dt='20180501'

    gen_sample_data_by_day $s_dt $e_dt

}

main
