ALTER TABLE weiclient_temp MODIFY id INT(11) AUTO_INCREMENT DEFAULT NULL;
ALTER TABLE weiclient_temp ADD PRIMARY KEY (id);
SELECT c.jobKey, c.cronExp from crontab as c JOIN job on c.jobKey = job.jobKey and job.runHost = '10.77.29.74';

SELECT * FROM weiclient_access_record where accessTime < 20180804 and accessTime > 20180801 ;
select DISTINCT command, count(command) from weiclient_access_record GROUP by command ;
# select distinct usage count by userName .
select userName ,count(userName) from weiclient_access_record
where (
  accessTime < 20180809 and
  accessTime > 20180802 and
  interfaces = '')
GROUP BY userName ;
SELECT * from cluster WHERE id = 40
select jobKey from job where name = 'waic-job-from-weiclient-4b7232b4'
SELECT * from job where clusterId = 57 ORDER BY startTime DESC
select userName ,accessTime,count(userName) from weiclient_access_record
where (
  accessTime < 20180823 and
  accessTime > 20180816 )
GROUP BY userName ;
select c.jobKey from job as j LEFT JOIN crontab as c
on c.jobKey = j.jobKey WHERE c.owner = 'wangyu23';
SELECT * from cluster where physicalCluster = '信息系统部热备集群' or name = 'emr-yarn';
# UPDATE crontab set owner = 'zejiang' where jobKey IN (select jobKey from job
# WHERE owner = 'wangyu223');
select job.jobKey from job as job
where job.jobKey in (select jobKey from crontab as crontab where crontab.owner = 'zejiang')
ORDER BY job.jobKey;
select jobKey from crontab where owner='zejiang' ORDER BY jobKey;
select jobKey from crontab where owner='xuemin5
' ORDER BY jobKey;
select * from job as j, job_instance as ji, cluster as c
where ji.endTime > '20181022' and
  ji.endTime < '20181023' and
  j.owner != 'fuqiang8' and
  j.owner != 'yumei1' and
  j.jobKey = ji.jobKey and
  j.clusterId = c.id and
  c.physicalCluster = '阿里云EMR集群';
select war.userName, tg.userName,tg.groupName from weiclient_access_record war, t_group tg
  where war.userName = tg.userName OR
    war.userName = tg.groupName OR
    war.userName like tg.groupName GROUP BY war.userName;
select userName from t_group tg where groupName = 'weibo_multimedia_live' or groupName = 'weibo_toutiao';
select DISTINCT groupName from t_group;
select j.owner,j.startTime from job j, t_group tg WHERE
  (j.owner = tg.userName OR
    j.owner = tg.groupName) and
  (groupName = 'weibo_multimedia_live' or groupName = 'weibo_toutiao');
select
  -- count(jar.applicationId), count(DISTINCT jar.applicationId),
  job.runHost, jar.jobKey, jar.applicationId from jobKey_applicationId_rel jar,
  job where jar.jobKey = job.jobKey;
select * from job where jobKey = 'haibo11-2048-3-35513453666641408';
# ALTER TABLE weiclient auto_increment=1;'
# INSERT INTO weiclient(version, size, timestamp, importance, url, other)
#   VALUES ('0.5.3.3','1233321','1333333333','3','http://datastrategy.intra.weibo.com/software/dist/tar/weiclient/weiclient-latest.tar','');
select * from project_user where projectId = 72 and role != 'acceptor';
select * from t_group, model WHERE
  model.algorithm = 'FM' and t_group.userName = model.owner
and t_group.groupName = 'weibo_bigdata_push'
select t_group.groupName,count(*) from t_group, model WHERE
  model.algorithm = 'FM' and t_group.userName = model.owner
 group by groupName;

select count(*) from t_group, sample WHERE
  (sample.id LIKE '%FM%' or sample.id like '%fm%')
                    and t_group.userName = sample.owner
 group by groupName

select * from job where job.jobUrl LIKE '%application_1531393812627_187141%'
select jobKey,owner,runHost, startTime, status, jobUrl, logUrl from job where clusterId = 3
and jobUrl LIKE '%application_1531393812627_187%'
select crontab.jobKey, job.codePath from crontab, job where crontab.jobKey = job.jobKey
select job.jobKey, job.startTime
    , jobKey_applicationId_rel.applicationId
from job, jobKey_applicationId_rel
where job.jobKey = jobKey_applicationId_rel.jobKey and job.clusterId != '3';
and jobKey_applicationId_rel.applicationId != 'NULL_YARN_APPLICATION_ID'
SELECT * from sample where owner = 'enzhao'
select t_group.groupName, model.owner from model,t_group where model.owner = t_group.userName
GROUP BY model.owner


-- weibox分布
select * from cluster where weiboxJson like '%29.68%'
select * from cluster where weiboxJson like '%29.69%'
select * from cluster where weiboxJson like '%29.73%'
select * from cluster where weiboxJson like '%29.74%'
select * from cluster where weiboxJson like '%29.75%'
-- 单次作业
select jobKey, startTime, codePath, count from (
  select job.jobKey, job.startTime, job.codePath,  count(*) as count
from job, job_instance
where job_instance.jobKey = job.jobKey
GROUP BY job_instance.jobKey)t1 where t1.count<2 and startTime < '20190101'
-- 7天内作业
select * from job where startTime > date_sub(CURRENT_DATE, INTERVAL 7 DAY);

-- 单次作业 且目录不重用
select t1.jobKey, t1.startTime, t1.codePath, t1.count, t2.count from (
  select job.jobKey, job.startTime, job.codePath,  count(*) as count
from job, job_instance
where job_instance.jobKey = job.jobKey
GROUP BY job_instance.jobKey)t1,
(
  select job.owner, job.jobKey, job.startTime, job.codePath,  count(*) as count
from job
GROUP BY job.codePath
)t2
where t1.jobKey = t2.jobKey AND
      t1.count<2 AND t2.count < 2 AND
      t1.startTime < date_sub(CURRENT_DATE, INTERVAL 30 DAY);
t2.owner = 'yueyao' ;

select * from job_instance where jobKey = 'zhangying8-10-35502496476085760'
select * from job where owner='enzhao' and jobKey like '%stream%'
select * from crontab where owner = 'zejiang'
select cronExp, jobKey, owner from crontab
select * from job where codePath = 'zhangying8-storm-storm-hello_world-1517215310940996'
select * from cluster where name='计算平台测试集群-1'
select auth.userName, auth.moduleId, module.moduleName from auth,module where
  auth.moduleId = module.id AND
  userName like 'yuxiang8'
select * from module
select * from t_group,auth where t_group.userName = auth.userName AND
  t_group.userName like '%shaojie%'

select t1.jobKey, t1.startTime, t1.codePath, t1.count, t2.count from
(
  select job.jobKey, job.startTime, job.codePath,  count(*) as count
    from job, job_instance
    where job_instance.jobKey = job.jobKey
    GROUP BY job.jobKey, job.startTime, job.codePath
)t1,
(
  select job.owner, job.jobKey, job.startTime, job.codePath,  count(*) as count
    from job
    GROUP BY job.owner, job.jobKey, job.startTime, job.codePath
)t2
where t1.jobKey = t2.jobKey AND
      t1.count<2 AND t2.count < 2 AND
      t1.startTime < date_sub(CURRENT_DATE, INTERVAL 15 DAY);

select * from job_instance where jobKey = 'enzhao-emr-cluster-quartz-submit-001-3-35506235409988608'
ORDER BY startTime DESC

# 90天前未被引用的目录
select code_path_count.codePath from (
  SELECT
    codePath,
    count(*) as count
  FROM job
  WHERE jobKey IN (
    -- 90天后15天前所有非定时作业
    SELECT job_key_count.jobKey
    FROM (
           SELECT
             job_instance.jobKey,
             count(*) AS count
           FROM job_instance
           GROUP BY job_instance.jobKey
         ) job_key_count, job
    WHERE job_key_count.count < 2 AND
          job_key_count.jobKey = job.jobKey AND
          job.startTime > date_sub(CURRENT_DATE, INTERVAL 90 DAY) AND
          job.startTime < date_sub(CURRENT_DATE, INTERVAL 15 DAY)
  ) AND jobKey NOT IN (
    select jobKey from crontab
  )
  GROUP BY job.codePath
) as code_path_count where code_path_count.count < 2;

  select * from job where job.codePath like '%zhihao7-core-submit-core-dispatch-hb_materials_new_tag%'

  select codePath from job where job.codePath like '%haibo11-core-submit-core-dispatch-15%'
  AND job.codePath IN (
    select code_path_count.codePath from (
  SELECT
    codePath,
    count(*) as count
  FROM job
  WHERE jobKey IN (
    -- 90天后15天前所有非定时作业
    SELECT job_key_count.jobKey
    FROM (
           SELECT
             job_instance.jobKey,
             count(*) AS count
           FROM job_instance
           GROUP BY job_instance.jobKey
         ) job_key_count, job
    WHERE job_key_count.count < 2 AND
          job_key_count.jobKey = job.jobKey AND
          job.startTime > date_sub(CURRENT_DATE, INTERVAL 90 DAY) AND
          job.startTime < date_sub(CURRENT_DATE, INTERVAL 15 DAY)
  ) AND jobKey NOT IN (
    select jobKey from crontab
  )
  GROUP BY job.codePath
) as code_path_count where code_path_count.count < 2
  )

(SELECT *
 FROM crontab
   LEFT JOIN job ON crontab.jobKey = job.jobKey
 WHERE job.codePath = 'haibo11-core-submit-core-dispatch-1550733787633-1550733787979956';
)
select job.jobKey, job.codePath FROM crontab left join job on job.jobKey = crontab.jobKey ORDER BY codePath;where job.codePath != 'null'
select job.codePath, jobKey from job where codePath = 'gaolin3-core-submit-core-dispatch-waic-job-from-weiclient-802f674b-1552477359441973'
SELECT job.name from job where job.codePath IN (
  'zhihao7-core-submit-core-dispatch-hb_materials_keyword_embedding_20190318095501-1552874101410154'
)
-- 移交作业给指定用户
UPDATE crontab set owner = 'lichao17' where jobKey IN (
)
UPDATE job,crontab set
  job.owner = 'lichao17',
  crontab.owner = 'lichao17'
where
  job.jobKey = crontab.jobKey
  and job.jobKey IN (
          'wangyu23-gen_data_new_feature_offline_user_develop_0-3-35509124855561216',
'wangyu23-gen_data_new_feature_offline_user_develop_1-3-35509124855743232',
'wangyu23-gen_data_new_feature_offline_user_develop_2-3-35509124855952384',
'wangyu23-gen_data_new_feature_offline_user_develop_3-3-35509124856071424',
'wangyu23-gen_data_new_feature_offline_user_develop_4-3-35509124856186880',
'wangyu23-gen_data_new_feature_offline_user_develop_5-3-35509124856423680',
'wangyu23-gen_data_new_feature_offline_user_develop_6-3-35509124856554240',
'wangyu23-gen_data_new_feature_offline_user_develop_7-3-35509124856658432',
'wangyu23-gen_data_new_feature_offline_user_develop_8-3-35509124856770048',
'wangyu23-gen_data_new_feature_offline_user_develop_9-3-35509124856888576'

);

select * from job where jobKey IN (
  'wangyu23-gen_data_new_feature_offline_user_develop_0-3-35509124855561216',
  'wangyu23-gen_data_new_feature_offline_user_develop_1-3-35509124855743232',
  'wangyu23-gen_data_new_feature_offline_user_develop_2-3-35509124855952384',
  'wangyu23-gen_data_new_feature_offline_user_develop_3-3-35509124856071424',
  'wangyu23-gen_data_new_feature_offline_user_develop_4-3-35509124856186880',
  'wangyu23-gen_data_new_feature_offline_user_develop_5-3-35509124856423680',
  'wangyu23-gen_data_new_feature_offline_user_develop_6-3-35509124856554240',
  'wangyu23-gen_data_new_feature_offline_user_develop_7-3-35509124856658432',
  'wangyu23-gen_data_new_feature_offline_user_develop_8-3-35509124856770048',
  'wangyu23-gen_data_new_feature_offline_user_develop_9-3-35509124856888576'
);
select * from crontab where jobKey IN (
    'wangyu23-gen_data_new_feature_offline_user_develop_0-3-35509124855561216',
  'wangyu23-gen_data_new_feature_offline_user_develop_1-3-35509124855743232',
  'wangyu23-gen_data_new_feature_offline_user_develop_2-3-35509124855952384',
  'wangyu23-gen_data_new_feature_offline_user_develop_3-3-35509124856071424',
  'wangyu23-gen_data_new_feature_offline_user_develop_4-3-35509124856186880',
  'wangyu23-gen_data_new_feature_offline_user_develop_5-3-35509124856423680',
  'wangyu23-gen_data_new_feature_offline_user_develop_6-3-35509124856554240',
  'wangyu23-gen_data_new_feature_offline_user_develop_7-3-35509124856658432',
  'wangyu23-gen_data_new_feature_offline_user_develop_8-3-35509124856770048',
  'wangyu23-gen_data_new_feature_offline_user_develop_9-3-35509124856888576'

);


(
  SELECT
    crontab.cronExp,
    job.jobKey
  FROM crontab
    LEFT JOIN job ON crontab.jobKey = job.jobKey
  WHERE job.codePath LIKE '%feichao-core-submit-core-dispatch%'
)

select * from cluster where cluster.name like '%ml-weibo%';

select * from (SELECT job.codePath
 FROM job
 WHERE job.owner IN ('zhihao7')
       AND job.startTime < '20190330'
  AND job.startTime > '20190323'
)c ;

where c.codePath = 'zhihao7-core-submit-core-dispatch-hb_materials_20190330164601-1553935561970256'

