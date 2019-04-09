-- 7天内作业
select * from job where startTime > date_sub(CURRENT_DATE, INTERVAL 7 DAY);

-- weibox分布
select * from cluster where weiboxJson like '%29.68%'
select * from cluster where weiboxJson like '%29.69%'
select * from cluster where weiboxJson like '%29.73%'
select * from cluster where weiboxJson like '%29.74%'
select * from cluster where weiboxJson like '%29.75%'

-- 单次作业
SELECT job_key_count.jobKey
    FROM (
           SELECT
             job_instance.jobKey,
             count(*) AS count
           FROM job_instance
           GROUP BY job_instance.jobKey
         ) job_key_count, job
    WHERE job_key_count.count < 2 AND
          job_key_count.jobKey = job.jobKey
          
# 90天前未被引用的目录;单次作业 且目录不重用
select code_path_count.codePath from (
  SELECT
    codePath,
    count(*) as count
  FROM job
  WHERE jobKey IN (
    -- 15天前所有非定时作业
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
          job.startTime < date_sub(CURRENT_DATE, INTERVAL 90 DAY)
  ) AND jobKey NOT IN (
    select jobKey from crontab
  )
  GROUP BY job.codePath
) as code_path_count where code_path_count.count < 2;

-- 移交作业给指定用户
UPDATE crontab set owner = 'wangyu23' where jobKey IN (
'wangyu23-sample_2-3-35507545613358336', 'zejiang-gen_data_new_feature_offline_visitor_9-3-35519842008537088'
)
UPDATE job set owner = 'wangyu23' where jobKey IN (
'wangyu23-sample_2-3-35507545613358336', 'zejiang-gen_data_new_feature_offline_visitor_9-3-35519842008537088'
);

select * from job where jobKey IN (
'wangyu23-sample_2-3-35507545613358336', 'zejiang-gen_data_new_feature_offline_visitor_9-3-35519842008537088'
);
select * from crontab where jobKey IN ( 'wangyu23-sample_2-3-35507545613358336', 'zejiang-gen_data_new_feature_offline_visitor_9-3-35519842008537088'
);      
