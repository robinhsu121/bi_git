#! /usr/bin/env pyspark
# -*- coding: utf-8 -*-
# endcoding:utf-8

#edit:2019-04-02
#update:2019-04-17

""" 应用安装报表：app_install_report"""
######################################################################################################################

##从日志里解析数据，并存入hive数据库

##load libraries

from __future__ import absolute_import,division,print_function
import pyspark
from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
#import numpy as np
#import pandas as pd
import os
import datetime
import getopt
import argparse
import commands
import subprocess

##解决因为编码，导致写入数据报错(ERROR - failed to write data to stream: <open file '<stdout>', mode 'w' at)
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

######################################################################################################################

""" spark环境具体配置参数  """

spark = SparkSession.builder.master("spark://master:7077").appName("app_install_report").enableHiveSupport().getOrCreate()

spark.conf.set("spark.master","spark://master:7077")
spark.conf.set("spark.default.parallelism",190) ##发生shuffle时的并行度，默认是核数，太大导致GC，太小执行速度慢
#spark.conf.set("spark.sql.shuffle.partitions",240) ##发生聚合操作的并行度，默认是200，太小容易导致OOM,executor丢失，任务执行时间过长,太大会导致保存的小文件过多，默认是200个小文件
#spark.conf.set("spark.sql.result.partitions",20)  ####最后的执行计划中加入一个repartition transformation。通过参数控制最终的partitions数且不影响shuffle partition的数量,减少小文件的个数
spark.conf.set("spark.executor.memory","3g")
spark.conf.set("spark.executor.cores",3)
spark.conf.set("spark.cores.max",72)
spark.conf.set("spark.driver.memory","3g")
spark.conf.set("spark.sql.execution.arrow.enabled","true")  ##spark df & pandas df性能优化，需开启
#spark.conf.set("spark.driver.maxResultSize","3g")  #一般是spark默认会限定内存，可以使用以下的方式提高
spark.conf.set("spark.yarn.executor.memoryOverhead",2048)
spark.conf.set("spark.core.connection.ack.wait.timeout",300)
spark.conf.set("spark.speculation","true")
spark.conf.set("spark.debug.maxToStringFields",500)
spark.conf.set("spark.rdd.compress","true")
spark.conf.set("spark.sql.codegen","true")
#spark.conf.set("spark.storage.memoryFraction",0.6) #spark.executor.memory内存资源分为两部分，一部分用于缓存，缓存比例是0.6;另一部分用于任务计算，计算资源为spark.executor.memory*0.4
#spark.conf.set("")  ###




##设定日期
#today = datetime.date(2019,04,8)
today = datetime.date.today()
day_before_0 = today - datetime.timedelta(days=1)  # 昨天
day_before_1 = day_before_0 - datetime.timedelta(days=1)  # 昨天前1天
day_before_7 = day_before_0 - datetime.timedelta(days=7)  # 昨天前7天
#日期转化为字符串
str_dt_0 = datetime.datetime.strftime(day_before_0, '%Y-%m-%d')
str_dt_1 = datetime.datetime.strftime(day_before_1, '%Y-%m-%d')
str_dt_7 = datetime.datetime.strftime(day_before_7, '%Y-%m-%d')





##从cms数据库拉取数据,并连接数据
def cms_to_hdfs():
    """ 将数据mysql导入hdfs"""
    table_name = """ (select fsk_pid,fsk_title,fsk_catalog from fsk.fsk_app_list union all select fsk_pid,fsk_title,fsk_catalog from fsk.fsk_game_list) as fsk_app_list """
    url = "jdbc:mysql://196.168.100.89:3306/fsk?user=tvad&password=tvad_12345"
    jdbc_df = spark.read.format('jdbc').options(
              url=url,
              driver='com.mysql.jdbc.Driver',
              dbtable=table_name).load()
    return jdbc_df





##解析 install_fin 日志函数
def parse_install_fin():
    """解析install_fin日志数据"""
    ##json日志文件的路径
    json_path = "hdfs://master:9000/data/{0}/*/*.gz".format(str_dt_0)
    ##判断路径文件条件
    cmd = "hadoop fs -ls -R /data/{0} | egrep '.gz$' | wc -l" .format(str_dt_0)
    if_zero = subprocess.check_output(cmd, shell=True).strip().split('\n')[0]
    ##判断日志文件路径是否存在
    if int(if_zero) == 0:
        print("the video_play_count_logs does not exists!")
        raise SystemExit(123)
    else:
        #json日志数据路径,并解析
        df_stat=spark.read.json(json_path).select('custom_uuid','account','rectime','device_name','vercode','vername','site',F.explode('data.install_fin').alias('install_fin')).filter(F.col("install_fin.time").isNotNull()).select(['custom_uuid','account',F.to_date(F.from_unixtime(F.col('rectime')/1000)).cast('date').alias("date"),F.to_date(F.from_unixtime(F.col('rectime')/1000)).cast('string').alias('dt'),'device_name','vercode','vername',F.when(F.col('site') == 'ALI','youku').when(F.col('site') == 'IQIYI','iqiyi').when(F.col('site') == 'BESTV','bestv').otherwise('others').alias('site'),F.col('install_fin.package_id').alias('package_id'),F.col('install_fin.title').alias('title')])
        ##把数据插入hive动态分区表中
        spark.sql("set hive.exec.dynamic.partition=true")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("show databases")
        spark.sql("use sharp")
        df_stat.write.format("orc").mode("append").partitionBy("dt").saveAsTable("sharp.install_fin")





##HDFS:install_fin与CMS数据库连接
def hdfs_join_cms(cms_df):
    """ 解析install_fin日志内容与cms数据库连接 """
    ##hdfs_df
    sql = """ 
    select t0.custom_uuid,t0.date,t0.package_id,t0.title,t0.site from sharp.install_fin t0 where t0.dt="{date_0}" 
    union all
    select t1.custom_uuid,t1.date,t1.package_id,t1.title,t1.site from sharp.install_fin t1 where t1.dt="{date_1}"
    union all
    select t7.custom_uuid,t7.date,t7.package_id,t7.title,t7.site from sharp.install_fin t7 where t7.dt="{date_7}" """ .format(date_0=str_dt_0,date_1=str_dt_1,date_7=str_dt_7)
    spark.sql("show databases")
    spark.sql("use sharp")
    hdfs_df = spark.sql(sql)
    ##与CMS数据库应用&游戏数据连接
    condition_0_1=(F.coalesce(F.col("t_0.package_id"),F.lit("123")) == F.coalesce(F.col("t_1.fsk_pid"),F.lit("123")))
    df = hdfs_df.alias("t_0").join(cms_df.alias("t_1"),condition_0_1,"left_outer") \
                             .select(F.col("t_0.custom_uuid").alias("custom_uuid"),F.col("t_0.date").alias("date"),F.col("t_0.site").alias("site"),F.col("t_0.package_id").alias("package_id"), \
                                F.col("t_1.fsk_title").alias("title"),F.col("t_1.fsk_catalog").alias("fsk_catalog"))
    return df





##应用安装报表函数
def app_install(df):
    """ 应用安装报表 """
    #分析数据
    df.createOrReplaceTempView("v_df")
    sql_0 = """select package_id,title,site,fsk_catalog,grouping_id() id_1,count(custom_uuid) playNum,count(distinct custom_uuid) users,round(count(custom_uuid)/count(distinct custom_uuid),2) avgPlayNum from v_df where date="{date_0}" group by package_id,title,site,fsk_catalog  grouping sets((package_id,title,site,fsk_catalog),(site,fsk_catalog),(site),(fsk_catalog),()) """.format(date_0=str_dt_0)
    sql_1 = """select package_id,title,site,fsk_catalog,grouping_id() id_1,count(custom_uuid) playNum,count(distinct custom_uuid) users,round(count(custom_uuid)/count(distinct custom_uuid),2) avgPlayNum from v_df where date="{date_1}" group by package_id,title,site,fsk_catalog  grouping sets((package_id,title,site,fsk_catalog),(site,fsk_catalog),(site),(fsk_catalog),()) """.format(date_1=str_dt_1)
    sql_7 = """select package_id,title,site,fsk_catalog,grouping_id() id_1,count(custom_uuid) playNum,count(distinct custom_uuid) users,round(count(custom_uuid)/count(distinct custom_uuid),2) avgPlayNum from v_df where date="{date_7}" group by package_id,title,site,fsk_catalog  grouping sets((package_id,title,site,fsk_catalog),(site,fsk_catalog),(site),(fsk_catalog),()) """.format(date_7=str_dt_7)
    spark.sql("show databases")
    spark.sql("use sharp")
    df_cube_0 = spark.sql(sql_0)
    df_cube_1 = spark.sql(sql_1)
    df_cube_7 = spark.sql(sql_7)

    ##天环比、周同比连接条件
    condition_0=(F.coalesce(F.col("t_0.package_id"),F.lit("123")) == F.coalesce(F.col("t_1.package_id"),F.lit("123")))
    condition_1=(F.coalesce(F.col("t_0.title"),F.lit("123")) == F.coalesce(F.col("t_1.title"),F.lit("123")))          
    condition_2=(F.coalesce(F.col("t_0.site"),F.lit("123")) == F.coalesce(F.col("t_1.site"),F.lit("123")))
    condition_3=(F.coalesce(F.col("t_0.fsk_catalog"),F.lit("123")) == F.coalesce(F.col("t_1.fsk_catalog"),F.lit("123")))
    condition_4=(F.col("t_0.id_1") == F.col("t_1.id_1")) 
    condition_5=(F.coalesce(F.col("t_0.package_id"),F.lit("123")) == F.coalesce(F.col("t_7.package_id"),F.lit("123")))
    condition_6=(F.coalesce(F.col("t_0.title"),F.lit("123")) == F.coalesce(F.col("t_7.title"),F.lit("123")))          
    condition_7=(F.coalesce(F.col("t_0.site"),F.lit("123")) == F.coalesce(F.col("t_7.site"),F.lit("123")))
    condition_8=(F.coalesce(F.col("t_0.fsk_catalog"),F.lit("123")) == F.coalesce(F.col("t_7.fsk_catalog"),F.lit("123")))
    condition_9=(F.col("t_0.id_1") == F.col("t_7.id_1"))

    ##天环比连接条件
    conditions_0_1 = condition_0 & condition_1 & condition_2 & condition_3 & condition_4
    ##周同比连接条件
    conditions_0_7 = condition_5 & condition_6 & condition_7 & condition_8 & condition_9

    ##最终报表
    app_report = df_cube_0.alias("t_0").join(df_cube_1.alias("t_1"),conditions_0_1,"left_outer") \
                                       .join(df_cube_7.alias("t_7"),conditions_0_7,"left_outer") \
                                       .select(F.regexp_replace(F.lit(str_dt_0),"-","").cast("int").alias("date"),F.col("t_0.package_id").alias("appId"),F.col("t_0.title").alias("appName"),F.col("t_0.site").alias("channelName"),F.col("t_0.fsk_catalog").alias("typeName"),F.col("t_0.id_1").alias("id_1"), \
                                             F.col("t_0.playNum").alias("totalPlayCount"),F.concat(F.round((F.col("t_0.playNum")/F.col("t_1.playNum")-1)*100,2),F.lit("%")).alias("playCountCompareDay"),F.concat(F.round((F.col("t_0.playNum")/F.col("t_7.playNum")-1)*100,2),F.lit("%")).alias("playCountCompareWeek"), \
                                             F.col("t_0.users").alias("totalUserCount"),F.concat(F.round((F.col("t_0.users")/F.col("t_1.users")-1)*100,2),F.lit("%")).alias("userCountCompareDay"),F.concat(F.round((F.col("t_0.users")/F.col("t_7.users")-1)*100,2),F.lit("%")).alias("userCountCompareWeek"), \
                                             F.col("t_0.avgPlayNum").alias("averagePlayCount"),F.concat(F.round((F.col("t_0.avgPlayNum")/F.col("t_1.avgPlayNum")-1)*100,2),F.lit("%")).alias("avgPlayCountCompareDay"),F.concat(F.round((F.col("t_0.avgPlayNum")/F.col("t_7.avgPlayNum")-1)*100,2),F.lit("%")).alias("avgPlayCountCompareWeek"))
    
    return app_report




##应用安装漏斗函数
def funnel_statistic_report():
    """解析 appstore_onclick\game_onclick(详情页)、download_onclick(点击下载)、download_fin(下载完成)、install_fin(安装完成) 日志数据"""
    ##json日志文件的路径
    json_path = "hdfs://master:9000/data/{0}/*/*.gz".format(str_dt_0)
    ##判断路径文件条件
    cmd = "hadoop fs -ls -R /data/{0} | egrep '.gz$' | wc -l" .format(str_dt_0)
    if_zero = subprocess.check_output(cmd, shell=True).strip().split('\n')[0]
    ##判断日志文件路径是否存在
    if int(if_zero) == 0:
        print("the logs does not exists!")
        raise SystemExit(123)
    else:
        #json日志数据路径,并解析
        #df_appstore_onclick=spark.read.json(json_path).select('custom_uuid','rectime',F.explode('data.appstore_onclick').alias('appstore_onclick')).filter(F.col("appstore_onclick.time").isNotNull()).select(['custom_uuid',F.regexp_replace(F.lit(str_dt_0),"-","").cast("int").alias("date"),F.lit("appstore_game_onclick").alias("featureName")])
        #df_game_onclick=spark.read.json(json_path).select('custom_uuid','rectime',F.explode('data.game_onclick').alias('game_onclick')).filter(F.col("game_onclick.time").isNotNull()).select(['custom_uuid',F.regexp_replace(F.lit(str_dt_0),"-","").cast("int").alias("date"),F.lit("appstore_game_onclick").alias("featureName")])
        #df_appstore_game = df_appstore_onclick.unionAll(df_game_onclick).groupBy(F.col('date'),F.col('featureName')).agg(F.count('custom_uuid').alias('totalPlayCount'))
        df_download_onclick=spark.read.json(json_path).select('custom_uuid','rectime',F.explode('data.download_onclick').alias('download_onclick')).filter(F.col("download_onclick.time").isNotNull()).select(['custom_uuid',F.regexp_replace(F.lit(str_dt_0),"-","").cast("int").alias("date"),F.lit('download_onclick').alias('featureName')]).groupBy(F.col('date'),F.col('featureName')).agg(F.count('custom_uuid').alias('totalPlayCount'))
        sql_download_fin=""" select cast(regexp_replace(date,'-','') as int) as date,'download_fin' as featureName,count(custom_uuid) as totalPlayCount from sharp.download_fin where dt='{date_0}' group by cast(regexp_replace(date,'-','') as int),'download_fin' """ .format(date_0=str_dt_0)
        sql_install_fin=""" select cast(regexp_replace(date,'-','') as int) as date,'install_fin' as featureName,count(custom_uuid) as totalPlayCount  from sharp.install_fin where dt='{date_0}' group by cast(regexp_replace(date,'-','') as int),'install_fin' """ .format(date_0=str_dt_0)
        spark.sql("show databases")
        spark.sql("use sharp")
        df_download_fin=spark.sql(sql_download_fin)
        df_install_fin=spark.sql(sql_install_fin)
        ##最终报表
        #funnel_report = df_appstore_game.unionAll(df_download_onclick).unionAll(df_download_fin).unionAll(df_install_fin)
        funnel_report = df_download_onclick.unionAll(df_download_fin).unionAll(df_install_fin)
        return funnel_report



##应用安装漏斗函数_改进(2019-04-22)
def funnel_statistic_report_(cms_df):
    """解析 appstore_onclick\game_onclick(详情页)、download_onclick(点击下载)、download_fin(下载完成)、install_fin(安装完成) 日志数据"""
    ##json日志文件的路径
    json_path = "hdfs://master:9000/data/{0}/*/*.gz".format(str_dt_0)
    ##判断路径文件条件
    cmd = "hadoop fs -ls -R /data/{0} | egrep '.gz$' | wc -l" .format(str_dt_0)
    if_zero = subprocess.check_output(cmd, shell=True).strip().split('\n')[0]
    ##判断日志文件路径是否存在
    if int(if_zero) == 0:
        print("the logs does not exists!")
        raise SystemExit(123)
    else:
        #json日志数据路径,并解析
        ##与cms_df连接条件
        condition_0_1=(F.coalesce(F.col("t_0.package_id"),F.lit("123")) == F.coalesce(F.col("t_1.fsk_pid"),F.lit("123")))
        ##与cms_df进行左连接，得到应用类别
        df_download_onclick=spark.read.json(json_path).select('custom_uuid','rectime',F.when(F.col('site') == 'ALI','youku').when(F.col('site') == 'IQIYI','iqiyi').when(F.col('site') == 'BESTV','bestv').otherwise('others').alias('site'),F.explode('data.download_onclick').alias('download_onclick')).filter(F.col("download_onclick.time").isNotNull()).select(['custom_uuid',F.regexp_replace(F.lit(str_dt_0),"-","").cast("int").alias("date"),F.lit('download_onclick').alias('featureName'),F.col("site").alias("site"),F.col("download_onclick.package_id").alias("package_id")]).alias("t_0").join(cms_df.alias("t_1"),condition_0_1,"left_outer").select(F.col("t_0.custom_uuid").alias("custom_uuid"),F.col("t_0.date").alias("date"),F.col("t_0.featureName").alias("featureName"),F.col("t_0.site").alias("site"),F.col("t_0.package_id").alias("package_id"),F.col("t_1.fsk_title").alias("title"),F.col("t_1.fsk_catalog").alias("fsk_catalog")) 
        sql_download_fin=""" select custom_uuid,cast(regexp_replace(date,'-','') as int) as date,site,package_id from sharp.download_fin where dt='{date_0}'  """ .format(date_0=str_dt_0)
        sql_install_fin=""" select custom_uuid,cast(regexp_replace(date,'-','') as int) as date,site,package_id from sharp.install_fin where dt='{date_0}' """ .format(date_0=str_dt_0)
        spark.sql("show databases")
        spark.sql("use sharp")
        df_download_fin=spark.sql(sql_download_fin).alias("t_0").join(cms_df.alias("t_1"),condition_0_1,"left_outer").select(F.col("t_0.custom_uuid").alias("custom_uuid"),F.col("t_0.date").alias("date"),F.lit('download_fin').alias('featureName'),F.col("t_0.site").alias("site"),F.col("t_0.package_id").alias("package_id"),F.col("t_1.fsk_title").alias("title"),F.col("t_1.fsk_catalog").alias("fsk_catalog"))
        df_install_fin=spark.sql(sql_install_fin).alias("t_0").join(cms_df.alias("t_1"),condition_0_1,"left_outer").select(F.col("t_0.custom_uuid").alias("custom_uuid"),F.col("t_0.date").alias("date"),F.lit('install_fin').alias('featureName'),F.col("t_0.site").alias("site"),F.col("t_0.package_id").alias("package_id"),F.col("t_1.fsk_title").alias("title"),F.col("t_1.fsk_catalog").alias("fsk_catalog")) 
        ##聚合操作
        df = df_download_onclick.unionAll(df_download_fin).unionAll(df_install_fin)
        df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        df.createOrReplaceTempView("v_df")
        sql_df_1 = """select date,featureName,site channelName,fsk_catalog typeName,grouping_id() id_1,count(custom_uuid) totalPlayCount from v_df group by date,featureName,site,fsk_catalog grouping sets((date,featureName,site,fsk_catalog),(date,featureName,site),(date,featureName,fsk_catalog),(date,featureName)) """
        sql_df_2 = """ select date,package_id appId,title appName,featureName,count(custom_uuid) totalPlayCount  from v_df group by date,package_id,title,featureName """
        spark.sql("show databases")
        spark.sql("use sharp")
        funnel_report_1 = spark.sql(sql_df_1)
        funnel_report_2 = spark.sql(sql_df_2)
        df.unpersist()
        ##最终报表
        return funnel_report_1,funnel_report_2


##写入mysql函数
def hdfs_to_mysql(src_df,table_name,mode_type='append'):
    """ 将数据src_df从HDFS写入MySQL表table_name """
    src_df.write.format('jdbc').options(
          url='jdbc:mysql://196.168.100.88:3306/sharpbi',
          driver='com.mysql.jdbc.Driver',
          dbtable=table_name,
          user='biadmin',
          password='bi_12345').mode(mode_type).save()



def main():
    """ app_install_report """
    import gc
    from time import sleep

    ##解析install_fin日志
    parse_install_fin()
    ##从CMS导入数据
    jdbc_df = cms_to_hdfs()
    ##HDFS与CMS数据库连接
    df_stat = hdfs_join_cms(jdbc_df)
    #应用安装报表并写入mysql
    app_report = app_install(df_stat)
    hdfs_to_mysql(app_report,"app_install_report","append")
    del app_report
    gc.collect()
    sleep(5)
    ##应用安装漏斗图
    funnel_report = funnel_statistic_report()
    hdfs_to_mysql(funnel_report,"funnel_app_install_report","append")
    del funnel_report
    gc.collect()
    sleep(5)
    ##应用安装漏斗图_改进
    funnel_report_1,funnel_report_2 = funnel_statistic_report_(jdbc_df)
    #宏观漏斗图
    hdfs_to_mysql(funnel_report_1,"funnel_app_install_report_macro","append")
    #微观漏斗图
    hdfs_to_mysql(funnel_report_2,"funnel_app_install_report_micro","append")
    del funnel_report_1,funnel_report_2
    

	
if  __name__ == "__main__":
    """ app_install_report 报表入口 """
    import time
    begin_time = time.time()
    ###############################
    main()
    ##############################
    end_time=time.time()
    cost_time=end_time-begin_time
    ##查看程序运行时间
    print("="*25 + "*"*10 + "="*25 + "\n")
    print("程序耗时 {:.2f} 秒" .format(cost_time))
    print("="*25 + "*"*10 + "="*25 + "\n")

    #停止stop
    spark.stop()
