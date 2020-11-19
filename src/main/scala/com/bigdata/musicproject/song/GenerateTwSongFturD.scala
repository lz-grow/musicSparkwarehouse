package com.bigdata.musicproject.song

import com.bigdata.musicproject.common.{ConfigUitls, DateUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 基于客户端歌曲播放表：“TO_MINIK_CLIENT_SONG_PLAY_OPERATE_REQ_D”和歌曲基本信息日全量表 ： “ TW_SONG_BASEINFO_D ”
 * 生成歌曲特征日统计表 ：“TW_SONG_FTUR_D”,主要是按照两张表的歌曲 ID 进行关联，
 * 主要统计出歌曲在当天、7 天、30 天内的点唱信息和点赞信息。
 */
object GenerateTwSongFturD {
  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  val iflocal: Boolean = ConfigUitls.iflocal
  val hivedatabasename: String = ConfigUitls.HIVE_DATABASE_NAME

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("需要传入 时间参数")
      System.exit(1)
    }
    if (iflocal) {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").master("local[4]").enableHiveSupport().getOrCreate()
    } else {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").enableHiveSupport().getOrCreate()
    }

    val date7Before = DateUtils.getDateTime(s"${args(0)}", 7)
    val date30Before = DateUtils.getDateTime(s"${args(0)}", 30)
    println(s"当天前日期:${args(0)}")
    println(s"七天前日期:$date7Before")
    println(s"三十天前日期:$date30Before")
    sparkSession.sql(s"use $hivedatabasename")
    val sing_sameDay: DataFrame = sparkSession.sql(
      s"""
         |SELECT songid,
         |      count(songid) AS sing_cnt,
         |      0 as supp_cnt,
         |      count(DISTINCT UID) AS usr_cnt,
         |      count(DISTINCT order_id) AS ordr_cnt
         |FROM to_minik_client_song_play_operate_req_d
         |WHERE data_dt=${args(0)}
         |GROUP BY songid
         |""".stripMargin)
    //    sing_sameDay.show(false)

    val rct_7_sing: DataFrame = sparkSession.sql(
      s"""
         |SELECT songid,
         |      count(songid) AS rct_7_sing_cnt,
         |      0 as rct_7_supp_cnt,
         |      count(DISTINCT UID) AS rct_7_usr_cnt,
         |      count(DISTINCT order_id) AS rct_7_ordr_cnt
         |FROM to_minik_client_song_play_operate_req_d
         |WHERE data_dt between ${date7Before} and ${args(0)}
         |GROUP BY songid
         |""".stripMargin)

    //每个歌曲，三十天内的播放量，点赞数，用户数，订单数
    val rct_30_sing: DataFrame = sparkSession.sql(
      s"""
         |SELECT songid,
         |      count(songid) AS rct_30_sing_cnt,
         |      0 as rct_30_supp_cnt,
         |      count(DISTINCT UID) AS rct_30_usr_cnt,
         |      count(DISTINCT order_id) AS rct_30_ordr_cnt
         |FROM to_minik_client_song_play_operate_req_d
         |WHERE data_dt between ${date30Before} and ${args(0)}
         |GROUP BY songid
         |""".stripMargin)

    val rct_7_30_top = sparkSession.sql(
      s"""
         |select
         |nbr,
         |max(case when data_dt>=${date7Before} and data_dt<=${args(0)} then sing_cnt end) rct_7_top_sing_cnt,
         |max(case when data_dt>=${date7Before} and data_dt<=${args(0)} then supp_cnt end) rct_7_top_supp_cnt,
         |max(sing_cnt) rct_30_top_sing_cnt,
         |max(supp_cnt) rct_30_top_supp_cnt from tw_song_ftur_d where data_dt between ${date30Before} and ${args(0)}
         |group by nbr
         |""".stripMargin)
    sing_sameDay.createOrReplaceTempView("sing_sameDay")
    rct_7_sing.createOrReplaceTempView("rct_7_sing")
    rct_30_sing.createOrReplaceTempView("rct_30_sing")
    rct_7_30_top.createOrReplaceTempView("rct_7_30_top")

    sparkSession.sql(
      s"""
         |insert overwrite table tw_song_ftur_d partition(data_dt=${args(0)})
         |select
         |a.NBR,
         |a.name,
         |SOURCE,
         |ALBUM,
         |PRDCT,
         |LANG,
         |VIDEO_FORMAT,
         |DUR,
         |SINGER1,
         |SINGER2,
         |SINGER1ID,
         |SINGER2ID,
         |MAC_TIME,
         |nvl(SING_CNT,0),
         |nvl(SUPP_CNT,0),
         |nvl(USR_CNT,0),
         |nvl(ORDR_CNT,0),
         |nvl(RCT_7_SING_CNT,0),
         |nvl(RCT_7_SUPP_CNT,0),
         |nvl(RCT_7_TOP_SING_CNT,0),
         |nvl(RCT_7_TOP_SUPP_CNT,0),
         |nvl(RCT_7_USR_CNT,0),
         |nvl(RCT_7_ORDR_CNT,0),
         |nvl(RCT_30_SING_CNT,0),
         |nvl(RCT_30_SUPP_CNT,0),
         |nvl(RCT_30_TOP_SING_CNT,0),
         |nvl(RCT_30_TOP_SUPP_CNT,0),
         |nvl(RCT_30_USR_CNT,0),
         |nvl(RCT_30_ORDR_CNT,0)
         |from tw_song_baseinfo_d a
         |left join sing_sameDay b on a.nbr=b.songid
         |left join rct_7_sing c on c.songid = b.songid
         |left join rct_30_sing d on d.songid = c.songid
         |left join rct_7_30_top e on e.nbr = d.songid
         |""".stripMargin)
  }
}
