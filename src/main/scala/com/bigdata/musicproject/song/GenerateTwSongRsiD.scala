package com.bigdata.musicproject.song

import java.util.Properties

import com.bigdata.musicproject.common.{ConfigUitls, DateUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 这里统计歌手和歌曲热度都是根据歌曲特征日统计表：“TW_SONG_FTUR_D计算得到，
 * 主要借助了微信指数来统计对应的歌曲和歌手的热度，统计了当天、近7天、近30天
 * 每个歌手和歌曲的热度信息存放在对应的层的歌手影响力指数日统计表“TW_SINGER_RSI_D”
 * 和歌曲影响力指数日统计表“TW_SONG_RSI_D”。
 */
object GenerateTwSongRsiD {
  /**
   * 01. 首先观察TW_SONG_RSI_D，查看需要哪些字段，发现字段RSI（近期歌曲热度） 需要多个字段结合计算得出。
   * 02.
   */
  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  val iflocal: Boolean = ConfigUitls.iflocal
  val hivedatabasename: String = ConfigUitls.HIVE_DATABASE_NAME

  /**
   *
   ** @param totalSing    歌曲的总点唱数
   ** @param day          天
   ** @param totalSupport 歌曲总点赞数
   ** @param topSingHits  最高点唱数
   ** @param topSupport   最高点赞数
   ** @return RSI 歌曲的热度
   */
  val getRsi: (Int, Int, Int, Int, Int) => String = (totalSing: Int, totalSupport: Int, day: Int, topSingHits, topSupport: Int) => {
    if (day == 1) {
      (Math.pow(Math.log(totalSing / day + 1) * 0.63 * 0.8 + Math.log(topSupport / day + 1) * 0.63 * 0.2, 2) * 10).toString
    }
    (Math.pow(
      (Math.log(totalSing / day + 1) * 0.63 + 0.37 * Math.log(topSingHits / day + 1)) * 0.8 +
        (Math.log(totalSupport / day + 1) * 0.63 + 0.37 * Math.log(topSupport / day + 1)) * 0.2
      , 2) * 10).toString
  }

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

    sparkSession.udf.register("getRsi", getRsi)
    val per7 = DateUtils.getDateTime(args(0), 7)
    val per30 = DateUtils.getDateTime(args(0), 30)

    //    从 TW_SONG_FTUR_D 表中，获取 我们需要的字段
    sparkSession.sql(s"use ${hivedatabasename}")
    sparkSession.table("TW_SONG_FTUR_D").select(
      "NBR",
      "NAME",
      "SING_CNT",
      "SUPP_CNT",
      "RCT_7_SING_CNT",
      "RCT_7_SUPP_CNT",
      "RCT_7_TOP_SING_CNT",
      "RCT_7_TOP_SUPP_CNT",
      "RCT_30_SING_CNT",
      "RCT_30_SUPP_CNT",
      "RCT_30_TOP_SING_CNT",
      "RCT_30_TOP_SUPP_CNT",
      "DATA_DT"
    )
      .where(s"data_dt>=${per30} and data_dt<=${args(0)}")
      //      .show()
      .createOrReplaceTempView("TW_SONG_FTUR_D_TMP")

    val dataFrame = sparkSession.sql(
      s"""
         |select
         |"1" period,
         |nbr,
         |name,
         |getRsi(SING_CNT,SUPP_CNT,1,0,0) rsi
         |from TW_SONG_FTUR_D_TMP where data_dt=${args(0)}
         |""".stripMargin)

    //    创建临时视图后，所有字段变为string，所以想要排序，需要类型转换
    dataFrame.createGlobalTempView("TW_SONG_FTUR_D_TMP_1")
    val frame1 = sparkSession.sql(
      s"""
         |select
         |period,
         |nbr,
         |name,
         |cast(rsi as double),
         |row_number() over(order by cast(rsi as double) desc) rsi_rank
         |from
         |global_temp.TW_SONG_FTUR_D_TMP_1
         |""".stripMargin)
    //      .show(false)

    val dataFrame7 = sparkSession.sql(
      s"""
         |select
         |"7" period,
         |nbr,
         |name,
         |getRsi(SING_CNT,SUPP_CNT,1,0,0) rsi
         |from TW_SONG_FTUR_D_TMP where data_dt>=${per7} and data_dt<=${args(0)}
         |""".stripMargin)

    //    创建临时视图后，所有字段变为string，所以想要排序，需要类型转换
    dataFrame7.createGlobalTempView("TW_SONG_FTUR_D_TMP_7")
    val frame7 = sparkSession.sql(
      s"""
         |select
         |period,
         |nbr,
         |name,
         |cast(rsi as double),
         |row_number() over(order by cast(rsi as double) desc) rsi_rank
         |from
         |global_temp.TW_SONG_FTUR_D_TMP_7
         |""".stripMargin)
    //      .show(false)

    val dataFrame30 = sparkSession.sql(
      s"""
         |select
         |"30" period,
         |nbr,
         |name,
         |getRsi(SING_CNT,SUPP_CNT,1,0,0) rsi
         |from TW_SONG_FTUR_D_TMP where data_dt>=${per30} and data_dt<=${args(0)}
         |""".stripMargin)

    //    创建临时视图后，所有字段变为string，所以想要排序，需要类型转换
    dataFrame30.createGlobalTempView("TW_SONG_FTUR_D_TMP_30")
    val frame30 = sparkSession.sql(
      s"""
         |select
         |period,
         |nbr,
         |name,
         |cast(rsi as double),
         |row_number() over(order by cast(rsi as double) desc) rsi_rank
         |from
         |global_temp.TW_SONG_FTUR_D_TMP_30
         |""".stripMargin)
    //      .show(false)
    //    将当天、最近7天、最近30天的数据进行关联，写入 tm_song_rsi_d 中。
    val unionData = frame1.union(frame7).union(frame30)
    unionData.createOrReplaceTempView("result")
    sparkSession.sql(
      s"""
         |insert overwrite table tm_song_rsi_d partition(data_dt=${args(0)})
         |select * from result
         |""".stripMargin)

    //    写入mysql中
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "123456")
    unionData.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hd01:3306/songresult", "tm_song_rsi", properties)
  }
}
