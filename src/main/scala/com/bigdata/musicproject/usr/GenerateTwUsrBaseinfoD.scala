package com.bigdata.musicproject.usr

import java.util.Properties

import com.bigdata.musicproject.common.{ConfigUitls, DateUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object GenerateTwUsrBaseinfoD {
  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  val hivedatabasename: String = ConfigUitls.HIVE_DATABASE_NAME
  val iflocal: Boolean = ConfigUitls.iflocal
  val mysqljdbcurl: String = ConfigUitls.MYSQL_JDBC_URL
  val mysqlusername: String = ConfigUitls.MYSQL_USERNAME
  val mysqlpassword: String = ConfigUitls.MYSQL_PASSWORD


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

    sparkSession.sql(s"use ${hivedatabasename}")
    import org.apache.spark.sql.functions._
    //    withColumn 增加一列
    val wechat_df = sparkSession.table("TO_MNK_USR_D")
      .withColumn("REG_DT", column("REG_TM").substr(0, 8))
      .withColumn("REG_TM", column("REG_TM").substr(9, 6))
      .withColumn("REG_CHNL", lit("1"))
      .withColumn("USR_TYPE", col = lit("2"))
      .withColumn("IS_CERT", lit(null))
      .withColumn("IS_STDNT", lit(null))
      .withColumnRenamed("WX_ID", "ref_uid") // 修改 列的名字
      .select(
        "UID",
        "REG_MID",
        "REG_CHNL",
        "ref_uid",
        "GDR",
        "BIRTHDAY",
        "MSISDN",
        "LOC_ID",
        "LOG_MDE",
        "REG_DT",
        "REG_TM",
        "USR_EXP",
        "SCORE",
        "LEVEL",
        "USR_TYPE",
        "IS_CERT",
        "IS_STDNT"
      )
    //      .show(334129, false)
    val ali_df = sparkSession.table("TO_MNK_USR_ALI_D")
      .withColumn("REG_DT", column("REG_TM").substr(0, 8))
      .withColumn("REG_TM", column("REG_TM").substr(9, 6))
      .withColumn("REG_CHNL", lit("2"))
      .select(
        "UID",
        "REG_MID",
        "REG_CHNL",
        "ALY_ID",
        "GDR",
        "BIRTHDAY",
        "MSISDN",
        "LOC_ID",
        "LOG_MDE",
        "REG_DT",
        "REG_TM",
        "USR_EXP",
        "SCORE",
        "LEVEL",
        "USR_TYPE",
        "IS_CERT",
        "IS_STDNT"
      )
    //    ali_df.show(false)
    val qq_df = sparkSession.sql(
      s"""
         |SELECT UID,
         |       reg_mid,
         |       3 REG_CHNL,
         |       qqid,
         |       gdr,
         |       birthday,
         |       msisdn,
         |       loc_id,
         |       log_mde,
         |       substr(reg_tm,0,8) REG_DT,
         |       substr(reg_tm,9,6) REG_TM,
         |       usr_exp,
         |       score,
         |       level,
         |       2 USR_TYPE,
         |       null IS_CERT,
         |       null IS_STDNT
         |FROM to_mnk_usr_qq_d
         |""".stripMargin)


    val app_df = sparkSession.sql(
      s"""
         |SELECT UID,
         |       reg_mid,
         |       4 REG_CHNL,
         |       app_id,
         |       gdr,
         |       birthday,
         |       msisdn,
         |       loc_id,
         |       0 log_mde,
         |       substr(reg_tm,0,8) REG_DT,
         |       substr(reg_tm,9,6) REG_TM,
         |       usr_exp,
         |       0 score,
         |       level,
         |       1 USR_TYPE,
         |       null IS_CERT,
         |       null IS_STDNT
         |FROM to_mnk_usr_app_d
         |""".stripMargin)

    //    将多终端的用户数据 关联到一起
    val terminalUsers = wechat_df.union(ali_df).union(qq_df).union(app_df)
    //    terminalUsers.show(false)

    //    获取当日活跃用户基础信息
    val daily_active_users = sparkSession.sql(
      s"""
         |select * from to_mnk_usr_login_d where data_dt=${args(0)}

         |""".stripMargin)
    //    选择我们需要的列，并创建临时视图
    daily_active_users.join(terminalUsers, Seq("uid"), "inner")
      .select("UID",
        "REG_MID",
        "REG_CHNL",
        "REF_UID",
        "GDR",
        "BIRTHDAY",
        "MSISDN",
        "LOC_ID",
        "LOG_MDE",
        "REG_DT",
        "REG_TM",
        "USR_EXP",
        "SCORE",
        "LEVEL",
        "USR_TYPE",
        "IS_CERT",
        "IS_STDNT").createOrReplaceTempView("result")
    //      .show(3000,false)


    //    将日活用户写入当天分区中
    sparkSession.sql(
      s"""
         |insert overwrite table tw_usr_baseinfo_d partition(data_dt=${args(0)})
         |select * from result
         |""".stripMargin)

    //    将7日 日活写入 mysql中
    //    七日活跃用户，意思是 连续七天都上线过
    val per7 = DateUtils.getDateTime(args(0), 7)
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", mysqlusername)
    properties.put("password", mysqlpassword)
sparkSession.sql(
s"""
  |select * from
  |(SELECT UID,sum(num) numcount from
  |(SELECT UID,1 as num
  |FROM tw_usr_baseinfo_d
  |WHERE data_dt BETWEEN ${per7} AND ${args(0)}
  |GROUP BY data_dt,UID) a group by a.UID HAVING numcount=7) b join (select * from tw_usr_baseinfo_d where data_dt=${args(0)}) c on b.uid=c.uid
  |""".stripMargin)
.write.mode(SaveMode.Overwrite).jdbc(mysqljdbcurl, "user_7days_active", properties)
}
}
