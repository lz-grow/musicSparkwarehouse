package com.bigdata.musicproject.song

import com.alibaba.fastjson.JSON
import com.bigdata.musicproject.base.ParRDDMultipleTextOutputFormat
import com.bigdata.musicproject.common.ConfigUitls
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * 客户端日志数据写入hdfs
 */
object ProduceClientLog {
  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  var musicRDD: RDD[String] = _
  val hivedatabasename: String = ConfigUitls.HIVE_DATABASE_NAME
  val iflocal: Boolean = ConfigUitls.iflocal
  val hdfspath: String = ConfigUitls.HDFS_PATH

  def main(args: Array[String]): Unit = {
    //由于此类 每天至少运行一次，数据基于天 进行分区，所以 必须传入一个 时间参数
    if(args.length<1){
      println("需要传入时间参数")
      System.exit(1)
    }

    //本地测试和集群运行  读取不同地方的文件
    if (iflocal) {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").master("local[4]").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      musicRDD = sc.textFile("file:///E:\\projects\\IDEAProject\\sparkwarehouse\\datas\\currentday_clientlog.tar.gz")
    } else {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      //      musicRDD = sc.textFile("file:///D:\\IdeaProject\\MusicProject\\datas\\currentday_clientlog.tar.gz")
      musicRDD = sc.textFile(s"${hdfspath}${args(0)}/currentday_clientlog.tar.gz")
    }

    val clearRDD = musicRDD.map(_.split("&"))
      .filter(_.length == 6)
      .map(arr => (arr(2), arr(3)))

    //    获取有多少种标识,  获取key值，去重，收集到数组中，获取数组的长度  为分区做准备
    val dataTypeNum = clearRDD.keys.distinct().collect().length
    val datas: RDD[(String, String)] = clearRDD.map(line => {
      //      数据类型
      val dataType = line._1
      //      音乐数据
      val musicData = line._2
      //数据类型是MINIK_CLIENT_SONG_PLAY_OPERATE_REQ的处理返回  为写入hive做准备  其他类型的原样返回
      if (dataType.contains("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ")) {
        val nObject = JSON.parseObject(musicData)
        val songid = nObject.getString("songid")
        val mid = nObject.getString("mid")
        val optrate_type = nObject.getString("optrate_type")
        val uid = nObject.getString("uid")
        val consume_type = nObject.getString("consume_type")
        val dur_time = nObject.getString("dur_time")
        val session_id = nObject.getString("session_id")
        val songname = nObject.getString("songname")
        val pkg_id = nObject.getString("pkg_id")
        val order_id = nObject.getString("order_id")
        //将得到的数据字段，放入元祖中，元祖中有两个元素，key是 数据标识，value中是 标识对应的数据。，字段以\t分割
        (dataType, songid + "\t" + mid + "\t" + optrate_type + "\t" + uid + "\t" + consume_type + "\t" + dur_time + "\t" + session_id + "\t" + songname + "\t" + pkg_id + "\t" + order_id)
      } else {
        line
      }
    })
    datas
    //将数据分别以标识名的方式存储在hdfs某个路径中， 也就是说我们需要通过 标识来进行分区，不同的数据写入不同文件
      .partitionBy(new HashPartitioner(dataTypeNum))
    //由于saveAsTextFile，不能够自定义文件名字，所以 替换掉
    //.saveAsTextFile("/logdata/all_client_tables")
      .saveAsHadoopFile(s"/logdata/${args(0)}/all_client_tables",
        classOf[String],classOf[String],classOf[ParRDDMultipleTextOutputFormat])

    val useData: RDD[String] = sc.textFile(s"${hdfspath}${args(0)}/all_client_tables/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ")
    useData.map(x=>{
      x.replaceAll("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ\t","")
    })
        .saveAsTextFile(s"/logdata/${args(0)}/useData")

    //数据库可以通过常量类写成变量  优化代码
    sparkSession.sql(s"use ${hivedatabasename}")
    //按时间来分区  我们可以把时间作为参数传入  优化代码
    sparkSession.sql(
      s"""
         |load data inpath "/logdata/${args(0)}/useData/*"
         |overwrite into table to_minik_client_song_play_operate_req_d partition(data_dt=${args(0)})
         |""".stripMargin)
  }
}
