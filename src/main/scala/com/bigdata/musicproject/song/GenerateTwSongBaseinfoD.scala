package com.bigdata.musicproject.song

import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}

import com.alibaba.fastjson.JSON
import com.bigdata.musicproject.common.ConfigUitls
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
 * 清洗to_song_info_d数据写入tw_song_baseinfo_d
 */
object GenerateTwSongBaseinfoD {
  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  val hivedatabasename: String = ConfigUitls.HIVE_DATABASE_NAME
  val iflocal: Boolean = ConfigUitls.iflocal


  def main(args: Array[String]): Unit = {
    //    由于此类 每天至少运行一次，数据基于天 进行分区，所以 必须传入一个 时间参数
    if (args.length < 1) {
      println("需要传入 时间参数")
      System.exit(1)
    }
    if (iflocal) {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").master("local[4]").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
    } else {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
    }

    sparkSession.udf.register("getAlbum", (album: String) => {
      var albumName = "暂无专辑"
      try {
        val pattern = Pattern.compile(".*?《(.*?)》.*?")
        val matcher: Matcher = pattern.matcher(album)
        if (matcher.find()) {
          albumName = matcher.group(1)
        }
      } catch {
        case e: Exception =>
          albumName
      }
      albumName
    })

    /**
     * 将时间格式解析成·要求格式
     */
    sparkSession.udf.register("getPostTime", (times: String) => {
      var postTime = ""
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      try {
        //        println(times.contains("E")+"\t"+times)
        if (times.contains("E")) {
          postTime = dateFormat.format(times.toDouble)
          //          println(postTime)
        } else {
          postTime = dateFormat.format(dateFormat.parse(times))
        }
      } catch {
        case e: Exception =>
        //          postTime = null
      }
      postTime
    })

    val getSingerAndId = (singerinfo: String, singerOrId: String) => {
      var singer1 = ""
      var singerid1 = ""
      var singer2 = ""
      var singerid2 = ""
      var a:String = ""
      try {
        val jSONArray = JSON.parseArray(singerinfo)
        if (jSONArray.size() > 0 && singerOrId.equals("singer1")) {
          val nObject = JSON.parseObject(jSONArray.get(0).toString)
          singer1 = nObject.getString("name")
          a = singer1
        } else if (jSONArray.size() > 0 && singerOrId.equals("singerid1")) {
          val nObject = JSON.parseObject(jSONArray.get(0).toString)
          singerid1 = nObject.getString("id")
          a = singerid1
        } else if (jSONArray.size() > 1 && singerOrId.equals("singer2")) {
          val nObject = JSON.parseObject(jSONArray.get(1).toString)
          singer2 = nObject.getString("name")
          a = singer2
        } else if (jSONArray.size() > 1 && singerOrId.equals("singerid2")) {
          val nObject = JSON.parseObject(jSONArray.get(1).toString)
          singerid2 = nObject.getString("id")
          a = singerid2
        }
      } catch {
        case e: Exception =>
          a = null
      }
      a
    }

    sparkSession.udf.register("getSingerAndId", getSingerAndId)

    sparkSession.udf.register("getProductTypeInfo", (productType: String) => {
      val ints = new ListBuffer[Int]
      try{
        val strings = productType.replaceAll("[\\[\\]]", "").split(",")
        for (s <- strings) {
          ints.append(s.toDouble.toInt)
        }
      }catch {
        case e:Exception=>
          null
      }
      ints
    })

    sparkSession.sql(s"use ${hivedatabasename}")
    sparkSession.sql(
      s"""
         |INSERT OVERWRITE TABLE tw_song_baseinfo_d
         |select
         |nbr,
         |nvl(name,other_name),
         |source,
         |getAlbum(album),
         |prdct,
         |lang,
         |video_format,
         |dur,
         |getSingerAndId(singer_info,"singer1"),
         |getSingerAndId(singer_info,"singer2"),
         |getSingerAndId(singer_info,"singerid1"),
         |getSingerAndId(singer_info,"singerid2"),
         |0,
         |getPostTime(post_time),
         |pinyin_fst,
         |pinyin,
         |sing_type,
         |ori_singer,
         |lyricist,
         |composer,
         |bpm_val,
         |star_level,
         |video_qlty,
         |video_mk,
         |video_ftur,
         |lyric_ftur,
         |img_qlty,
         |subtitles_type,
         |audio_fmt,
         |ori_sound_qlty,
         |ori_trk,
         |ori_trk_vol,
         |acc_ver,
         |acc_qlty,
         |acc_trk_vol,
         |acc_trk,
         |width,
         |height,
         |video_rsvl,
         |song_ver,
         |auth_co,
         |state,
         |getProductTypeInfo(prdct_type) from to_song_info_d
         |""".stripMargin)
      .show(false)
  }
}
