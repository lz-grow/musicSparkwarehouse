package com.bigdata.musicproject.song

import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object GenerateTwSongBaseinfoD_firstEdition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GenerateTwSongBaseinfoD")
      .master("local[*]").enableHiveSupport().getOrCreate()
    //    val sc = spark.sparkContext
    import spark.implicits._
    val songInfoData: DataFrame = spark.sql("select * from TO_SONG_INFO_D")
    val dataFrame = songInfoData.rdd
      .map(x => {
        val str: String = x.toString()
        var nbr: String = x.get(0).toString
        var name: String = ""
        if (x.get(1).toString == "") {
          name = x.get(2).toString
        } else {
          name = x.get(1).toString
        }
        var source: Int = x.get(3).toString.toInt
        //正则匹配专辑名
        val regex = "(?<=\\《)[^\\》]+"
        var album: String = ""
        var matcher: Matcher = Pattern.compile(regex).matcher(x.get(4).toString)
        if (matcher.find()) {
          album = "《" + matcher.group() + "》"
        } else {
          album = "暂无专辑"
        }
        var prdct: String = x.get(5).toString
        var lang: String = x.get(6).toString
        var video_format: String = x.get(7).toString;
        var dur: Int = x.get(8).toString.toInt;
        var singer1: String = null
        var singer2: String = null
        var singer1id: String = null
        var singer2id: String = null
        var singerInfo: String=null
                if (x.get(9) != null && x.get(9).toString.split(",").length >= 2 && x.get(9).toString!="null" ) {
                  singerInfo = x.get(9).toString.substring(1, x.get(9).toString.length - 1)
                    .replaceAll("\\{", "")
                    .replaceAll("}", "")
                    .replaceAll("\"", "")
                  if (singerInfo.split(",").length == 4) {
                    if (singerInfo.split(",")(0).split(":").length == 2) {
                      singer1 = singerInfo.split(",")(0).split(":")(1)
                    }
                    singer1 = ""
                    if (singerInfo.split(",")(2).split(":").length == 2) {
                      singer2 = singerInfo.split(",")(2).split(":")(1)
                    }
                    singer2 = ""
                    if (singerInfo.split(",")(1).split(":").length == 2) {
                      singer1id = singerInfo.split(",")(1).split(":")(1)
                    }
                    singer1id = ""
                    if (singerInfo.split(",")(3).split(":").length == 2) {
                      singer2id = singerInfo.split(",")(3).split(":")(1)
                    }
                    singer2id = ""
                  }
                  else if (singerInfo.split(",").length == 2) {
                      if (singerInfo.split(",")(0).split(":").length == 2) {
                        singer1 = singerInfo.split(",")(0).split(":")(1)
                      }else{
                        singer1 = ""
                      }
                      singer2 = ""
                      if (singerInfo.split(",")(1).split(":").length == 2) {
                        singer1id = singerInfo.split(",")(1).split(":")(1)
                      } else {
                        singer1id = ""
                      }
                      singer2id = ""
                  }
                }


        var mac_time: Int = 0;
        var post_time: String = "";
        if (x.get(10).toString.contains("E")) {
          post_time = new java.math.BigDecimal(x.get(10).toString).toPlainString;
          var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          post_time = sdf.format(new java.util.Date(post_time.toLong))
        } else {
          post_time = x.get(10).toString;
        }

        var pinyin_fst: String = x.get(11).toString;
        var pinyin: String = x.get(12).toString;
        var sing_type: Int = x.get(13).toString.toInt;
        var ori_singer: String = x.get(14).toString;
        var lyricist: String = x.get(15).toString;
        var composer: String = x.get(16).toString;
        var bpm_val: Int = 0
        if (x.get(17) != null) {
          bpm_val = x.get(17).toString.toInt;
        } else {
          bpm_val = 0
        }
        //        var bpm_val: Int = x.get(17).toString.toInt;
        var star_level: Int = x.get(18).toString.toInt;
        var video_qlty: Int = x.get(19).toString.toInt;
        var video_mk: Int = x.get(20).toString.toInt;
        var video_ftur: Int = x.get(21).toString.toInt;
        var lyric_ftur: Int = x.get(22).toString.toInt;
        var img_qlty: Int = x.get(23).toString.toInt;
        var subtitles_type: Int = x.get(24).toString.toInt;
        var audio_fmt: Int = x.get(25).toString.toInt;
        var ori_sound_qlty: Int = x.get(26).toString.toInt;
        var ori_trk: Int = x.get(27).toString.toInt;
        var ori_trk_vol: Int = x.get(28).toString.toInt;
        var acc_ver: Int = x.get(29).toString.toInt;
        var acc_qlty: Int = x.get(30).toString.toInt;
        var acc_trk_vol: Int = x.get(31).toString.toInt;
        var acc_trk: Int = x.get(32).toString.toInt;
        var width: Int = x.get(33).toString.toInt;
        var height: Int = x.get(34).toString.toInt;
        var video_rsvl: Int = x.get(35).toString.toInt;
        var song_ver: Int = x.get(36).toString.toInt;
        var auth_co: String = x.get(37).toString;
        var state: Int = 0
        if (x.get(38) != null) {
          state = x.get(38).toString.toInt;
        } else {
          state = 0
        }
        //        var state: Int = x.get(38).toString.toInt;
        var s: String = x.get(39).toString.replaceAll("\\[", "").replaceAll("]", "");
        var prdct_type: Array[Int] = new Array[Int](s.split(",").length)
        if (s.length == 0) {
          prdct_type = null;
        } else {
          var strings: Array[String] = s.split(",")
          var n = 0
          for (i <- strings) {
            prdct_type(n) = i.toDouble.toInt
            n = n + 1
          }
        }
//        singerInfo
//                (singer1,singer1id,singer2,singer2id)
        //由于元组元素的个数最多为22个 所以返回scala元组行不通
        //通过自定义MyProduct类继承Product来满足业务需求
        //        (nbr + "\t" + name + "\t" + source + "\t" + album + "\t" + prdct + "\t" + lang + "\t" + video_format + "\t" + dur + "\t" + singer1 + "\t" + singer2 + "\t" + singer1id + "\t" + singer2id + "\t" + mac_time + "\t" + post_time + "\t" + pinyin_fst + "\t" + pinyin + "\t" + sing_type + "\t" + ori_singer + "\t" + lyricist + "\t" + composer + "\t" + bpm_val + "\t" + star_level + "\t" + video_qlty + "\t" + video_mk + "\t" + video_ftur + "\t" + lyric_ftur + "\t" + img_qlty + "\t" + subtitles_type + "\t" + audio_fmt + "\t" + ori_sound_qlty + "\t" + ori_trk + "\t" + ori_trk_vol + "\t" + acc_ver + "\t" + acc_qlty + "\t" + acc_trk_vol + "\t" + acc_trk + "\t" + width + "\t" + height + "\t" + video_rsvl + "\t" + song_ver + "\t" + auth_co + "\t" + state, prdct_type)
        new com.bigdata.musicproject.base.MyProduct(nbr, name, source, album, prdct, lang, video_format, dur, singer1, singer2, singer1id, singer2id, mac_time, post_time, pinyin_fst, pinyin, sing_type, ori_singer, lyricist, composer, bpm_val, star_level, video_qlty, video_mk, video_ftur, lyric_ftur, img_qlty, subtitles_type, audio_fmt, ori_sound_qlty, ori_trk, ori_trk_vol, acc_ver, acc_qlty, acc_trk_vol, acc_trk, width, height, video_rsvl, song_ver, auth_co, state, prdct_type)
      }).toDF()
    dataFrame.createOrReplaceTempView("info_table")
    spark.sql("insert into table tw_song_baseinfo_d select * from info_table")
  }



//繁琐
  /*var singerInfo: String=null
        if (x.get(9) != null & x.get(9).toString.split(",").length >= 2) {
          singerInfo = x.get(9).toString.substring(1, x.get(9).toString.length - 1)
            .replaceAll("\\{", "")
            .replaceAll("}", "")
            .replaceAll("\"", "")
          if (singerInfo.split(",").length >= 4) {
            if (singerInfo.split(",")(0).split(":").length == 2) {
              singer1 = singerInfo.split(",")(0).split(":")(1)
            }
            singer1 = ""
            if (singerInfo.split(",")(2).split(":").length == 2) {
              singer2 = singerInfo.split(",")(2).split(":")(1)
            }
            singer2 = ""
            if (singerInfo.split(",")(1).split(":").length == 2) {
              singer1id = singerInfo.split(",")(1).split(":")(1)
            }
            singer1id = ""
            if (singerInfo.split(",")(3).split(":").length == 2) {
              singer2id = singerInfo.split(",")(3).split(":")(1)
            }
            singer2id = ""
          } else {
            if (singerInfo.split(",")(0).split(":").length == 2) {
              singer1 = singerInfo.split(",")(0).split(":")(1)
            }
            singer1 = ""
            singer2 = ""
            if (singerInfo.split(",")(1).split(":").length == 2) {
              singer1id = singerInfo.split(",")(1).split(":")(1)
            } else {
              singer1id = ""
            }
            singer2id = ""
          }
        }*/

  //不适合所有数据
  /*if (x.get(9).toString.split(",").length==2){
            val json = x.get(9).toString.substring(1, x.get(9).toString.length - 1)
            singer1 = JSON.parseObject(json).get("name").toString
            singer1id=JSON.parseObject(json).get("id").toString
          }
          else if(x.get(9).toString.split(",").length==4){
            val json = x.get(9).toString.substring(1, x.get(9).toString.length - 1)
            val json1 = json.substring(0, json.indexOf("}")+1)
            val json2 = json.substring(json.lastIndexOf("{"))
            singer1 = JSON.parseObject(json1).get("name").toString
            singer1id=JSON.parseObject(json1).get("id").toString

            singer2 = JSON.parseObject(json2).get("name").toString
            singer2id=JSON.parseObject(json2).get("id").toString
          }*/
}
