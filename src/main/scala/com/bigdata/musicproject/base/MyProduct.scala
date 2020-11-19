package com.bigdata.musicproject.base

//解决元组最大元素个数问题
class MyProduct(val nbr: String ,
                val name: String ,
                val source: Int ,
                val album: String ,
                val prdct: String ,
                val lang: String ,
                val video_format: String ,
                val dur: Int ,
                val singer1: String ,
                val singer2: String ,
                val singer1id: String ,
                val singer2id: String ,
                val mac_time: Int ,
                val post_time: String ,
                val pinyin_fst: String ,
                val pinyin: String ,
                val sing_type: Int ,
                val ori_singer: String ,
                val lyricist: String ,
                val composer: String ,
                val bpm_val: Int ,
                val star_level: Int ,
                val video_qlty: Int ,
                val video_mk: Int ,
                val video_ftur: Int ,
                val lyric_ftur: Int ,
                val img_qlty: Int ,
                val subtitles_type: Int ,
                val audio_fmt: Int ,
                val ori_sound_qlty: Int ,
                val ori_trk: Int ,
                val ori_trk_vol: Int ,
                val acc_ver: Int ,
                val acc_qlty: Int ,
                val acc_trk_vol: Int ,
                val acc_trk: Int ,
                val width: Int ,
                val height: Int ,
                val video_rsvl: Int ,
                val song_ver: Int ,
                val auth_co: String ,
                val state: Int ,
                val prdct_type: Array[Int]) extends Product with Serializable {
  override def productElement(n: Int): Any = n match {
    case 1 => name: String
    case 0 => nbr: String
    case 2 => source: Int
    case 3 => album: String
    case 4 => prdct: String
    case 5 => lang: String
    case 6 => video_format: String
    case 7 => dur: Int
    case 8 => singer1: String
    case 9 => singer2: String
    case 10 => singer1id: String
    case 11 => singer2id: String
    case 12 => mac_time: Int
    case 13 => post_time: String
    case 14 => pinyin_fst: String
    case 15 => pinyin: String
    case 16 => sing_type: Int
    case 17 => ori_singer: String
    case 18 => lyricist: String
    case 19 => composer: String
    case 20 => bpm_val: Int
    case 21 => star_level: Int
    case 22 => video_qlty: Int
    case 23 => video_mk: Int
    case 24 => video_ftur: Int
    case 25 => lyric_ftur: Int
    case 26 => img_qlty: Int
    case 27 => subtitles_type: Int
    case 28 => audio_fmt: Int
    case 29 => ori_sound_qlty: Int
    case 30 => ori_trk: Int
    case 31 => ori_trk_vol: Int
    case 32 => acc_ver: Int
    case 33 => acc_qlty: Int
    case 34 => acc_trk_vol: Int
    case 35 => acc_trk: Int
    case 36 => width: Int
    case 37 => height: Int
    case 38 => video_rsvl: Int
    case 39 => song_ver: Int
    case 40 => auth_co: String
    case 41 => state: Int
    case 42 => prdct_type: Array[Int]
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

  override def productArity: Int = 43

  override def canEqual(that: Any): Boolean = that.isInstanceOf[MyProduct]

  override def toString: String = {
    s"MyProduct[${
      (for (i <- 0 until productArity) yield productElement(i) match {
        case Some(x) => x
        case t => t
      }).mkString(",")
    }]"
  }
}
