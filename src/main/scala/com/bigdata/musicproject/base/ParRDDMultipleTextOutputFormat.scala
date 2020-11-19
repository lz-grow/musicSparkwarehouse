package com.bigdata.musicproject.base

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

//重写generateFileNameForKeyValue方法  给分区文件重命名为key的值
class ParRDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any,Any]{
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = key.asInstanceOf[String]

}
