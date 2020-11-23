package com.bigdata.musicproject.common

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUitls {
  //通过ConfigFactory.load()方法 读取resource中的配置文件
  // application.conf、application.properties或application.json 都可以被识别
  val load: Config = ConfigFactory.load()
  val HIVE_DATABASE_NAME: String = load.getString("hive.database.name")
  val iflocal: Boolean = load.getBoolean("iflocal")
  val HDFS_PATH: String = load.getString("hdfs.path")
  val MYSQL_USERNAME:String=load.getString("mysql.username")
  val MYSQL_JDBC_URL:String=load.getString("mysql.jdbc.url")
  val MYSQL_PASSWORD:String=load.getString("mysql.password")
  //这里定义的常量  共全局去使用

}
