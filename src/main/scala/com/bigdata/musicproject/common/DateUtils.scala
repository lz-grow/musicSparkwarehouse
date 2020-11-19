package com.bigdata.musicproject.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object DateUtils {
  def getDateTime(times: String, num: Int): String = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val str: Date = format.parse(times)
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(str)
    calendar.add(Calendar.DAY_OF_MONTH, -num)
    format.format(calendar.getTime)
  }

}
