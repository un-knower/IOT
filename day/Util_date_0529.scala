package com.wangxiaoxia

import java.sql
import java.util.Date
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.Column

/**
 * Created by sss on 2018/2/6.
 */
object Util_date {

  def str2date(str: String, pattern: String = "yyyyMMdd"): sql.Date = {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(str)
    new sql.Date(input_date.getTime)
  }



  def main(args: Array[String]) {

    
    println(get_period_forOrder_2seconds("20180226"))
    println(get_period_forOrder_2seconds("20180227"))
    println(get_period_forOrder_2seconds("20180308"))
    println(get_day_time_forOrder("20180228"))

   
  }



  /**
    得到上个账期的最后一天即26号
    println(get_file_month_last_day("20180430"))
    返回：20180426
   * */
  def get_file_month_last_day(day: String,  pattern: String = "yyyyMMdd") = {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) <= 31) {

    } else{
      input_calendar.add(Calendar.MONTH, -1)
    }
    input_calendar.set(Calendar.DAY_OF_MONTH, 26)

    val days = sdf.format(input_calendar.getTime)
    days
  }


  /**
   增量的数据获取北京时间转换成UTC时间后，从账期初到截止日的日期
    * println(get_account_period_day_list("20180430"))
    * 返回：List(20180501, 20180430, 20180429, 20180428, 20180427)
   */
  def get_account_period_day_list(day: String, pattern: String = "yyyyMMdd") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    val tmp_calendar = Calendar.getInstance()
    tmp_calendar.setTime(input_date)
    tmp_calendar.add(Calendar.DAY_OF_MONTH, 1)

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.set(Calendar.DAY_OF_MONTH, 27)
    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) <= 31) {

    } else{
      last_day.add(Calendar.MONTH, -1)

    }
    //println(sdf.format(last_day.getTime))


    var days: List[String] = List(sdf.format(tmp_calendar.getTime))

    while (true) {
      tmp_calendar.add(Calendar.DAY_OF_YEAR, -1)
      if (tmp_calendar.compareTo(last_day) >= 0 ) {
        days = days :+ sdf.format(tmp_calendar.getTime)
      }else {
        return days
      }
    }
    days
  }

  /**
        获取从账期初到统计日转换成UTC时间后：（以utc时间返回）
      println(get_account_period_2utc("20180430"))
      返回：List(20180426160000, 20180430155959)
    * @param day
    * @param pattern
    * @return
    */
  def get_account_period_2utc(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    input_calendar.set(Calendar.HOUR_OF_DAY, 15)
    input_calendar.set(Calendar.MINUTE,59)
    input_calendar.set(Calendar.SECOND,59)
    //println(sdf.format(input_calendar.getTime))

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.set(Calendar.DAY_OF_MONTH, 27)
    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) <= 31) {

    } else{
      last_day.add(Calendar.MONTH, -1)
    }

    last_day.add(Calendar.HOUR, -8)

    val out_sdf = new SimpleDateFormat(outPutPattern)
    var days: List[String] = List(out_sdf.format(last_day.getTime),out_sdf.format(input_calendar.getTime))

    days
  }




  /*
   订单系统专用，北京时间从账期初到统计日转换成UTC时间后的日期
   println(get_account_period_day_forOrder("20180228"))
   List(20180228, 20180227, 20180226)
    */

  def get_account_period_day_forOrder(day: String, pattern: String = "yyyyMMdd") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    val tmp_calendar = Calendar.getInstance()
    tmp_calendar.setTime(input_date)
    tmp_calendar.add(Calendar.DAY_OF_MONTH, 0)

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.set(Calendar.DAY_OF_MONTH, 26)
    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) <= 31) {

    } else{
      last_day.add(Calendar.MONTH, -1)

    }

    var days: List[String] = List(sdf.format(tmp_calendar.getTime))

    while (true) {
      tmp_calendar.add(Calendar.DAY_OF_YEAR, -1)
      if (tmp_calendar.compareTo(last_day) >= 0 ) {
        days = days :+ sdf.format(tmp_calendar.getTime)
      }else {
        return days
      }
    }
    days
  }


  /*
      订单系统专用，北京时间从0点到24点
       println(get_day_time_forOrder("20180227"))
      返回：List(20180227000000, 20180227235959)
       */

  def get_day_time_forOrder(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)
    input_calendar.set(Calendar.HOUR_OF_DAY, 23)
    input_calendar.set(Calendar.MINUTE,59)
    input_calendar.set(Calendar.SECOND,59)

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)

    val out_sdf = new SimpleDateFormat(outPutPattern)
    var days: List[String] = List(out_sdf.format(last_day.getTime),out_sdf.format(input_calendar.getTime))
    days
  }


  /*
    订单系统专用，北京时间从账期初到统计日转换成UTC时间后的日期
    println(get_day_time_forOrder_2seconds("20180227"))
    返回：List(1519660800000, 1519747199000)
     */
  def get_day_time_forOrder_2seconds(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[String] = {

    val input_date: List[String]  = get_day_time_forOrder(day)
    val input_begin = input_date(0)
    val input_end = input_date(1)

    val sdf = new SimpleDateFormat(outPutPattern)
    val output_begin = sdf.parse(input_begin).getTime().toString
    val output_end = sdf.parse(input_end).getTime().toString

    var days: List[String] = List(output_begin,output_end)
    days
    // println("thsis"+output_begin)
    //println("hdhd"+output_end)
  }


  /*
  订单系统专用，北京时间从账期初到统计日
  println(get_period_forOrder_2seconds("20180226"))
  返回：List(1516982400000, 1519660799000)
   println(get_period_forOrder_2seconds("20180308"))
   返回：List(20180227000000, 20180308235959)
   */

  def get_period_forOrder_2seconds(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[String] = {

    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    input_calendar.set(Calendar.HOUR_OF_DAY, 23)
    input_calendar.set(Calendar.MINUTE,59)
    input_calendar.set(Calendar.SECOND,59)
    //println(sdf.format(input_calendar.getTime))

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.set(Calendar.DAY_OF_MONTH, 27)
    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) <= 31) {

    } else{
      last_day.add(Calendar.MONTH, -1)
    }

    val out_sdf = new SimpleDateFormat(outPutPattern)
    var days: List[String] = List(out_sdf.format(last_day.getTime),out_sdf.format(input_calendar.getTime))
    val days_begin = out_sdf.parse(days(0)).getTime().toString
    val days_end =  out_sdf.parse(days(1)).getTime().toString
    val days_2seconds = List(days_begin,days_end)


    days_2seconds
    //days
  }







  /**
    * 没有用到这个函数
  统计日的0点到24点转换成UTC时间后
    println(get_account_day_period_2utc("20180430"))
    返回：List(20180429160000, 20180430155959)
    */

  def get_account_day_period_2utc(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    input_calendar.set(Calendar.HOUR_OF_DAY, 15)
    input_calendar.set(Calendar.MINUTE,59)
    input_calendar.set(Calendar.SECOND,59)

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.add(Calendar.DAY_OF_MONTH, -1)
    last_day.set(Calendar.HOUR_OF_DAY, 16)

    val out_sdf = new SimpleDateFormat(outPutPattern)
    var days: List[String] = List(out_sdf.format(last_day.getTime),out_sdf.format(input_calendar.getTime))
    days
  }



  /**
    *没有用到这个函数
    * @param day_logic  逻辑日期
    * @param pattern 文件日期
    * @return 文件比逻辑日期多一天
    * println(get_logic2file_day("20180430"))
    * 返回：20180501
    */
  def get_logic2file_day(day_logic: String,  pattern: String = "yyyyMMdd") = {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day_logic)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    input_calendar.add(Calendar.DAY_OF_YEAR, +1)

    sdf.format(input_calendar.getTime)
  }


  /**
    * 没有用到这个函数
    * 获取本月，从1号到今天的日期
   println(get_sofar_month_day("20180430"))  结果：
  List(20180430, 20180429, 20180428, 20180427, 20180426, 20180425, 20180424, 20180423, 20180422, 20180421,
   20180420, 20180419, 20180418, 20180417, 20180416, 20180415, 20180414, 20180413, 20180412, 20180411,
  20180410, 20180409, 20180408, 20180407, 20180406, 20180405, 20180404, 20180403, 20180402, 20180401)
    * */
  def get_sofar_month_day(day: String, pattern: String = "yyyyMMdd") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    val month = input_calendar.get(Calendar.MONTH)

    var days: List[String] = List(sdf.format(input_calendar.getTime))

    while (true) {
      input_calendar.add(Calendar.DAY_OF_YEAR, -1)
      if (input_calendar.get(Calendar.MONTH) == month) {
        days = days :+ sdf.format(input_calendar.getTime)
      }else {
        return days
      }
    }
    return days
  }







}
