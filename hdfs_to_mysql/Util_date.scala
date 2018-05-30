package hdfs_to_mysql

import java.sql
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import java.util.Date
import java.sql.Timestamp



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

    println(get_logic2file_day("20180305"))
    //20180306
    println(get_file_month_last_day("20180305"))
    //20180226

    println(get_account_period_day_list("20180305"))
    //List(20180306, 20180305, 20180304, 20180303, 20180302, 20180301, 20180228, 20180227)

    println(get_account_period_2utc("20180301"))
    //List(20180226160000, 20180305160000)

    println(get_account_period_2timestamp("20180301"))

    println(get_account_daily_2utc("20180301"))

    println(get_account_daily_2timestamp("20180301"))

  }

  /**
   *
   * @param day_logic  逻辑日期
   * @param pattern 文件日期
   * @return 文件比逻辑日期多一天
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

  以   上月27，到本月26 为一个账期


   * */
  def get_file_month_last_day(day: String,  pattern: String = "yyyyMMdd") = {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) < 31) {

    } else{
      input_calendar.add(Calendar.MONTH, -1)
    }
    input_calendar.set(Calendar.DAY_OF_MONTH, 26)

    val days = sdf.format(input_calendar.getTime)
    days
  }

  /**
   * 获取本月，到今天为止 所有的天
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

  /**
   以   上月27，到本月26 为一个账期，获取一个账期内的所有天

   * @param day
   * @param pattern
   * @return
   */
  def get_account_period_day_list(day: String, pattern: String = "yyyyMMdd") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)
    //取得账期开始时间
    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.set(Calendar.DAY_OF_MONTH, 27)
    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) < 31) {

    } else{
      last_day.add(Calendar.MONTH, -1)
    }

    //获取账期内，源文件最新日期，
    input_calendar.add(Calendar.DAY_OF_YEAR, +1)
    sdf.format(input_calendar.getTime)

    var days: List[String] = List(sdf.format(input_calendar.getTime))

    while (true) {
      input_calendar.add(Calendar.DAY_OF_YEAR, -1)
      if (input_calendar.compareTo(last_day) >= 0 ) {
        days = days :+ sdf.format(input_calendar.getTime)
      }else {
        return days
      }
    }
    days
  }

  /**
        获取一个账期的周期：（以utc时间返回）

    * @param day
    * @param pattern
    * @return
    */
  def get_account_period_2utc(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.set(Calendar.DAY_OF_MONTH, 27)
    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) < 31) {

    } else{
      last_day.add(Calendar.MONTH, -1)
    }

    last_day.add(Calendar.HOUR, -8)

    val out_sdf = new SimpleDateFormat(outPutPattern)
    var days: List[String] = List(out_sdf.format(last_day.getTime))

    input_calendar.add(Calendar.HOUR,+16)
    days = days :+ out_sdf.format(input_calendar.getTime)
    days
  }

  def get_account_period_2timestamp(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[Long]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.set(Calendar.DAY_OF_MONTH, 27)
    if (input_calendar.get(Calendar.DAY_OF_MONTH) >= 27 && input_calendar.get(Calendar.DAY_OF_MONTH) < 31) {

    } else{
      last_day.add(Calendar.MONTH, -1)
    }


    last_day.add(Calendar.HOUR, 0)
    input_calendar.add(Calendar.HOUR,+24)

    val out_sdf = new SimpleDateFormat(outPutPattern)

    val input_calendar_utc =out_sdf.format(input_calendar.getTime)
    val last_day_utc =out_sdf.format(last_day.getTime)

    val head_ts=out_sdf.parse(input_calendar_utc).getTime()/1000
    val last_ts=out_sdf.parse(last_day_utc).getTime()/1000
    var days: List[Long] = List(last_ts)
    days = days :+ head_ts
    days

  }



  /**
  获取当日的周期：（以utc时间返回）

    * @param day
    * @param pattern
    * @return
    */
  def get_account_daily_2utc(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[String]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    last_day.add(Calendar.HOUR, -8)

    val out_sdf = new SimpleDateFormat(outPutPattern)
    var days: List[String] = List(out_sdf.format(last_day.getTime))

    input_calendar.add(Calendar.HOUR,+16)
    days = days :+ out_sdf.format(input_calendar.getTime)
    days
  }

  /**
  获取当日的周期：（以utc时间返回）
          3月1号：返回：      20180228160000, 20180301160000;
                             2018-02-28 16:00:00,2018-03-01 16:00:00,
          3月1号：返回时间戳：List(1519804800000, 1519891200000)


    * @param day
    * @param pattern
    * @return
    */
  def get_account_daily_2timestamp(day: String, pattern: String = "yyyyMMdd", outPutPattern: String = "yyyyMMddHHmmss") : List[Long]= {
    val sdf = new SimpleDateFormat(pattern)
    val input_date = sdf.parse(day)
    val input_calendar = Calendar.getInstance()
    input_calendar.setTime(input_date)
    //input_calendar.add(Calendar.HOUR,+16)
    input_calendar.add(Calendar.HOUR,+24)

    val last_day = Calendar.getInstance()
    last_day.setTime(input_date)
    //last_day.add(Calendar.HOUR, -8)
    last_day.add(Calendar.HOUR, 0)

    val out_sdf = new SimpleDateFormat(outPutPattern)
    val input_calendar_utc=out_sdf.format(input_calendar.getTime)
    val last_day_utc=out_sdf.format(last_day.getTime)


    val head_ts=out_sdf.parse(input_calendar_utc).getTime()/1000
    val last_ts=out_sdf.parse(last_day_utc).getTime()/1000
    var days: List[Long] = List(last_ts)
    days = days :+ head_ts
    days


    
  }






  /*
      * change date string to timestamp value
      */
  def getTimestamp(x:String) :java.sql.Timestamp = {
    //       "20151021235349"
    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    var ts = new Timestamp(System.currentTimeMillis());
    try {
      if (x == "")
        return null
      else {
        val d = format.parse(x);
        val t = new Timestamp(d.getTime());
        return t
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }
    return null
  }


  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()

    tim
  }

  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }


}
