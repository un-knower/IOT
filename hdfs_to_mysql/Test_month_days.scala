package hdfs_to_mysql

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Created by sss on 2018/2/6.
 */
object Test_month_days {

  def main(args: Array[String]) {

    val days = get_sofar_month_day("20180206")
    for( index <- 1 until days.length){
      println(days(index))
    }
    println(days)
  }
  //
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
