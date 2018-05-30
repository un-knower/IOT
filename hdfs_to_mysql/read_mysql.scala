package hdfs_to_mysql

import java.sql.Date
import java.util.Properties
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import hdfs_to_mysql.Util_date
import java.io.StringReader
import java.sql.DriverManager


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVReader

object read_mysql {

  private val master = "xxx.xx.xx.xx"
  private val port = "xxxx"
  private val appName = "xxxx"
  private val hdfs_path = "xxxx://cdh1:8020"
  private val data_output_one = hdfs_path + "/xxxx/"
  private val data_output_two = hdfs_path + "/xxxx/"

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println( s"""
                             | <day> the date of data
    """.stripMargin)
      System.exit(1)
    }


    val spark = SparkSession
      .builder
      .appName(appName)
      .config("spark.executor.memory", "100g")
      .config("spark.cores.max", "72")
      .config("spark.dynamicAllocation.enabled", "false")
      .master(s"spark://$master:$port")
      .getOrCreate()

    val connectProperties = new Properties()

    //方式一：通过不指定查询条件
    val url="jdbc:mysql://xxxx"
    val df=spark.read.jdbc(url,"wlw_user_info",connectProperties)
    println("第一种方法输出："+df.count());
    println("1.------------->" + df.count());
    println("1.------------->" + df.rdd.partitions.size);
    df.repartition(1).rdd.saveAsTextFile(data_output_one)

    //方式二：定义mysql信息,通过load来获取
    val jdbcDF = spark.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://xxxx",
        "dbtable"->"(select CUST_NAME,CUST_ID,GROUP_ID,WLW_NUMBER from wlw_user_info) as some_alias",
        "driver"->"com.mysql.jdbc.Driver",
        "user"-> "root",
        //"partitionColumn"->"day_id",
        "lowerBound"->"0",
        "upperBound"-> "100",
        //"numPartitions"->"2",
        "fetchSize"->"100",
        "password"->"root")).load()

    jdbcDF.collect().take(20).foreach(println) //终端打印DF中的数据。
    jdbcDF.rdd.saveAsTextFile(data_output_two)
  }



}
