package hdfs_to_mysql
import java.util.Properties
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
/**
 * Created by dan on 2018-05-11.
 * nohup /opt/spark2/bin/spark-submit --class hdfs_to_mysql.day_data_hive /home/yimr/xudd/jar_spark/IoT_xudd.jar 20180305 > /home/yimr/xudd/nohup_all.txt
   /user/hdfs/iot/wlw_order/
 */
object day_data_hive {

  private val master = "xxx.xx.xx.xx"
  private val port = "xxxx"
  private val appName = "xxxx"

  private val hdfs_path = "xxxx://cdh1:8020"
  private val input_path_order = hdfs_path + "/xxxx/"
  private val data_output=hdfs_path + "/xxxx/"

  case class iot_order_sim(ORDER_NO: String,
                           PHONE_NUM: String,
                           CREATE_DATE: String,
                           STATE: String,
                           SIM_CARD_TYPE: String,
                           RECEIVE_DATE: String,
                           ORDER_NUM: Int,
                           SUM_P: Float,
                           PREMIUM_PROCESSING: String,
                           ORDER_SOURCE: String,
                           ORDER_STATE: String)
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println( s"""
                           | <day> the date of data
    """.stripMargin)
      System.exit(1)
    }

    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(hdfs_path), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(new Path(data_output)))
      hdfs.delete(new Path(data_output), true)

    val spark = SparkSession
      .builder
      .appName(appName)
      .config("spark.executor.memory", "50g")
      .config("spark.cores.max", "72")
      .config("spark.dynamicAllocation.enabled", "false")
      .master(s"spark://$master:$port")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext


    val day_logic = args(0)
    val data_input_order = input_path_order + "xxxx*"

    val order_daily_2ts= Util_date.get_account_daily_2timestamp(day_logic)
    val order_period_2ts = Util_date.get_account_period_2timestamp(day_logic)

    import spark.implicits._
    val iot_order_sim_df = spark.read.textFile(data_input_order)
      .map(_.replace(",","").split("\\|", -1))
      //.map(_.split("\\|", -1))
      .filter(splits => splits.length > 10)
      .map(splits => iot_order_sim(
      splits(0), splits(1), splits(2),
      splits(3),splits(4), splits(5),
      splits(6).toInt,splits(7).toFloat, splits(8),
      splits(9),splits(10)
    ))

      .toDF()


    iot_order_sim_df
      .filter("RECEIVE_DATE < '"+order_daily_2ts(1)+"'")
      //.filter("ORDER_STATE = '21'")
      .groupBy("PHONE_NUM")
      .agg(count(lit(1)).as("acct_addup_shipped_order"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_addup_order")


    iot_order_sim_df
      .filter("RECEIVE_DATE < '"+order_daily_2ts(1)+"'")

      .filter("ORDER_NUM > 100")
      .groupBy("PHONE_NUM")
      .agg(count(lit(1)).as("acct_addup_shipped_order_gt_100"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_addup_order_gt_100")


    iot_order_sim_df
      .filter("ORDER_STATE = '0' ")
      .filter("RECEIVE_DATE >= '"+order_daily_2ts(0)+"' AND RECEIVE_DATE < '"+order_daily_2ts(1)+"'")
      .groupBy("PHONE_NUM")
      .agg(count(lit(1)).as("acct_shipped_new_order_daily"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_iot_order_sim_daily")


    iot_order_sim_df
      .filter("ORDER_STATE = '0' ")
      .filter("RECEIVE_DATE >= '"+order_period_2ts(0)+"' AND RECEIVE_DATE < '"+order_period_2ts(1)+"'")
      .groupBy("PHONE_NUM")
      .agg(count(lit(1)).as("acct_shipped_new_order_period"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_iot_order_sim_period")



    spark.sql("SELECT "+
      "vao.PHONE_NUM, vao.acct_addup_shipped_order, "+
      "vao1.acct_addup_shipped_order_gt_100, "+
      "osd.acct_shipped_new_order_daily, "+
      "osp.acct_shipped_new_order_period "+
      "FROM view_addup_order vao left join view_addup_order_gt_100 vao1 on vao1.PHONE_NUM =vao.PHONE_NUM " +
      "left join view_iot_order_sim_period osp on osp.PHONE_NUM = vao.PHONE_NUM " +
      "left join view_iot_order_sim_daily osd on osd.PHONE_NUM = vao.PHONE_NUM "
    ).createOrReplaceTempView("view_order")
    import spark.sql

    sql(
      s"""
    |create table if not exists iot.wlw_xudd_order
    |as select * from view_order
  """.stripMargin
    )
    sql("CREATE TABLE IF NOT EXISTS iot.xxxx(PHONE_NUM string," +
      "acct_addup_shipped_order int," +
      "acct_addup_shipped_order_gt_100 int," +
      "acct_shipped_new_order_daily int," +
      "acct_shipped_new_order_period int)" )
    sql("insert overwrite table iot.xxxx select * from view_order")

    }
}
