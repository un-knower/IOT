package hdfs_to_mysql

/**
 * Created by dan on 2018-02-28.
 * nohup /opt/spark2/bin/spark-submit --class hdfs_to_mysql.day_data_mysql_all /home/yimr/xudd/IoT_xudd.jar 20180302 > /home/yimr/xudd/nohup_all.txt
   /user/hdfs/iot/jasper
 */

import java.sql.Date
import java.util.Properties
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
//import hdfs_to_mysql.Util_date

object day_data_mysql_all {
  private val master = "xxx.xx.xx.xx"
  private val port = "xxxx"
  private val appName = "xxxx"

  private val hdfs_path = "xxxx://cdh1:8020"
  private val input_path = hdfs_path + "/xxxx/"
  private val businesschance_path = hdfs_path + "/xxxx/"

  private val input_last_month_path = hdfs_path + "/xxxx/"
  private val data_output = hdfs_path + "/xxxx/"


  case class subs_snapshot(iccid: String, acctID: String, ini_act_date: String, rate_plan_id: String,
                           sim_state: String)

  case class last_month_subs_snapshot(iccid: String, acctID: String, ini_act_date: String, rate_plan_id: String,
                                      sim_state: String)

  case class acct_snapshot(acct_cycle: String, acctID: String, account_name: String, operator_id: String,
                           account_status: String,
                           account_billable_flag: String)

  case class subs_changes(acctID: String, change_type: Int, new_value: String, change_datetime: String)

  case class Usage_Detail_SMS(acctID: String, received_date: String, JPO_ACCT_SMS_NUM: Long, iccid: String, rounded:Int)
  case class Usage_Detail_Data(acctID: String, received_date: String, JPO_ACCT_DATA_NUM: Long, iccid: String, rounded:Int)
  case class Usage_Detail_Voice(acctID: String, received_date: String, JPO_ACCT_VOI_NUM: Long, iccid: String, rounded:Int)

  case class Rate_Plan_Snapshot(acctID: String, rate_plan_id: String, rate_plan_type:String)

  case class business_chance(company_stock_connect_num: String,
                             is_chinaunicom_sole: String,
                             year_pre_connect_num: String,
                             stage: String,
                             province: String,
                             city: String,
                             principle_industry: String,
                             industry_main_product: String,
                             industry_detail_product: String,
                             customer_degree: String,
                             customer_name: String,
                             customer_group_id: String,
                             customer_id: String,
                             first_order_time: String,
                             commerce_model: String,
                             facility_type: String,
                             customer_manager: String,
                             telephone: String,
                             email: String,
                             support_manager: String,
                             telephone2: String,
                             email2: String,
                             oop_check_status_code: String)

  case class dm_custom(acct_cycle: Date,
                       account_id: String,
                       actual_connect: String,
                       JPO_ACCT_STOCK_JOIN_NUM: String,
                       JPO_ACCT_INVAL_JOIN_NUM: String,

                       ADD_CONNECT_NUM: String,
                       JPO_ACCT_EXPAND_JOIN_NUM: String,
                       JPO_ACCT_CLEAR_JOIN_NUM: String,
                       acc_acti_connect_num: String,
                       //TOTAL_ACCT_NUMS:String,
                       new_connect_num: String,

                       jpo_acct_plan_facility_num: String,
                       JPO_ACCT_SMS_NUM: String,
                       JPO_ACCT_DATA_NUM: String,
                       JPO_ACCT_VOI_NUM: String,
                       live_connect_num: String,

                       LIVE_CONNECT_PROP: String,
                       off_connect_num: String,
                       acc_off_connect_num: String,
                       rate_plan_num: String,
                       rate_plan_type_num: String,

                       jpo_current_clear_join_num: String,
                       jpo_current_inval_join_num: String,
                       current_acti_num: String,
                       company_stock_connect_num: String,
                       is_chinaunicom_sole: String,

                       year_pre_connect_num: String,
                       stage: String,
                       province: String,
                       city: String,
                       principle_industry: String,

                       industry_main_product: String,
                       industry_detail_product: String,
                       customer_degree: String,
                       customer_name: String,
                       customer_group_id: String,

                       customer_id: String,
                       virture_id: String,
                       first_order_time: String,
                       commerce_model: String,
                       facility_type: String,

                       customer_manager: String,
                       telephone: String,
                       email: String,
                       support_manager: String,
                       telephone2: String,

                       email2: String)

  case class wlw_user(cust_id: String, wlw_num: String)

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
      .config("spark.executor.memory", "100g")
      .config("spark.cores.max", "72")
      .config("spark.dynamicAllocation.enabled", "false")
      .master(s"spark://$master:$port")
      .getOrCreate()
    val sc = spark.sparkContext

    val day_logic = args(0)

    val last_month_day_file = Util_date.get_file_month_last_day(day_logic)

    val data_input_AcctSnapshot = input_path + "xxxx_" + day_logic + "_xx_*"
    val data_input_subs_snapshot = input_path + "xxxx_" + day_logic + "_xx_*"
    val data_input_last_month_subs_snapshot = input_path + "xxxx_" + last_month_day_file + "_xx_*"
    val data_input_Rate_Plan_Snapshot = input_path + "xxxx_" + day_logic + "_xx_*.dat"

    val days_period = Util_date.get_account_period_day_list(day_logic)

    //增量日数据

    var data_input_SubsChanges = spark.read.textFile(input_path + "JWCC_" + days_period(0) + "_SubsChanges_*")
    for (index <- 1 until days_period.length) {
      data_input_SubsChanges = data_input_SubsChanges.union(spark.read.textFile(input_path + "JWCC_" + days_period(index) + "_SubsChanges_*"))
    }

    //累计数据用量
    var data_input_Usage_Detail_Data = spark.read.textFile(input_path + "JWCC_" + days_period(0) + "_DataUsage_*")
    for (index <- 1 until days_period.length) {
      data_input_Usage_Detail_Data = data_input_Usage_Detail_Data.union(spark.read.textFile(input_path + "JWCC_" + days_period(index) + "_DataUsage_*"))
    }

    //累计短信用量
    var data_input_Usage_Detail_SMS = spark.read.textFile(input_path + "JWCC_" + days_period(0) + "_SMSUsage_*")
    for (index <- 1 until days_period.length) {
      data_input_Usage_Detail_SMS = data_input_Usage_Detail_SMS.union(spark.read.textFile(input_path + "JWCC_" + days_period(index) + "_SMSUsage_*"))
    }

    //累计语音用量
    var data_input_Usage_Detail_Voice = spark.read.textFile(input_path + "JWCC_" + days_period(0) + "_VoiceUsage_*")
    for (index <- 1 until days_period.length) {
      data_input_Usage_Detail_Voice = data_input_Usage_Detail_Voice.union(spark.read.textFile(input_path + "JWCC_" + days_period(index) + "_VoiceUsage_*"))
    }


    val data_input_bc = businesschance_path + "xx_*"
    val data_input_user = hdfs_path + "/xx"





    //注意：要使用Spark SQL的内置函数，就一定要导入SQLContext下的隐式转换
    import spark.implicits._

    val acct_snapshot_df = spark.read.textFile(data_input_AcctSnapshot)
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 6)
      .map(splits => acct_snapshot(
      day_logic, splits(0), splits(1), splits(6),
      splits(2),
      splits(5)
    ))
      .filter("account_status = 'A' and account_billable_flag ='Y' and account_name like 'Company%'")

      .toDF()


    val subs_snapshot_df = spark.read.textFile(data_input_subs_snapshot)
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 6)
      .map(splits => subs_snapshot(splits(0), splits(3), splits(10), splits(5), splits(4)
    ))
      .toDF()


    val last_month_subs_snapshot_df = spark.read.textFile(data_input_last_month_subs_snapshot)
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 6)
      .map(splits => last_month_subs_snapshot(splits(0), splits(3), splits(10), splits(5), splits(4)))
      .filter("sim_state!='9' and sim_state!='13' ")
      .groupBy("acctID")
      .agg(countDistinct("iccid").as("JPO_ACCT_STOCK_JOIN_NUM"))
      .toDF()

    val period = Util_date.get_account_period_2utc(day_logic)

    val subs_changes_df = data_input_SubsChanges
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 6)
      .map(splits => subs_changes(splits(4), splits(5).toInt, splits(6), splits(7))).toDF()
      .filter("change_datetime >= '"+period(0)+"' and change_datetime < '"+period(1)+"'")


    val Usage_Detail_SMS_df = data_input_Usage_Detail_SMS
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 18)
      .map(splits => Usage_Detail_SMS(
      splits(4), splits(12), 1, splits(1), 1
    )).toDF()
      .filter("received_date >= '"+period(0)+"' and received_date < '"+period(1)+"'")


    val Usage_Detail_Data_df = data_input_Usage_Detail_Data
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 20)
      .map(splits => Usage_Detail_Data(
      splits(4), splits(12), splits(16).toLong, splits(1),splits(16).toInt
    )).toDF()
      .filter("received_date >= '"+period(0)+"' and received_date < '"+period(1)+"'")
      .filter("rounded > 0 ")


    val Usage_Detail_Voice_df = data_input_Usage_Detail_Voice
      .map(_.split("\\|", -1))
      .filter(splits => splits.length > 18)
      .map(splits => Usage_Detail_Voice(
      splits(4), splits(12), splits(16).toLong, splits(1),splits(16).toInt
    )).toDF()
      .filter("received_date >= '"+period(0)+"' and received_date < '"+period(1)+"'")
      .filter("rounded > 0 ")


    Usage_Detail_SMS_df.groupBy("acctID")
      .agg(count(lit(1)).as("JPO_ACCT_SMS_NUM"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_Usage_Detail_SMS")

    Usage_Detail_Data_df.groupBy("acctID")
      .agg(sum("JPO_ACCT_DATA_NUM").as("JPO_ACCT_DATA_NUM"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_Usage_Detail_Data")

    Usage_Detail_Voice_df.groupBy("acctID")
      .agg(sum("JPO_ACCT_VOI_NUM").as("JPO_ACCT_VOI_NUM"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_Usage_Detail_Voice")


    Usage_Detail_SMS_df
      .union(Usage_Detail_Data_df)
      .union(Usage_Detail_Voice_df)
      .groupBy("acctID")
      .agg(countDistinct("iccid").as("live_connect_num"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_live_connect_num")



    subs_snapshot_df
      .filter("ini_act_date >= '"+period(0)+"' and ini_act_date < '"+period(1)+"'")
      .groupBy("acctID")
      .agg(count(lit(1)).as("new_connect_num"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_new_connect_num")

    subs_snapshot_df
      .filter("ini_act_date is not null and ini_act_date != ''")
      .groupBy("acctID")
      .agg(count(lit(1)).as("acc_acti_connect_num"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_acc_acti_connect_num")

    subs_snapshot_df
      .filter("sim_state = '6'")
      .groupBy("acctID")
      .agg(count(lit(1)).as("current_acti_num"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_current_acti_num")


    subs_changes_df
      .filter("change_type = 3 and new_value in ('Purged','Replaced')")
      .groupBy("acctID")
      .agg(count(lit(1)).as("off_connect_num"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_off_connect_num")


    subs_snapshot_df
      .filter("sim_state = '9' or sim_state = '13' ")
      .groupBy("acctID")
      .agg(count(lit(1)).as("acc_off_connect_num"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_acc_off_connect_num")


    subs_snapshot_df
      .filter("sim_state = '9'")
      .groupBy("acctID")
      .agg(count(lit(1)).as("JPO_ACCT_CLEAR_JOIN_NUM"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_JPO_ACCT_CLEAR_JOIN_NUM")


    subs_changes_df
      .filter("change_type = 3 and new_value = 'Purged'")
      .groupBy("acctID")
      .agg(count(lit(1)).as("jpo_current_clear_join_num"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_jpo_current_clear_join_num")


    subs_snapshot_df
      .filter("sim_state = '8'")
      .groupBy("acctID")
      .agg(count(lit(1)).as("JPO_ACCT_INVAL_JOIN_NUM"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_JPO_ACCT_INVAL_JOIN_NUM")

    subs_changes_df
      .filter("change_type = 3 and new_value = 'Retired'")

      .groupBy("acctID")
      .agg(count(lit(1)).as("jpo_current_inval_join_num"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_jpo_current_inval_join_num")



    val Rate_Plan_Snapshot_DF = spark.read.textFile(data_input_Rate_Plan_Snapshot)
      .map(_.split("\\|"))
      .map(splits => Rate_Plan_Snapshot(splits(2), splits(0),splits(3)))
      .toDF()
    Rate_Plan_Snapshot_DF.groupBy("acctID")
      .agg(countDistinct("rate_plan_id").as("rate_plan_num"),  countDistinct("rate_plan_type").as("rate_plan_type_num"))
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_Rate_Plan_Snapshot")


    val business_chance_df = spark.read.textFile(data_input_bc)
      .map(_.split("\\u0001", -1))
      .filter(_.length > 53)
      .map(splits =>
      business_chance(if (splits(52) != "") splits(52) else "0",
        splits(53),
        if (splits(36) != "") splits(36) else "0",
        splits(11),
        splits(24),
        splits(25),
        splits(27),
        splits(28),
        splits(29),
        splits(30),
        splits(31),
        splits(32),
        splits(33),
        splits(37),
        splits(39),
        splits(40),
        splits(45),
        splits(46),
        splits(47),
        splits(48),
        splits(49),
        splits(50),
        splits(10)
      )).toDF()

    val wlw_user_df = spark.read.textFile(data_input_user)
      .map(_.split("\\|", -1))
      .map(splits => wlw_user(splits(3), splits(7))).toDF()

    acct_snapshot_df.createOrReplaceTempView("AcctSnapshot")
    subs_snapshot_df.createOrReplaceTempView("SubsSnapshot")
    subs_changes_df.createOrReplaceTempView("SubsChanges")

    last_month_subs_snapshot_df.createOrReplaceTempView("last_month_subs_snapshot")

    business_chance_df.createOrReplaceTempView("BusinessChance")
    wlw_user_df.createOrReplaceTempView("WLWUser")


    spark.sql("SELECT ss.acctID, count(1) as actual_connect " +
      "FROM SubsSnapshot ss " +
      "where sim_state!='9' and sim_state!='13' " +
      "group by ss.acctID")
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_actual_connect_pre")

    spark.sql("SELECT as1.acct_cycle, as1.acctID, as1.operator_id as operator_id, " +
      "sum(ss.actual_connect) as actual_connect " +
      "FROM AcctSnapshot as1 left join view_actual_connect_pre ss on as1.acctID = ss.acctID " +
      "where 1=1 " +
      "group by as1.acct_cycle, as1.acctID, as1.operator_id")
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_actual_connect")




    spark.sql("SELECT ss.acctID, " +
      "count(distinct(ss.rate_plan_id)) as jpo_acct_plan_facility_num " +
      "From SubsSnapshot ss " +
      "group by ss.acctID")
      .repartition(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("view_jpo_acct_plan_facility_num")


    spark.sql("SELECT vac.acct_cycle, vac.acctID as account_id, " +
      "vac.operator_id as virture_id, " +

      "lmss.JPO_ACCT_STOCK_JOIN_NUM as JPO_ACCT_STOCK_JOIN_NUM," +
      "vjaijn.JPO_ACCT_INVAL_JOIN_NUM as JPO_ACCT_INVAL_JOIN_NUM," +
      "vjacjn.JPO_ACCT_CLEAR_JOIN_NUM as JPO_ACCT_CLEAR_JOIN_NUM," +

      "(vac.actual_connect - lmss.JPO_ACCT_STOCK_JOIN_NUM) as ADD_CONNECT_NUM," +
      "(vac.actual_connect - lmss.JPO_ACCT_STOCK_JOIN_NUM + off_connect_num) as JPO_ACCT_EXPAND_JOIN_NUM," +

      "wu.cust_id as customer_id, " +

      "actual_connect, " +
      "acc_acti_connect_num, " +
      "new_connect_num, " +


      "uds.JPO_ACCT_SMS_NUM as JPO_ACCT_SMS_NUM, " +
      "udd.JPO_ACCT_DATA_NUM as JPO_ACCT_DATA_NUM, " +
      "udv.JPO_ACCT_VOI_NUM as JPO_ACCT_VOI_NUM," +

      "vlcn.live_connect_num as live_connect_num," +
      "(vlcn.live_connect_num/actual_connect) as LIVE_CONNECT_PROP," +
      "vrps.rate_plan_num," +
      "vrps.rate_plan_type_num," +
      "off_connect_num, " +
      "acc_off_connect_num, " +
      "vccjn.jpo_current_clear_join_num, " +
      "vcijn.jpo_current_inval_join_num, " +
      "current_acti_num, " +

      "jpo_acct_plan_facility_num " +


      "FROM view_actual_connect vac left join view_new_connect_num vncn on vac.acctID = vncn.acctID " +

      "left join view_off_connect_num vocn on vac.acctID = vocn.acctID " +
      "left join view_jpo_acct_plan_facility_num vjacp on vac.acctID=vjacp.acctID " +
      "left join view_acc_acti_connect_num vaccc on vac.acctID=vaccc.acctID " +

      "left join last_month_subs_snapshot lmss on vac.acctID = lmss.acctID " +

      "left join view_JPO_ACCT_INVAL_JOIN_NUM vjaijn on vac.acctID = vjaijn.acctID " +
      "left join view_JPO_ACCT_CLEAR_JOIN_NUM vjacjn on vac.acctID = vjacjn.acctID " +

      "left join view_Usage_Detail_SMS uds on vac.acctID = uds.acctID " +
      "left join view_Usage_Detail_Data udd on vac.acctID = udd.acctID " +
      "left join view_Usage_Detail_Voice udv on vac.acctID = udv.acctID " +
      "left join view_live_connect_num vlcn on vac.acctID = vlcn.acctID " +
      "left join view_acc_off_connect_num vaocn on vac.acctID = vaocn.acctID " +
      "left join view_jpo_current_clear_join_num vccjn on vac.acctID = vccjn.acctID " +
      "left join view_jpo_current_inval_join_num vcijn on vac.acctID = vcijn.acctID " +
      "left join view_current_acti_num vccn on vac.acctID = vccn.acctID " +


      "left join view_Rate_Plan_Snapshot vrps on vac.acctID = vrps.acctID " +

      "join WLWUser wu on vac.operator_id = wu.wlw_num").createOrReplaceTempView("Jasper_User")



    spark.sql("SELECT " +
      "bc.company_stock_connect_num, " +
      "bc.is_chinaunicom_sole, " +
      "bc.year_pre_connect_num, " +
      "bc.stage, " +
      "bc.province, " +
      "bc.city, " +
      "bc.principle_industry, " +
      "bc.industry_main_product, " +
      "bc.industry_detail_product, " +
      "bc.customer_degree, " +
      "bc.customer_name, " +
      "bc.customer_group_id, " +
      "bc.customer_id, " +
      "bc.first_order_time, " +
      "bc.commerce_model, " +
      "bc.facility_type, " +
      "bc.customer_manager, " +
      "bc.telephone, " +
      "bc.email, " +
      "bc.support_manager, " +
      "bc.telephone2, " +
      "bc.email2 " +
      "FROM BusinessChance bc " +
      "group by " +
      "bc.company_stock_connect_num, " +
      "bc.is_chinaunicom_sole, " +
      "bc.year_pre_connect_num, " +
      "bc.stage, " +
      "bc.province, " +
      "bc.city, " +
      "bc.principle_industry, " +
      "bc.industry_main_product, " +
      "bc.industry_detail_product, " +
      "bc.customer_degree, " +
      "bc.customer_name, " +
      "bc.customer_group_id, " +
      "bc.customer_id, " +
      "bc.first_order_time, " +
      "bc.commerce_model, " +
      "bc.facility_type, " +
      "bc.customer_manager, " +
      "bc.telephone, " +
      "bc.email, " +
      "bc.support_manager, " +
      "bc.telephone2, " +
      "bc.email2 "
    ).createOrReplaceTempView("BusinessChance")


    val out_df = spark.sql("SELECT TO_DATE(CAST(unix_timestamp(ju.acct_cycle,'yyyyMMdd') AS TIMESTAMP)) as acct_cycle, " +
      "ju.account_id, " +
      "ju.actual_connect, " +

      "ju.JPO_ACCT_STOCK_JOIN_NUM, " +
      "ju.JPO_ACCT_INVAL_JOIN_NUM, " +
      "ju.ADD_CONNECT_NUM, " +
      "ju.JPO_ACCT_EXPAND_JOIN_NUM, " +
      "ju.JPO_ACCT_CLEAR_JOIN_NUM, " +

      "ju.acc_acti_connect_num, " +
      //"ju.TOTAL_ACCT_NUMS, " +
      "ju.new_connect_num, " +

      "ju.jpo_acct_plan_facility_num, " +

      "ju.JPO_ACCT_SMS_NUM, " +
      "ju.JPO_ACCT_DATA_NUM, " +
      "ju.JPO_ACCT_VOI_NUM, " +

      "ju.live_connect_num, " +
      "LIVE_CONNECT_PROP," +

      "ju.off_connect_num, " +
      "acc_off_connect_num, " +

      "rate_plan_num," +
      "rate_plan_type_num," +
      "ju.jpo_current_clear_join_num," +
      "ju.jpo_current_inval_join_num," +
      "current_acti_num," +

      "bc.company_stock_connect_num, " +
      "bc.is_chinaunicom_sole, " +
      "bc.year_pre_connect_num, " +
      "bc.stage, " +
      "bc.province, " +
      "bc.city, " +
      "bc.principle_industry, " +
      "bc.industry_main_product, " +
      "bc.industry_detail_product, " +
      "bc.customer_degree, " +
      "bc.customer_name, " +
      "bc.customer_group_id, " +
      "bc.customer_id, " +
      "ju.virture_id, " +
      "bc.first_order_time, " +
      "bc.commerce_model, " +
      "bc.facility_type, " +
      "bc.customer_manager, " +
      "bc.telephone, " +
      "bc.email, " +
      "bc.support_manager, " +
      "bc.telephone2, " +
      "bc.email2 " +
      "FROM Jasper_User ju left join BusinessChance bc on ju.customer_id = bc.customer_id "

    )
      .na.fill(Map(
      "actual_connect" -> "0",

      "JPO_ACCT_STOCK_JOIN_NUM" -> "0",
      "JPO_ACCT_INVAL_JOIN_NUM" -> "0",
      "ADD_CONNECT_NUM" -> "0",
      "JPO_ACCT_EXPAND_JOIN_NUM" -> "0",
      "JPO_ACCT_CLEAR_JOIN_NUM" -> "0",

      "acc_acti_connect_num" -> "0",
     // "TOTAL_ACCT_NUMS" -> "0",
      "new_connect_num" -> "0",
      "off_connect_num" -> "0",
      "jpo_acct_plan_facility_num" -> "0",

      "JPO_ACCT_SMS_NUM" -> "0",
      "JPO_ACCT_DATA_NUM" -> "0",
      "JPO_ACCT_VOI_NUM" -> "0",

      "live_connect_num" -> "0",
      "LIVE_CONNECT_PROP" -> "0",
      "rate_plan_num" ->"0",
      "rate_plan_type_num" ->"0",
      "off_connect_num" ->"0",
      "acc_off_connect_num" ->"0",
      "new_connect_num" ->"0",
      "jpo_current_clear_join_num" ->"0",
      "jpo_current_inval_join_num" ->"0",
      "current_acti_num" ->"0",

      "company_stock_connect_num" -> "0"

    ))






    out_df.repartition(1).rdd.saveAsTextFile(data_output)


    val connectProperties = new Properties()
    connectProperties.put("user", "xx")
    connectProperties.put("password", "xx")
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val mysqlDriverUrl = "jdbc:mysql://xxxx"

    val dm_custom_df = spark.read.textFile(data_output+"part-00000")
      .map(_.replace("[","").replace("]","").replace("null","").split("\\,", -1))
      .filter(splits => splits.length > 43)
      .map(splits => dm_custom(
      Util_date.str2date(splits(0), "yyyy-MM-dd"),
      //splits(0),
      splits(1),
      splits(2),
      splits(3),
      splits(4),

      splits(5),
      splits(6),
      splits(7),
      splits(8),
      splits(9),

      splits(10),
      splits(11),
      splits(12),
      splits(13),
      splits(14),

      splits(15),
      splits(16),
      splits(17),
      splits(18),
      splits(19),
      //splits(20),

      if (splits(20).isEmpty)"0" else splits(20),
      splits(21),
      splits(22),
      splits(23),
      splits(24),
      //splits(25),

      if (splits(25).isEmpty)"0" else splits(25),
      splits(26),
      splits(27),
      splits(28),
      splits(29),

      splits(30),
      splits(31),
      splits(32),
      splits(33),
      splits(34),

      splits(35),
      splits(36),
      splits(37),
      splits(38),
      splits(39),

      splits(40),
      splits(41),
      splits(42),
      splits(43),
      splits(44),

      splits(45)
      //splits(46)
    )
      ).na.fill(Map(
      "actual_connect" -> "0",

      "JPO_ACCT_STOCK_JOIN_NUM" -> "0",
      "JPO_ACCT_INVAL_JOIN_NUM" -> "0",
      "ADD_CONNECT_NUM" -> "0",
      "JPO_ACCT_EXPAND_JOIN_NUM" -> "0",
      "JPO_ACCT_CLEAR_JOIN_NUM" -> "0",

      "acc_acti_connect_num" -> "0",
      "new_connect_num" -> "0",
      "off_connect_num" -> "0",
      "jpo_acct_plan_facility_num" -> "0",

      "JPO_ACCT_SMS_NUM" -> "0",
      "JPO_ACCT_DATA_NUM" -> "0",
      "JPO_ACCT_VOI_NUM" -> "0",

      "live_connect_num" -> "0",
      "LIVE_CONNECT_PROP" -> "0",
      "rate_plan_num" ->"0",
      "rate_plan_type_num" ->"0",
      "off_connect_num" ->"0",
      "acc_off_connect_num" ->"0",
      "new_connect_num" ->"0",
      "jpo_current_clear_join_num" ->"0",
      "jpo_current_inval_join_num" ->"0",
      "current_acti_num" ->"0",

      "YEAR_PRE_CONNECT_NUM" ->"0",
      "company_stock_connect_num" ->"0"

    ))

    dm_custom_df.write.mode(SaveMode.Append).jdbc(mysqlDriverUrl, "iotoperation.xxxx", connectProperties)


  }
}
