// Databricks notebook source
// MAGIC %md
// MAGIC #### purpose
// MAGIC - the purpose of this uat is to assess how the new suite of consumer tables will affect the existing pltv pipeline
// MAGIC - the ticket is not to make the changes to code that will enable smooth functioning of the pipeline, but to assess the discprepancies one might encounter when we entirely migrate to the new consumer tables
// MAGIC 
// MAGIC #### inference
// MAGIC - as noted in the previous ltv uat, not all features used in the pltv pipeline are currently present in the new facts_device_activity table. while we can get some/most of the features from beacons, and metric loader, it is recommended to revisit the features used in ltv
// MAGIC - why? there was no exploratory analysis conducted while including features in the pltv pipeline. this pipeline is expensive and its suggested to revisit these features as most of them might not contribute to the target in anyway
// MAGIC - also, given the ios14 update, the pipeline will be used to assess only android users. so its a good time to redo these checks from the perspective of cost 
// MAGIC - the pltv pipelines are still in old user id definition and migration is deemed necessary and long overdue to match with rest of the reporting
// MAGIC - the analysis below is conducted with new user definition in mind
// MAGIC - we notice an increase in user count based on new suite of tables. however most/all of the additional users in the new suite of tables contribute to 0 revenue(atleast on day 0). this is just based on one day of user sample. if this is the general trend, we will see a decrease to all of the per user metrics. 
// MAGIC - the new suite of tables also have a new acquisition logic (the first acq source the user ever came with is chosen). this is different from the existing logic. however i do not see a media source called "untrackable". waiting to verify with the DIET team on what the previously called 'untrackables' are now grouped under

// COMMAND ----------

//explore the new suite of tables

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import com.flipp.datalib.fieldparsers.{UserId, GeoMapping}
import org.apache.spark.sql.types._
import com.flipp.datalib.fieldparsers._
import com.flipp.datalib.fieldparsers
val userIDUDF = udf((accountGuid: String, sid: String) => UserId.parse(accountGuid, sid)) 
val organic = Seq("organic", "Organic Blog", "Organic Facebook", "Organic Instagram", "Organic Social", "Organic Soial", "Organic Twitter")
val clean_media_source = Seq("googleadwordsoptimizer_int", "Facebook Ads", "Twitter", "googleadwords_int", "inmobi_int", "pinterest_int", "Apple Search Ads", "Facebook", "Google")
val blacklistedSids = fieldparsers.UserId.SID_BLACKLIST

// COMMAND ----------

val userInfo = spark.table("gold.gold_user_info")
val deviceInfo = spark.table("gold.gold_device_info")
val userDeviceMapping = spark.table("gold.gold_user_device_mapping")

// COMMAND ----------

val device_activity = spark.table("gold.facts_device_activity")
display(device_activity.groupBy('user_id)
                       .agg(countDistinct('device_id) as "sidCount", 
                           min('date) as "birth_date")
              
       )

// COMMAND ----------

display(userInfo)

// COMMAND ----------

// MAGIC %md #### user info

// COMMAND ----------

// MAGIC %md ###### assess the change in user counts produced by both gold layer pipeline tables and the new gold suite tables

// COMMAND ----------

/// pull users from gold layer pipeline
def pullUsersWithBday(bdayMin: String, bdayMax: String, ltvHorizonLength: Int, userInfoDate: String,
                      userInfo: DataFrame = spark.table("gold_layer_pipeline.user_info")): DataFrame = {
  println("Calling pullUsersWithBday")
  userInfo
    .where('date === userInfoDate)
    .filter('first_device_platform.isin("Android", "iOS") 
            && !$"user_id".isin("unknown") &&
            col("birth_date").between(lit(bdayMin), lit(bdayMax))
            )
    .select(col("user_id"), col("birth_date").cast(DateType).as("birth_date"))
    .distinct
    .withColumn("birth_date_plus_horizon", date_add(col("birth_date"), ltvHorizonLength - 1).cast(DateType))
}

// COMMAND ----------

//with existing userinfo table 
val olduinf = pullUsersWithBday("2021-04-19", "2021-04-19", 365, "2021-04-19")
olduinf.select('user_id).distinct.count

// COMMAND ----------

olduinf.select('user_id).count //no duplicates

// COMMAND ----------

/// pull users from gold suite of tables
def pullUsersWithBday(bdayMin: String, bdayMax: String, ltvHorizonLength: Int, userInfoDate: String,
                      userInfo: DataFrame = spark.table("gold_layer_pipeline.user_info")): DataFrame = {
  println("Calling pullUsersWithBday")
  userInfo
    .where('date === userInfoDate)
    .filter('first_device_platform.isin("Android", "iOS") 
            && !$"user_id".isin("unknown") &&
            col("birth_date").between(lit(bdayMin), lit(bdayMax))
            )
    .select(col("user_id"), col("birth_date").cast(DateType).as("birth_date"))
    .distinct
    .withColumn("birth_date_plus_horizon", date_add(col("birth_date"), ltvHorizonLength - 1).cast(DateType))
    .withColumnRenamed("first_device_platform", "device_platform")
}

// COMMAND ----------

val newunifDf = spark.table("gold.gold_user_info")
                     .withColumn("os_size", size('all_device_os))
                     .withColumn("first_device_platform", $"all_device_os"($"os_size"-1))
                    .filter('first_device_platform.isin("Android", "iOS"))
                    .drop("user_id.")

val newuinf = pullUsersWithBday("2021-04-19", "2021-04-19", 365, "2021-04-19", newunifDf)
newuinf.select('user_id).distinct.count

//73% increase in users 

// COMMAND ----------

newuinf.select('user_id).count

// COMMAND ----------

newunifDf.select('first_device_platform).distinct.show

// COMMAND ----------

val outerJoinedDf = newuinf.withColumnRenamed("user_id", "new_user_id")
                           .drop("birth_date", "birth_date_plus_horizon")
                           .join(olduinf, $"user_id" === $"new_user_id", "outer")
                           .withColumn("notpresentinold", when((!($"new_user_id".isNull) && 'user_id.isNull), lit(1)).otherwise(lit(0)))


display(outerJoinedDf)

// COMMAND ----------

outerJoinedDf.where('notpresentinold === 1).select('new_user_id).distinct.count

// COMMAND ----------

//lets see is there is something unusual about these guids that are not present in the gold layer pipeline tables
val guidsNotPresentInOld = outerJoinedDf.where('notpresentinold === 1).select('new_user_id as "user_id")

// COMMAND ----------

display(newunifDf.join(guidsNotPresentInOld, Seq("user_id"), "inner"))
//nothing unusual about these users. 

// COMMAND ----------

// MAGIC %md ### acquisition sources 

// COMMAND ----------

//what would be the split in acq sources as per the new data vs the old data tables

// COMMAND ----------

//the new user info from gold suite of tables has the first acq source of a user - ie first obtained media source
//for the gold layer pipeline user info , use the logic used in ltv to obtain the media source. assess the change in distributions

def newAcqLogic(tempDir: String, df: DataFrame): DataFrame ={
      println("Calling new acquisition logic")
  val jdbcUsername = dbutils.secrets.get(scope = "datascience", key = "jdbcUsername_redshift")
  val jdbcPassword = dbutils.secrets.get(scope = "datascience", key = "jdbcPassword_redshift")
  val jdbcHostname = "10.0.1.119"
  val jdbcPort = 5439
  val jdbcDatabase = "analyticsproduction"
  val jdbcUrl = s"jdbc:redshift://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}&sslfactory=org.postgresql.ssl.NonValidatingFactory "
  
  
      val newuserIds = df
                        .select('user_id)
                        .distinct
                        .coalesce(1)
                        .cache() 
      print("check 1")
       val uds_first_sid = spark
                              .table("silver_layer_pipeline.user_daily_summary")
                              .filter('device_platform.isin("iOS", "Android"))
                              .select('user_id, 
                                      'sid, 
                                      'date
                                     )
                              .join(broadcast(newuserIds), Seq("user_id"), "right")
                              .withColumn("sid_flag", row_number().over(Window.partitionBy('user_id).orderBy('date.asc)))
                              .filter('sid_flag === 1)//filter out only the users in newDf
                              .select('user_id, 
                                      'sid
                                     )
      print("check 2")
      val rawAcqSourcesDF = spark
                                .read
                                .format("com.databricks.spark.redshift")
                                .option("url", jdbcUrl) 
                                .option("tempdir", tempDir) 
                                .option("forward_spark_s3_credentials", true)
                                .option("query", "select * from public.user_acquisition_sources")
                                .load()
                                .filter('device_platform.isin("iOS", "Android")
                                        && !trim('sid).isin(blacklistedSids:_*)
                                       )
  
                                .withColumn("proposed_agg_source", 
                                                           when('media_source.isin("Blacklisted"), lit("Blacklisted"))
                                                          .when('media_source.isin("Untrackable"), lit("Untrackable"))
                                                          .when('media_source.isin(organic: _*) , lit("Organic"))
                                                          .when('media_source.isNull || lower('media_source).isin("unknown", "null"), lit("Unknown"))
                                                          .otherwise(lit("Paid"))
                                           )
                                .withColumn("media_source_clean", 
                                                        when('media_source.isin(clean_media_source: _*), 'media_source)
                                                       .otherwise(lit("Other"))
                                           )
                                .select('sid, 
                                        'media_source as "proposed_media_source",
                                        'proposed_agg_source
                                        )
                                .cache()
      print("check 3")
      val finalnewDf = 
                rawAcqSourcesDF
                        .join(broadcast(uds_first_sid), Seq("sid"), "right")
                        .select('user_id, 
                                'proposed_media_source, 
                                'proposed_agg_source
                               )
    print("check 4")
     df
      .join(finalnewDf, Seq("user_id"), "left")
//       .withColumn("proposed_media_source", when('media_source === "Blacklisted", coalesce('proposed_media_source, 'media_source))
//                                           .otherwise('proposed_media_source)
//                  )
//       .withColumn("proposed_agg_source", when('agg_source === "Blacklisted", coalesce('proposed_agg_source, 'agg_source))
//                                         .otherwise('proposed_agg_source)
//                 )
      .drop("media_source", "agg_source")
      .withColumnRenamed("proposed_media_source", "media_source")
      .withColumnRenamed("proposed_agg_source", "agg_source")
      .distinct
  
}


val olduinfWithAcq = newAcqLogic("s3a://flipp-datalake/data/redshift/tmp_etls", olduinf)

 


// COMMAND ----------

display(olduinfWithAcq)

// COMMAND ----------

val newinfWithAcq = newunifDf
                                .withColumn("proposed_agg_source", 
                                                           when('media_source.isin("Blacklisted"), lit("Blacklisted"))
                                                          .when('media_source.isin("Untrackable"), lit("Untrackable"))
                                                          .when('media_source.isin(organic: _*) , lit("Organic"))
                                                          .when('media_source.isNull || lower('media_source).isin("unknown", "null"), lit("Unknown"))
                                                          .otherwise(lit("Paid"))
                                           )
                                .withColumn("media_source_clean", 
                                                        when('media_source.isin(clean_media_source: _*), 'media_source)
                                                       .otherwise(lit("Other"))
                                           )
                               .select('user_id, 
                                        'media_source as "proposed_media_source",
                                        'proposed_agg_source
                                        )
display(newinfWithAcq)

// COMMAND ----------

// MAGIC %md #### media source distribution change

// COMMAND ----------

display(newinfWithAcq.groupBy('proposed_media_source)
        
                     .agg(countDistinct('user_id) as "userCount")
                     .withColumn("percentage_users", round(('userCount/(sum('userCount) over()))*100, 0))
        )

// COMMAND ----------

display(olduinfWithAcq.groupBy('media_source)
        
                     .agg(countDistinct('user_id) as "userCount")
                     .withColumn("percentage_users", round(('userCount/(sum('userCount) over()))*100, 0))
        )

// COMMAND ----------

// MAGIC %md #### agg source distribution change

// COMMAND ----------

display(newinfWithAcq.groupBy('proposed_agg_source)
        
                     .agg(countDistinct('user_id) as "userCount")
                     .withColumn("percentage_users", round(('userCount/(sum('userCount) over()))*100, 0))
        )

// COMMAND ----------

display(olduinfWithAcq.groupBy('agg_source)
        
                     .agg(countDistinct('user_id) as "userCount")
                     .withColumn("percentage_users", round(('userCount/(sum('userCount) over()))*100, 0))
        )

// COMMAND ----------

// MAGIC %md ### User daily summary

// COMMAND ----------

//previously
val olduds = spark
        .table("silver_layer_pipeline.user_daily_summary")
display(olduds)

// COMMAND ----------

//new suite of tables
val newuds = spark
        .table("gold.facts_device_activity")
         .withColumn("postal_size", size('all_postal_codes))
         .withColumn("postal_code", $"all_postal_codes"($"postal_size"-1))
        .select('user_id, 'device_id as "sid", 'device_os as "device_platform", 'postal_code, 'app_opens, 'evs, 'uevs, 'premium_uevs, 'clippings, 'ttms, 'shares, 'date)

display(newuds)

// COMMAND ----------

List(olduds.columns)

// COMMAND ----------

List(newuds.columns)

// COMMAND ----------

//find common columns
(olduds.columns).intersect(newuds.columns)

// COMMAND ----------

//the column names have changed. so for the common columns we manually find them and put it in a list - ugh
('user_id, 'device_id as "sid", 'device_os as "device_platform", 'app_opens, 'evs, 'uevs, 'premium_uevs, 'clippings, 'ttms, 'shares, 'date)

// COMMAND ----------

// DBTITLE 1,helper functions - already reviewed
import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS

def generateDateListStr(dateStart: String, dateEnd: String): Vector[String] = {
  
  val start = LocalDate.parse(dateStart)
  val end = LocalDate.parse(dateEnd)
  val daysDiff = DAYS.between(start, end).toInt + 1

  Vector
    .tabulate(daysDiff)(identity)
    .map(i => start.plusDays(i).toString)
}

val generateDateListCol = udf((dateStart: String, dateEnd: String) => generateDateListStr(dateStart, dateEnd))


def getCountry = udf((postal_code: String) => GeoMapping.inferCountryFromPostalCode(postal_code))

/****** Helper Methods *********/
def aggColOnDayX(udsCol: String, i: Int): (Column, String) = {
  
  val name = s"uds_${udsCol}_day_${i}"
  val column = sum(when(col("metric_date") === date_add(col("birth_date"), i), col(udsCol))).as(name)
  
  (column, name)
}

def aggColBetweenBdayAndDayX(udsCol: String, i: Int): (Column, String) = {
  
  val name = s"uds_${udsCol}_first_days_${i}"
  val birthDatePlusX = date_add(col("birth_date"), i)
  val minDate = least(birthDatePlusX, col("window_end_date"))
  
  val column = sum(when(col("metric_date") <= minDate, col(udsCol))).as(name)
  
  (column, name)
}

def aggColBetweenLastXDays(udsCol: String, i: Int): (Column, String) = {
  
  val name = s"uds_${udsCol}_last_days_${i}"
  val birthDateMinusX = date_add(col("window_end_date"), -i)
  val maxDate = greatest(birthDateMinusX, col("birth_date"))
  
  val column = sum(when(col("metric_date") >= maxDate, col(udsCol))).as(name)
  
  (column, name)
}

def aggByRangeByColumn(
                        range: Vector[Int], 
                        udsCols: Vector[String],
                        aggFunc: (String, Int) => (Column, String)): Vector[(Column, String)] = {
  
  udsCols
    .flatMap(
      udsCol => range.map(i => aggFunc(udsCol, i))
    )
}

def aggCumByColumn(udsCols: Vector[String]): Vector[(Column, String)] = {
  def aggFuncCum(udsCol: String): Column = sum(when(col("metric_date") <= col("window_end_date"), col(udsCol))).as(s"uds_${udsCol}_cum")
  udsCols.map(udsCol => (aggFuncCum(udsCol), s"uds_${udsCol}_cum"))
}

// COMMAND ----------

val udsCols = Vector("app_opens", "evs", "uevs", "premium_uevs", "clippings", "ttms", "shares")

// COMMAND ----------

//generate the revenue columns to for gold suite of user tables

  def withCumUDSDimensions(
                            udsCols: Vector[String] = udsCols, 
                            metricDateMin: String = "2021-04-19",
                            metricDateMax: String = "2021-04-19", 
  df: DataFrame): DataFrame = {
    println("calling withCumUDSDimensions")
    val cumUDSDimensions = 
      spark
        .table("gold.facts_device_activity")
         .withColumn("postal_size", size('all_postal_codes))
         .withColumn("postal_code", $"all_postal_codes"($"postal_size"-1))
        .select('user_id, 'device_id as "sid", 'device_os as "device_platform", 'postal_code, 'app_opens, 'evs, 'uevs, 'premium_uevs, 'clippings, 'ttms, 'shares, 'date)
        .transform(
          df => {
            if (metricDateMin == null)
              df.filter(col("date") <= lit(metricDateMax))
            else
              df.filter(col("date").between(lit(metricDateMin), lit(metricDateMax)))
          }
        )
        .withColumn("country", getCountry(col("postal_code")))
        .withColumn("revenue", when(col("country") === lit("US"), col("premium_uevs") * lit(0.29)).otherwise(col("premium_uevs") * lit(0.31)))
        .filter(
           col("device_platform").isin("iOS", "Android") &&
             !$"user_id".isin("unknown")
        )
        .groupBy(col("user_id"), col("date").as("metric_date"))
        .agg(
          sum(col("revenue")).as("revenue"),
          udsCols.map(columnString => sum(col(columnString)).as(columnString)): _*)
    
    // group by columns list
    val dfOriginalColumns = df.columns.map(columnString => col(columnString).as(columnString))
    val udsColsWithRev = "revenue" +: udsCols
    
    
    // aggregate metrics    
    val sumRevMetricOnDayX = aggByRangeByColumn(Vector.range(0, 15), udsColsWithRev.filter(Vector("revenue").contains(_)), aggColOnDayX)
    val (sumRevMetricOnDayXCols, sumRevMetricOnDayXNames) = (sumRevMetricOnDayX.map(_._1), sumRevMetricOnDayX.map(_._2))

    println("Feature aggregation process initiated")
    // aggregate list
    val aggList = 
      sumRevMetricOnDayXCols 
    
    // output
    df
      .join(cumUDSDimensions, Seq("user_id"), "left")
      .groupBy(dfOriginalColumns: _*)
      .agg(
        aggList.head,
        aggList.tail: _*
      )
      .na.fill(0)
  }

// COMMAND ----------

val cumudsnew = withCumUDSDimensions(udsCols, "2021-04-19", "2021-04-19", newuinf)

// COMMAND ----------

display(cumudsnew)

// COMMAND ----------

cumudsnew.select('user_id).distinct.count

// COMMAND ----------

val oldudsCols = Vector("app_opens", "engaged_visits", "unique_engaged_visits", "premium_unique_engaged_visits", "flyer_item_clippings", "ecom_item_clippings", "flyer_item_shares", "ecom_item_shares", "coupon_shares", "flyer_item_ttm", "ecom_item_ttm")

// COMMAND ----------

//change metric dates to only the birth dates
// generate revenue metrics for old suite of tables
  def withCumUDSDimensionsOld(
                            oldudsCols: Vector[String] = oldudsCols, 
                            metricDateMin: String = "2021-04-19",
                            metricDateMax: String = "2021-04-19", 
  df: DataFrame): DataFrame = {
    println("calling withCumUDSDimensions")
    val cumUDSDimensions = 
      spark
        .table("silver_layer_pipeline.user_daily_summary")
        .transform(
          df => {
            if (metricDateMin == null)
              df.filter(col("date") <= lit(metricDateMax))
            else
              df.filter(col("date").between(lit(metricDateMin), lit(metricDateMax)))
          }
        )
        .withColumn("country", getCountry(col("postal_code")))
        .withColumn("revenue", when(col("country") === lit("US"), col("premium_unique_engaged_visits") * lit(0.29)).otherwise(col("premium_unique_engaged_visits") * lit(0.31)))
        .filter(
           col("device_platform").isin("iOS", "Android") &&
             !$"user_id".isin("unknown")
        )
        .groupBy(col("user_id"), col("date").as("metric_date"))
        .agg(
          sum(col("revenue")).as("revenue"),
          oldudsCols.map(columnString => sum(col(columnString)).as(columnString)): _*)
    
    // group by columns list
    val dfOriginalColumns = df.columns.map(columnString => col(columnString).as(columnString))
    val udsColsWithRev = "revenue" +: oldudsCols
    
    // custom metrics
    val distinctDaysActiveCol = countDistinct(col("metric_date")).as("uds_distinct_days_active")
    val daysSinceLastSeenCol = max(datediff(col("window_end_date"), col("metric_date"))).as("uds_days_since_last_seen")
    
    // aggregate metrics    
    val sumRevMetricOnDayX = aggByRangeByColumn(Vector.range(0, 15), udsColsWithRev.filter(Vector("revenue").contains(_)), aggColOnDayX)
    val (sumRevMetricOnDayXCols, sumRevMetricOnDayXNames) = (sumRevMetricOnDayX.map(_._1), sumRevMetricOnDayX.map(_._2))
    
    println("Feature aggregation process initiated")
    // aggregate list
    val aggList = 
      sumRevMetricOnDayXCols
    // output
    df
      .join(cumUDSDimensions, Seq("user_id"), "left")
      .groupBy(dfOriginalColumns: _*)
      .agg(
        aggList.head,
        aggList.tail: _*
      )
      .na.fill(0)
  }

// COMMAND ----------

val cumudsold = withCumUDSDimensionsOld(oldudsCols, "2021-04-19", "2021-04-19", olduinf)

// COMMAND ----------

display(cumudsold
       )

// COMMAND ----------

//compare day 0 rev between the old and new suite of tables 

// COMMAND ----------

//overall revenue - old

val sumRevOld: Long = cumudsold.agg(sum("uds_revenue_day_0").cast("long")).first.getLong(0)

// COMMAND ----------

//overall revenue - new
val sumRevNew: Long = cumudsnew.agg(sum("uds_revenue_day_0").cast("long")).first.getLong(0)
//a 0.4% decrease. im surprised the revenue has decreased even though we see more users in the new suite of tables 

// COMMAND ----------

//take the users we did not see in the old suite of tables but just in the new ones. what is their day 0 revenue?
val revNewUsersNotinOld = cumudsnew.join(guidsNotPresentInOld, Seq("user_id"), "inner")
val sumRevNewNotinOld: Long = revNewUsersNotinOld.agg(sum("uds_revenue_day_0").cast("long")).first.getLong(0)
//these users are not contributing much. um

// COMMAND ----------

val compDay0Rev = cumudsold.select('user_id, 'uds_revenue_day_0)
                           .join(cumudsnew.select('user_id, 'uds_revenue_day_0 as "new_uds_revenue_day_0"), Seq("user_id"), "outer")
                           .withColumn("user_present_in_both", when((!('new_uds_revenue_day_0.isNull) && !('uds_revenue_day_0.isNull)), lit(1)).otherwise(lit(0)))
                           .filter('user_present_in_both === 1)
                           .withColumn("disc_day0_rev", round((('new_uds_revenue_day_0 - 'uds_revenue_day_0)/'uds_revenue_day_0)*100, 0))
                           .withColumn("isSame", when('new_uds_revenue_day_0 === 'uds_revenue_day_0 , lit(1)).otherwise(lit(0)))
display(compDay0Rev)

// COMMAND ----------

display(compDay0Rev.where('isSame === 0))
//very minute difference. some of it looks like rounding error

// COMMAND ----------

compDay0Rev.where('isSame === 0).count

// COMMAND ----------

//lets see is there is something unusual about these guids that are not present in the gold layer pipeline tables
val guidsNotPresentInNew = outerJoinedDf.withColumn("notpresentinnew", when((($"new_user_id".isNull) && !('user_id.isNull)), lit(1)).otherwise(lit(0)))
                                        .where('notpresentinnew === 1)//.select('new_user_id as "user_id")

display(guidsNotPresentInNew)

// COMMAND ----------

guidsNotPresentInNew.count