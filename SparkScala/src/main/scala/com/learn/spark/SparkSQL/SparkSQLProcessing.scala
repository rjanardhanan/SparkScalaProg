package com.learn.spark.SparkSQL
import com.learn.spark.SparkDataFrame.DataFrameProcessing.sparkSession
import com.learn.spark.utils.Context

object SparkSQLProcessing extends App with Context{

  val dfCrime = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/crime25k.csv")
    .toDF("INCIDENT_ID","OFFENSE_ID","OFFENSE_CODE","OFFENSE_CODE_EXTENSION",
      "OFFENSE_TYPE_ID","OFFENSE_CATEGORY_ID","FIRST_OCCURRENCE_DATE","LAST_OCCURRENCE_DATE",
      "REPORTED_DATE","INCIDENT_ADDRESS","GEO_X","GEO_Y","GEO_LON","GEO_LAT","DISTRICT_ID","PRECINCT_ID",
      "NEIGHBORHOOD_ID","IS_CRIME","IS_TRAFFIC")

  val dfOffenseCodes = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/Offense_codes.csv")
    .toDF("OFFENSE_CODE","OFFENSE_CODE_EXTENSION","OFFENSE_TYPE_ID","OFFENSE_TYPE_NAME",
      "OFFENSE_CATEGORY_ID","OFFENSE_CATEGORY_NAME","IS_CRIME","IS_TRAFFIC")

  dfCrime.createOrReplaceTempView("Denver_Crime")
  dfOffenseCodes.createOrReplaceTempView("Offense_Codes")

  println(s"Listing the tables in Spark Catalog...")
  sparkSession.catalog.listTables().show()

  println(s"Listing the first 10 rows from denver_crime temp view using select sql...")
  sparkSession.sql("select INCIDENT_ID,OFFENSE_ID,OFFENSE_CODE,OFFENSE_CODE_EXTENSION from denver_crime limit 10").show()

  println(s"Applying filter on rows queried using where clause...")
  sparkSession.sql("select INCIDENT_ID,OFFENSE_ID,OFFENSE_CODE,OFFENSE_CODE_EXTENSION from denver_crime " +
    "where OFFENSE_CODE = 5213 limit 5").show()

  println(s"Count total rows with the specific offense_code 5213...")
  sparkSession.sql("select count(*) from denver_crime where OFFENSE_CODE=5213").show()

  println(s"Like example...")
  sparkSession.sql("select INCIDENT_ID,OFFENSE_ID,OFFENSE_CODE," +
    "OFFENSE_CODE_EXTENSION,OFFENSE_TYPE_ID,OFFENSE_CATEGORY_ID from denver_crime " +
    "where OFFENSE_TYPE_ID like 'theft%'").show()

  println(s"SQL IN clause...")
  sparkSession.sql("select * from denver_crime where INCIDENT_ID IN (201872837,2016340553,20188004580)").show()

  println(s"Group By example...")
  sparkSession.sql("select OFFENSE_CODE,count(*) as count from denver_crime group by OFFENSE_CODE").show()

  println(s"DISTINCT example....")
  sparkSession.sql("select DISTINCT OFFENSE_TYPE_ID from denver_crime").show()
  sparkSession.stop()

}
