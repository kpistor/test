// Databricks notebook source exported at Tue, 25 Oct 2016 19:48:33 UTC
// MAGIC %md #ETL Pipeline for Flight Data
// MAGIC * Loads a 128 GB CSV dataset into a partitioned parquet table and a table in Redshift

// COMMAND ----------

val sourcePath = "/databricks-datasets/airlines"

// COMMAND ----------

// MAGIC %fs head "/databricks-datasets/airlines/part-00000"

// COMMAND ----------

//128,911,087,141; 128GB
import org.apache.spark.sql.functions._
val flightFileList = dbutils.fs.ls("/databricks-datasets/airlines")
val flightFileDf = sc.parallelize(flightFileList).toDF()
val flightFileSize = flightFileDf.agg(sum("size")).collect()(0).getLong(0)
println("Total Size of Flight CSV Files: " + flightFileSize)

// COMMAND ----------

import org.apache.spark.sql.types._

val flightSchema = StructType(StructField("year",IntegerType,true)::
  StructField("month",IntegerType,true)::
  StructField("dayofmonth",IntegerType,true)::
  StructField("DayOfWeek",IntegerType,true)::
  StructField("DepTime",StringType,true)::
  StructField("CRSDepTime",IntegerType,true)::
  StructField("ArrTime",StringType,true)::
  StructField("CRSArrTime",IntegerType,true)::
  StructField("UniqueCarrier",StringType,true)::
  StructField("FlightNum",IntegerType,true)::
  StructField("TailNum",StringType,true)::
  StructField("ActualElapsedTime",StringType,true)::
  StructField("CRSElapsedTime",IntegerType,true)::
  StructField("AirTime",StringType,true)::
  StructField("ArrDelay",StringType,true)::
  StructField("DepDelay",StringType,true)::
  StructField("Origin",StringType,true)::
  StructField("Dest",StringType,true)::
  StructField("Distance",StringType,true)::
  StructField("TaxiIn",StringType,true)::
  StructField("TaxiOut",StringType,true)::
  StructField("Cancelled",IntegerType,true)::
  StructField("CancellationCode",StringType,true)::
  StructField("Diverted",IntegerType,true)::
  StructField("CarrierDelay",StringType,true)::
  StructField("WeatherDelay",StringType,true)::
  StructField("NASDelay",StringType,true)::
  StructField("SecurityDelay",StringType,true)::
  StructField("LateAircraftDelay",StringType,true)::
  StructField("IsArrDelayed",StringType,true)::
  StructField("IsDepDelayed",StringType,true)::Nil)

// COMMAND ----------

val flight = sqlContext.read.format("com.databricks.spark.csv").schema(flightSchema).option("mode","DROPMALFORMED").load(sourcePath)

// COMMAND ----------

// MAGIC %md ##Save as Parquet Table

// COMMAND ----------

flight.write.mode("overwrite").saveAsTable("flight")

// COMMAND ----------

// MAGIC %sql select count(*) from flight

// COMMAND ----------

// MAGIC %sql show create table flight