// Databricks notebook source exported at Wed, 22 Jun 2016 19:52:46 UTC
// MAGIC %md #Load Airports Data
// MAGIC * Data for all airport can be found at [OpenFlights.org](http://openflights.org/data.html)
// MAGIC 
// MAGIC ![Spark ETL Pipeline](/files/ETL_Http_To_Parquet.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Step 1:** Pull the data from the web.

// COMMAND ----------

val kyle = "weird"
def getUrlAsString(url: String): String = {
  val client = new org.apache.http.impl.client.DefaultHttpClient()
  val request = new org.apache.http.client.methods.HttpGet(url)
  val response = client.execute(request)
  val handler = new org.apache.http.impl.client.BasicResponseHandler()
  handler.handleResponse(response).trim
}

// COMMAND ----------

val airports = getUrlAsString("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat")

// COMMAND ----------

dbutils.fs.put("/tmp/airports", airports, true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Step 2:** Parse the data into Spark as a DataFrame

// COMMAND ----------

import org.apache.spark.sql.types._

val airportSchema = StructType(StructField("AirportId",IntegerType,true)::
  StructField("Name",StringType,true)::
  StructField("City",StringType,true)::
  StructField("Country",StringType,true)::
  StructField("FAA_Code",StringType,true)::
  StructField("ICAO_Code",StringType,true)::
  StructField("Latitude",FloatType,true)::
  StructField("Longitude",FloatType,true)::  
  StructField("Altitude",IntegerType,true)::    
  StructField("TimeZone",FloatType,true)::    
  StructField("DST",StringType,true)::      
  StructField("TimeZoneOlsonFormat",StringType,true)::Nil)

// COMMAND ----------

val airportsDf = sqlContext.read.format("com.databricks.spark.csv").schema(airportSchema).load("/tmp/airports")

// COMMAND ----------

airportsDf.count()

// COMMAND ----------

display(airportsDf)

// COMMAND ----------

airportsDf.registerTempTable("airportTempTable")

// COMMAND ----------

// MAGIC %sql select * from airportTempTable

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Step 3:** Persist the data as Parquet and Register Table

// COMMAND ----------

airportsDf.write.mode("Overwrite").saveAsTable("airports")

// COMMAND ----------

// MAGIC %fs ls user/hive/warehouse/airports

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Step 4:** Validate the Data in the Table

// COMMAND ----------

// MAGIC %sql select * from airports where City = 'Chicago' 

// COMMAND ----------

// MAGIC %sql select * from airports limit 10

// COMMAND ----------

// MAGIC %sql select * from airports limit 100