// Databricks notebook source
val airportrdd=sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val USairport=airportrdd.filter(line => line.split(",")(3) == "\"United States\"")

USairport.collect()

// COMMAND ----------

// DBTITLE 1,First Approach
//to find airport city and state

def splitInput(line:String) = {
  
  val datasplit=line.split(",")
  
  val airportId = datasplit(1)
  val cityName = datasplit(2)
  
  (airportId,cityName)
}

// COMMAND ----------

USairport.map( splitInput ).take(3)

// COMMAND ----------

// DBTITLE 1,Second Approach - same task
USairport.map(line => {
  val splitData= line.split(",")
  splitData(1)+" "+splitData(2) }).collect()

// COMMAND ----------

// DBTITLE 1,Task-1
val filterCountry= airportrdd.filter(x=>(x.split(",")(7)).toDouble>40 || x.split(",")(3)=="\"Iceland\"")

filterCountry.take(20)

// COMMAND ----------

filterCountry.saveAsTextFile("CountryIceland.csv")

// COMMAND ----------

// DBTITLE 1,Task-2
val pacific= airportrdd.filter(x=>(x.split(",")(8)).toDouble%2==0).map(x=>x.split(",")(11))

pacific.countByValue
