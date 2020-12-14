// Databricks notebook source
//File location path ->  /FileStore/tables/ratings.csv

//creating rdd from external dataset

val data= sc.textFile("/FileStore/tables/ratings.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

val ratingsData = data.map(x => x.split(",")(2))

ratingsData.collect()

// COMMAND ----------

ratingsData.count()

// COMMAND ----------

ratingsData.countByValue()
