// Databricks notebook source
val data1= sc.parallelize(List(("raj",2020),("aryan",2022)))

val data2= sc.parallelize(List(("arvind","kumar"),("aryan","US")))

// COMMAND ----------

data1.join(data2).collect()

// COMMAND ----------

data1.leftOuterJoin(data2).collect()

// COMMAND ----------

data1.rightOuterJoin(data2).collect()

// COMMAND ----------

data1.fullOuterJoin(data2).collect()
