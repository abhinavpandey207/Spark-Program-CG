// Databricks notebook source
// /FileStore/tables/nasa_july-1.tsv

// /FileStore/tables/nasa_august-1.tsv

val nasaJuly= sc.textFile("/FileStore/tables/nasa_july-1.tsv")

val nasaAug=sc.textFile("/FileStore/tables/nasa_august-1.tsv")


// COMMAND ----------

val hostJuly = nasaJuly.map( x => x.split("\t")(0)).filter(row=> row != "host")
val hostAugust = nasaAug.map( x => x.split("\t")(0)).filter(row=> row != "host")

hostJuly.take(10)
hostAugust.take(10)


// COMMAND ----------

val intersect= hostJuly.intersection(hostAugust)
intersect.collect()

// COMMAND ----------

val unionrdd = hostJuly.union(hostAugust)
unionrdd.collect()
