// Databricks notebook source
// File uploaded to /FileStore/tables/nasa_july.tsv
//File uploaded to /FileStore/tables/nasa_august.tsv

val julyRdd = sc.textFile("/FileStore/tables/nasa_july.tsv")

val augRdd = sc.textFile("/FileStore/tables/nasa_august.tsv")



// COMMAND ----------

val unionRdd = augRdd.union(julyRdd)
unionRdd.take(10)

// COMMAND ----------

// DBTITLE 1,First function takes the first row in header object
val header = unionRdd.first

// COMMAND ----------

// DBTITLE 1,Filter condition to not take data- 1st Approach
unionRdd.filter(line =>line != header).take(2)

// COMMAND ----------

// DBTITLE 1,2nd Approach
def headerRemover(line:String): Boolean = !(line.startsWith("host"))

// COMMAND ----------

val finalRdd = unionRdd.filter(x => headerRemover(x))

finalRdd.take(3)

// COMMAND ----------

val zeroResult = finalRdd.filter(x => x.split("\t")(5).toInt == 0)

val bytes = finalRdd.filter(x => x.split("\t")(6).toInt >= 1000)

(zeroResult.collect() , bytes.collect())

// COMMAND ----------

bytes.count()

// COMMAND ----------

val requireddata = finalRdd.filter(x => ( x.split("\t")(5).toInt == 0 || x.split("\t")(6).toInt >= 1000))

requireddata.count()

// COMMAND ----------

finalRdd.sample(withReplacement = true, fraction=0.20).collect()
