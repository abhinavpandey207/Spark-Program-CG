// Databricks notebook source
val data=sc.textFile("/FileStore/tables/Property_data.csv")

data.take(10)

// COMMAND ----------

val removeHeader=data.filter(line => !line.contains("Price") )

removeHeader.take(10)

// COMMAND ----------

val roomrdd = removeHeader.map(x => ( x.split(",")(3).toInt, (1,x.split(",")(2).toDouble) ))

roomrdd.take(10)

// COMMAND ----------

val reducedrdd = roomrdd.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2) )
reducedrdd.collect()

// COMMAND ----------

val finalrdd = reducedrdd.mapValues( data => data._2 / data._1)


// COMMAND ----------

finalrdd.collect()

// COMMAND ----------

for((bedroom, avg) <- finalrdd.collect() ) println(bedroom + " : " + avg)

// COMMAND ----------

finalrdd.saveAsTextFile("PropertyFinal.csv")
