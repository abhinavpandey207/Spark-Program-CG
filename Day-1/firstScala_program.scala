// Databricks notebook source
var a=1
var b:Int=1
var c=1:Int

// COMMAND ----------

val c=20
c=100

// COMMAND ----------

// lazy val functioning
lazy val a=25
a

// COMMAND ----------

lazy val variabl_lazy={println("Welcome to Scala");100}
variabl_lazy

// COMMAND ----------

variabl_lazy

// COMMAND ----------

val b=20
val sum= 50+b
println(sum)

// COMMAND ----------

val `my name is`="rajkumar"

// COMMAND ----------

var `item`=10
print(`item`)

// COMMAND ----------

def square(a:Int):Int={
  a*a
}
square(2)

// COMMAND ----------

//unicode values generation
val a="\u03BB"

// COMMAND ----------

//Function in Function

def square(a:Int):Int={
  a*a
}

def getresult(y:Int, function:Int => Int): Int={
  
  function(y)
}

getresult(2, square)

// COMMAND ----------

//study of colections
val collect= List(1,2,3,4,5,6,7,8,9)
collect

// COMMAND ----------

// accessing collection data 
collect(2)

// COMMAND ----------

collect.reverse

// COMMAND ----------

//to show first element
collect.head

// COMMAND ----------

// DBTITLE 1,First RDD- SC(Spark Context)
//by parallelize method
val data=List(1,2,3,4,5)

val creationRDD = sc.parallelize(data)

// COMMAND ----------

//to get result of RDD- use action on RDD
creationRDD.collect()

// COMMAND ----------

//to check how many partitions created 
creationRDD.partitions.size

// COMMAND ----------

//to give partitions of own choice
val rddPartition = sc.parallelize(List(1,2,3,4,5),2)

// COMMAND ----------

rddPartition.partitions.size

// COMMAND ----------

//to count values in a rdd set
rddPartition.count()

// COMMAND ----------

//map- used for transforming the data of rdd
rddPartition.map(y => y*y).collect()

// COMMAND ----------

val maprdd=rddPartition.map(y => y*y)


maprdd.take(3)

// COMMAND ----------

maprdd.filter(x => x%2==0).collect()

// COMMAND ----------

val mainrdd = sc.parallelize(List("Hey","hello","how","are","you?"))
mainrdd.collect()

// COMMAND ----------

//map vs flatmap

mainrdd.map(x => x.split(",")).collect //contents saved in individual lists

// COMMAND ----------

mainrdd.flatMap(x => x.split(",")).collect() //apllies changes to all considering the entire set as single entity

// COMMAND ----------

//creating rdd for key-value pairs

val rdd0=sc.parallelize(Array("one","two","three","one","two"))

// COMMAND ----------

val keyrdd = rdd0.map(x => (x,1))

keyrdd.collect()

// COMMAND ----------

keyrdd.reduceByKey(_+_).collect()

// COMMAND ----------


