// Databricks notebook source
object MyAWS {
val secret = "**********************".replace("/", "%2F")
val key = "*********************"
val myBucket = "com.harish.sample"
val MountName = "dvdrental"
lazy val dvdrentalRootPath = s"s3a://${"*********"}:${"**********"}@s3/buckets/com.harish.sample/${"dvdrental"}"
}

// COMMAND ----------

val secret = "***********".replace("/", "%2F")
val key = "*******"
val myBucket = "com.harish.sample"
val MountName = "dvdrental"

// COMMAND ----------

dbutils.fs.unmount(s"""/mnt/${MyAWS.MountName}""")


// COMMAND ----------

dbutils.fs.mount(s"s3a://$key:$secret@$myBucket", s"/mnt/$MountName")
display(dbutils.fs.ls(s"/mnt/$MountName"))

// COMMAND ----------

// MAGIC %fs ls /mnt/dvdrental/dvdrental/json/

// COMMAND ----------

val actors = spark.read.json(s"/mnt/$MountName/dvdrental/json/actor.json")


// COMMAND ----------

actors.show

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val actorNames = actors.withColumn("FullName",concat($"first_name",$"last_name")).select($"actor_id",$"FullName")

// COMMAND ----------

val movies = spark.read.json(s"/mnt/$MountName/dvdrental/json/film_actor.json")


// COMMAND ----------

import org.apache.spark.sql.functions._
var actorMoviesCount = movies.groupBy($"actor_id").count

// COMMAND ----------

actorMoviesCount = actorMoviesCount.select($"actor_id",$"count".as("movieCount"))

// COMMAND ----------

actorMoviesCount.show

// COMMAND ----------

val result = actorNames.join(actorMoviesCount,"actor_id").select($"FullName",$"count")

// COMMAND ----------

result.orderBy($"count".desc).limit(5).show

// COMMAND ----------

val df = spark.read.json(s"/mnt/$MountName/dvdrental/json/actor.json")df.show


// COMMAND ----------

df.show

// COMMAND ----------

// MAGIC %fs ls /mnt/dvdrental/Dvd Rental

// COMMAND ----------


