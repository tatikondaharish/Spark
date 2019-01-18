// Databricks notebook source
### -> Adding AGE column

// COMMAND ----------

val data = Seq((1,"harish","tat",23),(2,"bobby","pra",18),(3,"amrutha","m",29),(4,"adi","ku",17)).toDF("id","fname","lname","age")

// COMMAND ----------

### Scala Function to check for teenager

// COMMAND ----------

def tennager (x : Int) = x < 19

// COMMAND ----------

### Udf to check for teenager

// COMMAND ----------

val isTennager = udf (tennager _)

// COMMAND ----------

data.withColumn("isTennager",isTennager($"age")).show

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

### Add array of colours

// COMMAND ----------

val addColurs = udf ({
  id : Int => id match {
    case 1 => Array("blue","green")
    case 2 => Array("yellow","white")
    case 3 => Array("pink","orange")
    case 4 => Array("black","white","yellow")
  }
  
})

// COMMAND ----------

val newData = data.withColumn("favColours",addColurs($"id"))

// COMMAND ----------

### Scala function to have favcolurs Stirng

// COMMAND ----------

def addStrings (arr : Seq[String]) : String = {
  arr.reduce((a,b) => (s"""$a,$b"""))
} 

// COMMAND ----------

### Udf to for favColurs String

// COMMAND ----------

val favColourString = udf (addStrings _) 

// COMMAND ----------

newData.withColumn("favColourString",favColourString($"favColours")).show

// COMMAND ----------

### Scala function to determine size of array

// COMMAND ----------

def colorCount(arr : Seq[String]) : Int = arr.size

// COMMAND ----------

### * Udf to define size of array

// COMMAND ----------

val NoOfcolours = udf (colorCount _)

// COMMAND ----------

newData.withColumn("favColorCount",NoOfcolours($"favColours")).show

// COMMAND ----------

### Scala function to reverse a string

// COMMAND ----------

def reverseString (str : String) : String = str.reverse

// COMMAND ----------

### Udf for reverse String

// COMMAND ----------

val reverse = udf (reverseString _)

// COMMAND ----------

newData.withColumn("reverseOfFirstName", reverse($"fname")).show

// COMMAND ----------

### Scala function to get current year

// COMMAND ----------

import java.util.Calendar

// COMMAND ----------

def calender = Calendar.getInstance.get(Calendar.YEAR)


// COMMAND ----------

### * udf to get year

// COMMAND ----------

val getYear = udf (calender _)

// COMMAND ----------

newData.withColumn("Year", getYear()).show

// COMMAND ----------


### Scala method to check Value is null or not

// COMMAND ----------

def check(value : Any) = value == null 

// COMMAND ----------

### * udf to check for null

// COMMAND ----------

val isNull = udf (check _)

// COMMAND ----------

newData.withColumn("isFirstNameNull", isNull($"fname")).show

// COMMAND ----------

## Use of Array_Contains

// COMMAND ----------

newData.show(false)

// COMMAND ----------

val c = array_contains($"favColours","blue")

// COMMAND ----------

newData.filter(c).show

// COMMAND ----------

### Use of Array_distict

// COMMAND ----------

val addtemp = udf ({
  id : Int => id match {
    case 1 => Array(1,2,3,2)
    case 2 => Array(5,4,3,2)
    case 3 => Array(1,0,3,1,2)
    case 4 => Array(1,2,1,1)
  }
  
})

// COMMAND ----------

val temp = newData.withColumn("tem",addtemp($"id"))

// COMMAND ----------

temp.withColumn("favcOL",array_distinct($"tem")).show

// COMMAND ----------


### Use of array_max

// COMMAND ----------

temp.withColumn("maxoftem",array_max($"tem")).show

// COMMAND ----------


### Use Of explode

// COMMAND ----------

temp.withColumn("favColours",explode($"favColours")).show

// COMMAND ----------

### Use Of map_keys

// COMMAND ----------

val addmap = udf ({
  id : Int => id match {
    case 1 => Map((1,2),(3,4))
    case 2 => Map((5,6),(2,4))
    case 3 => Map((7,5),(1,4))
    case 4 => Map((2,1),(3,4))
  }
  
})

// COMMAND ----------

newData.withColumn("map",addmap($"id")).withColumn("mapkeys",map_keys($"map")).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### Use of map_values

// COMMAND ----------

newData.withColumn("map",addmap($"id")).withColumn("mapvalues",map_values($"map")).show

// COMMAND ----------

newData.withColumn("fullname",$"fname" + " " +$"lname").show

// COMMAND ----------

### Use of concat
* Concat is used for adding two or more columnsMAGIC i

