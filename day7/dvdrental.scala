
dbutils.fs.unmount(s"""/mnt/${MyAWS.MountName}""")

// COMMAND ----------

dbutils.fs.mount(s"s3a://$key:$secret@$myBucket", s"/mnt/$MountName")


// COMMAND ----------

%fs ls /mnt/dvdrental/dvdrental/json

// COMMAND ----------

// 1. Customers Names Starting with Jamie

// COMMAND ----------

val customer = spark.read.json("/mnt/dvdrental/dvdrental/json/cusomer.json")

// COMMAND ----------

customer.filter($"first_name" === "Jamie").show

// COMMAND ----------

// 2. Customers with rental amount less than 1USD and greater than 8USD

// COMMAND ----------

val payment = spark.read.json("/mnt/dvdrental/dvdrental/json/payment.json")

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val payment_filter = payment.where("amount < 1 OR amount > 8")

// COMMAND ----------

payment.join(temp,payment.col("customer_id") === payment_filter.col("customer_id"),"leftsemi").show

// COMMAND ----------

// 3. Retrive 4 Films starting from thrid one ordered by film_id

// COMMAND ----------

val film = spark.read.json("dbfs:/mnt/dvdrental/dvdrental/json/film.json")

// COMMAND ----------

// MAGIC %sql
// MAGIC USE dvdrental;
// MAGIC SELECT * 
// MAGIC FROM   film
// MAGIC ORDER BY film_id
// MAGIC OFFSET (2 ROWS FETCH NEXT 1 ROWS ONLY);

// COMMAND ----------

// MAGIC %python
// MAGIC film = spark.read.json("dbfs:/mnt/dvdrental/dvdrental/json/film.json")

// COMMAND ----------

var count = 0

// COMMAND ----------

film.orderBy("film_id").filter(count++ > 3)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Get Five films sorted by title

// COMMAND ----------

display(q3.sort($"title".asc).limit(5)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Customers name whose names contains "er"

// COMMAND ----------

q1.filter($"first_name".contains("er")).show

// COMMAND ----------

// MAGIC %md
// MAGIC ###  6. Customer whose first name does not begin with "Jen"

// COMMAND ----------

q1.filter(!$"first_name".startsWith("Jen")).show

// COMMAND ----------

// MAGIC %md 
// MAGIC ###  customer whose first name does not begin with "Jen" with case sensitive

// COMMAND ----------

sqlContext.sql("set caseSensitive: false")

// COMMAND ----------

q1.filter($"first_name".startsWith("jen")).show

// COMMAND ----------

q1.filter($"first_name".toLowerCase.startsWith("jen".toLowerCase)).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.Get rental Information of id 1,2

// COMMAND ----------

val q4 = spark.read.json("dbfs:/mnt/dvdrental/dvdrental/json/rental.json")

// COMMAND ----------

q4.filter(row => row.get(0) == 1 || row.get(0) == 2).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.Get rental Information of id 1,2

// COMMAND ----------

//Use column operations

// COMMAND ----------

q4.filter(row => row.get(0) != 1 || row.get(0) != 2).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9.Customer list for customers with return date 2005-05-27

// COMMAND ----------

q4.select("customer_id").filter($"return_date".startsWith("2005-05-27")).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 10.From payment select payment where amount is between 8 and 9

// COMMAND ----------

val q5 = spark.read.json("/mnt/dvdrental/dvdrental/json/payment.json")

// COMMAND ----------

q5.filter($"amount" > 8 && $"amount" < 9).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 11.From payment select payment where amount is not between 8 and 9

// COMMAND ----------

q5.filter(!($"amount" > 8 && $"amount" < 9)).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 12.From customer, display full names of all customers and name this column as "full_name"

// COMMAND ----------

q1.withColumn("full_name",concat($"first_name",$"last_name")).show

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 13.Each customer may have zero or many payments. Each payment belongs to one and only one customer. The customer_id field establishes the link between two tables. Display information (all columns) from both the tables and sort the result set by customer id

// COMMAND ----------

val customerjoin = q1.join(q5,"customer_id").sort("customer_id")

// COMMAND ----------

display(
  customerjoin
)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 14.Filter the output of the #13 for the customer id 2

// COMMAND ----------

display(customerjoin.filter($"customer_id" === 2)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 15.Using staff, payment and customer dataset, display the following columns: customer.customer_id, customer.first_name customer_first_name,customer.last_name customer_last_name, customer.email, staff.first_name staff_first_name,staff.last_name staff_last_name,payment.amount. Each staff relates to zero or many payments. Each payment is processed by one and only one staff.Each customer has zero or many payments. Each payment belongs to one and only customer.

// COMMAND ----------

val q6 = spark.read.json("/mnt/dvdrental/dvdrental/json/staff.json")

// COMMAND ----------

val staff_customer_payment_join = q1.select($"customer_id",$"first_name".as("customer_first_name")
                                           ,$"last_name".as("customer_last_name")
                                           ,$"email".as("customer_email"))
                                           .join(q5.select($"customer_id",$"amount",$"staff_id"),"customer_id")
                                           

// COMMAND ----------

val temp = staff_customer_payment_join.join(q6.select($"first_name".as("staff_first_name")
                                          ,$"last_name".as("staff_last_name"),$"staff_id"),"staff_id")

// COMMAND ----------

temp.drop("staff_id").show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 16.Each row in the film may have zero or many rows in the inventory. Each row in the inventory has one and only one row in the film. select only films that are not in the inventory

// COMMAND ----------

val q7 = spark.read.json("dbfs:/mnt/dvdrental/dvdrental/json/inventory.json")

// COMMAND ----------

q3.join(q7.select($"film_id".as("inverntory_film_id")),q3("film_id") === q7("film_id"),"left_semi")

// COMMAND ----------

display(q3.join(q3,"length"))

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 2.Aggregations

// COMMAND ----------

// MAGIC %md
// MAGIC ### From payment, count the number of transactions each staff has been processing

// COMMAND ----------

q2.groupBy("staff_id").count.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### From payment, show only customer who has been spending more than 200. Hint: You need to sum the amount column

// COMMAND ----------

//show upto 2 decimal points

// COMMAND ----------

q2.groupBy("customer_id").agg(sum($"amount").as("total")).filter($"total" > 200).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### From customner, select store that has more than 300 customers

// COMMAND ----------

q1.groupBy("store_id").count.as("total").filter($"total.count" > 300).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Handeling Null

// COMMAND ----------

// MAGIC %md
// MAGIC ### Create a dataframe called contacts using the following data
// MAGIC 
// MAGIC * Find the contact who does not have a phone number
// MAGIC * Find the contact who does have a phone number

// COMMAND ----------

val df = List(("John","Doe","john.doe@example.com",null),
("Lily","Bush","lily.bush@example.com","408-234-2764")).toDF("first_name","last_name","email","phone_number")


// COMMAND ----------

df.filter($"phone_number".isNull).show

// COMMAND ----------

df.filter($"phone_number".isNotNull).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2) Create two dataframe called departments and employees using the following data
// MAGIC 
// MAGIC * Find the department that does have any employee
// MAGIC * Find the employee who does not belong to any department,

// COMMAND ----------

val dept = Seq((1,"Sales"),
 (2,"Marketing"),
 (3,"HR"),
 (4,"IT"),
 (5,"Production")).toDF("dept_id","dept")

// COMMAND ----------

val employee = Seq(("Bette Nicholson", Some(1)),
 ("Christian Gable", Some(1)),
 ("Joe Swank", Some(2)),
 ("Fred Costner", Some(3)),
 ("Sandra Kilmer", Some(4)),
 ("Julia Mcqueen", null)).toDF("emp_name","emp_id")

// COMMAND ----------

dept.join(employee,dept("dept_id") === employee("emp_id"),"left").filter($"emp_id".isNull).show

// COMMAND ----------

dept.join(employee,dept("dept_id") === employee("emp_id"),"right").filter($"emp_id".isNull).show

// COMMAND ----------


