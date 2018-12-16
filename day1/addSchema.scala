
//**Define a schema and read the data according to schema and display**

import org.apache.spark.sql.types._

val schema = new StructType().
add("S.No",StringType,true).
add("Movie Id",StringType,true).
add("Gender",StringType,true).
add("Profession",StringType,true).
add("user id",StringType,true)

val userData = spark.read.
option("delimiter","|").
schema(schema).
csv("dbfs:/harish/u.user")

userData.show

