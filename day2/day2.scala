//Convert json format data into parquet format data

//Assigning paths to variables
val inputPath = "/databricks-datasets/definitive-guide/data/flight-data/json/"
val outputPath = "/mnt/harish/day2/json_to_parquet_conversion"

//counting files in source location
val jsonCount = spark.read.json(inputPath).count

//writing data to target location and counting no of files 
spark.read.json(inputPath).write.parquet(outputPath)
val parquetData = spark.read.parquet(outputPath)
val parquetCount = parquetData.count

//Checking if no. of files in source is equal to no. of files in target
assert (parquetCount == jsonCount,"Error in writing data")

//Checking No of columns in target file is 3
assert(parquetData.columns.size == 3, "target column size != source column size")

//Checking datatype of count is of type long
assert(parquetData.schema("count").dataType == org.apache.spark.sql.types.DataTypes.LongType,
 									"Count column is not of type Long")
