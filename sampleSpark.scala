val userData = spark.read.option("delimiter","|")
		.csv("/Users/babbu/downloads/projects/DataScience Project/MovieSet Project/spark/ml-100k")

userData.show
