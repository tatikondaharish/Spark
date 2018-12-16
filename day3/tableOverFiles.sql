--Create sql table over json file
CREATE TABLE flightdata_json 
USING json
LOCATION "databricks-datasets/definitive-guide/data/flight-data/json/"

--Create sql table over csv file
CREATE TABLE flightdata_csv
USING csv
LOCATION "databricks-datasets/definitive-guide/data/flight-data/csv/"

--Create sql table over parquet file
CREATE TABLE flightdata_parquet (dest_country string,origin_dest string,count int)
USING parquet
LOCATION "databricks-datasets/definitive-guide/data/flight-data/parquet/"

--Create sql table over orc file
CREATE TABLE flightdata_orc (dest_country string,origin_dest string,count int)
USING orc
LOCATION "databricks-datasets/definitive-guide/data/flight-data/orc/"

