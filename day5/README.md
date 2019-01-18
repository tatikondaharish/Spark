Setup dvdrental dataset in AWS S3 bucket and access them from Databricks notebook

Read https://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html to generate your access and secret key
Create a bucket called dvdrental and upload json and parquet files
Define MyAWS object in Databricks notebook and fill in your secret, key and bucket name

* Calculate the number of movies each actor has played a part in. The output dataframe should have two columns:
 * Full name
 * Movie_count
 Then, display top-5 actors based on count only. The count should be in descending order.
