-- Show all databases present
show databases;

-- Create a Database if the given database dosen't exists
CREATE DATABASE IF NOT EXISTS day3;

--Create a Table if not present
CREATE TABLE IF NOT EXISTS day3.Person (
id int,
name varchar(25),
profession varchar(25)
);

--Get details about the table including the location, type, provider
DESCRIBE EXTENDED day3.Person;

--Insert values into table
INSERT INTO day3.Person
VALUES(1,'HARISH','SOFTWARE');

--Fetching data from table
SELECT * FROM day3.Person

--Drop a table from table if exists
DROP TABLE IF EXISTS Person

