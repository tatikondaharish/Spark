--Create a table named persons
CREATE TABLE day3.Persons (
PersonID int,
LastName varchar(255),
FirstName varchar(255),
Address varchar(255),
City varchar(255)
);


--Add a column to the table after a certain column
ALTER TABLE day3.Persons ADD COLUMNS (PostalCode string After City);

--Entended details of table
DESCRIBE Extended day3.Persons;

--Insert values into table
INSERT INTO day3.Persons
VALUES (15,'Pinky','Sri','93 fair oaks ave','Sanmateo','95068');

SELECT * FROM day3.Persons
Order by PersonID

--Count no of people with in a postal code of each city
SELECT City,PostalCode,COUNT(PersonID) 
FROM day3.Persons
GROUP BY City,PostalCode;

--Create Person_info table
CREATE TABLE IF NOT EXISTS day3.Person_Info (
id int,
email string
);

INSERT INTO day3.Person_Info
VALUES (18,'instagram@gmail.com');

SELECT * FROM day3.Person_Info
ORDER BY id

--InnerJoin
SELECT * FROM day3.Persons p
JOIN day3.Person_Info i 
ON i.id = p.PersonID
ORDER BY p.PersonID;

--Left Outer Join
SELECT * FROM day3.Persons p
LEFT OUTER JOIN day3.Person_Info i
ON p.PersonID = i.id
ORDER BY i.id;

--RightOuter Join
SELECT * FROM day3.Persons p
RIGHT OUTER JOIN day3.Person_Info i
ON p.PersonID = i.id
ORDER BY i.id;


--Full Outer Join
SELECT * FROM day3.Persons p
FULL OUTER JOIN day3.Person_Info i
ON p.PersonID = i.id
ORDER BY i.id;


