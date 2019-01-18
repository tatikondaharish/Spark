Select, alias, filter, sort, join
From customer, get all customers whose first names are Jamie
From customer, show the customers who paid the rental with amount is either less than 1USD or greater than 8USD
From film, Retrieve 4 films starting from the third one ordered by film_id
From film, get first five films sorted by the title
From customer, get customers whose first name contains "er" string e.g., Jenifer, Kimberl
From customer, get customer whose first name does not begin with "Jen". Is it case sensitive? If it's case insesitive, how do you make the match case sensitive or vice versa?
From rental, get the all the rental information of only customers with id 1 and 2
From rental, get the all the rental information for all customers except those with id 1 and 2
From rental, get a list of customer id of customers that has rental’s return date on 2005-05-27
From payment, select the payment whose amount is between 8 and 9 (USD)
From payment, select the payment whose amount is not between 8 and 9 (USD)
From customer, display full names of all customers and name this column as "full_name"
Each customer may have zero or many payments. Each payment belongs to one and only one customer. The customer_id field establishes the link between two tables. Display information (all columns) from both the tables and sort the result set by customer id
Filter the output of the #13 for the customer id 2
Using staff, payment and customer dataset, display the following columns: customer.customer_id, customer.first_name customer_first_name,customer.last_name customer_last_name, customer.email, staff.first_name staff_first_name,staff.last_name staff_last_name,payment.amount. Note: dataset name is prepended just to clarify the column and dataset relationship. Hint: you need to join the three datasets. Each staff relates to zero or many payments. Each payment is processed by one and only one staff.Each customer has zero or many payments. Each payment belongs to one and only customer.
Each row in the film may have zero or many rows in the inventory. Each row in the inventory has one and only one row in the film. select only films that are not in the inventory
From film, find all pair of films that have the same length. hint: Use Self-join
Aggregation
From payment, count the number of transactions each staff has been processing
From payment, show only customer who has been spending more than 200. Hint: You need to sum the amount column
From customner, select store that has more than 300 customers
Handeling Nulls
1) Create a dataframe called contacts using the following data

('John','Doe','john.doe@example.com',NULL),
('Lily','Bush','lily.bush@example.com','(408-234-2764)')
Find the contact who does not have a phone number
Find the contact who does have a phone number
2) Create two dataframe called departments and employees using the following data

// departments
(1,'Sales'),
 (2,'Marketing'),
 (3,'HR'),
 (4,'IT'),
 (5,'Production');

 // employees

 ('Bette Nicholson', 1),
 ('Christian Gable', 1),
 ('Joe Swank', 2),
 ('Fred Costner', 3),
 ('Sandra Kilmer', 4),
 ('Julia Mcqueen', NULL);
Find the department that does have any employee
Find the employee who does not belong to any department,
