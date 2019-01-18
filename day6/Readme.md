Given a dataframe of id, fname, lname, email, age:

write a udf function that accepts age as in integer and returns a boolean to indicate whether a person is a teen age or not
add a column "isTeenage" that uses the above udf function
display(id, fname, lname, email, age, isTeenage)
2) Given a dataframe of id, fname, lname, email, age, favColors favColors is a array of String e.g. ["red", "blue"]

write a udf function that accepts favColors as an array of string and returns a string // Array("red", "blue") => "red, blue"
write a udf function that accepts favColors as an array of string returns an integer denoting number of fav. colors
Add a column "favColorString" and favColorsCount that uses the above udf function
display(id, fname, lname, email, age, isTeeenage, favColorsString, favColorsCount)
3) Create a UDF to reverse a string

4) Create a UDF to return a current year

5) Create a UDF to return a boolean that checks if the value is null of not
4) Find Spark's implementation of concat() UDF and can you reason about how its implemented and compares to your own implmentation

Use of:
* array_contains
* array_distinct
* array_max
* explode // most frequently used
* map_keys
* map_values
