//We dont need to use extra vairable to store and return the result. Due to type inference scala returns the type

//As it uses type inference it does not need return type

def gradeMyScore(score: Int): String = {
  
  if (score >= 90) {
      "A"
  }
  else if (score >= 80) {
      "B"
  }
  else if (score >= 70) {
     "C"
  }
  else if (score >= 60) {
      "D"
  }
  else {
     "E"
  }
  
}

//Above Program using pattern matching

def gradeMyScore1(score: Int): String = {
  
  score match {
    case x1 if score >= 90 => "A"
    case x2 if score >= 80 => "B"
    case x3 if score >= 70 => "C"
    case x4 if score >= 60 => "D"
    case _ => "E"
  }
   
}
 
//Using Some and None
val getTodaysWeather : Option[String] = None
val getTodaysDay : Option[String] = Some("Monday")

//List Operations

val genderData = List(
  (1,"smith", "john", "john.smith@gmail.com", 1, None),
  (2,"smith", "kate", "kate.smith@gmail.com", 2, Some("female")))

//Function to find no of None cases in list

def getEmailsForMissingGendervalue(list : List[(Int, String, String, String, Int, Option[String])])  = {
  list.collect( l => l._6 match {case None => l})
}

//Unit Test

assert(getEmailsForMissingGendervalue(genderData).map(_._6 != None).length > 0 , "Error in Function")

//case class declaration in scalacase class Address (

  city : String, postalCode : String, streetAddress : String, aptNo : Option[Int] 
)

case class Person (
  id : Int, firstName: String, lastName : String, email : String, city : String, gender : Option[Char]
)

//Find no of persons in a city

def findPersonIn(city: String, people: Seq[Person]): Set[Person] = {
  var set : Set[Person] = Set()
  for (l <- people ) if (l.city == city) set= set + l
  set
}

//Unit Test

assert (findPersonIn("sunnyvale",persons) != 4, "Error in finding persons")

