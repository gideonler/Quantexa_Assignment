/*
This is my submission for the quantexa coding assignment.
Above the functions there are comments to issue out the purpose of what each function does
Inside the main class, run the codes and feel free to comment out the codes if certain parts of the codes are not running in order.
Done by Gideon Ler
*/
package example
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.io.PrintWriter


object Main {

  case class PassengerData(passengerId: Int, firstName: String, lastName: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Airplane App")
      .master("local[*]")
      .getOrCreate()

    val flightData: DataFrame = spark.read.option("header", true).csv("src/main/resources/flightData.csv")
    val passengerData: DataFrame = spark.read.option("header", true).csv("src/main/resources/passengers.csv")

    // Question 1
    val questionOneAnswer = calculateFlightsPerMonth(flightData)
    questionOneAnswer.show()

    // Question 2
    val questionTwoAnswer = calculateFrequentFlyers(passengerData, flightData)
    questionTwoAnswer.show()

    // Question 3
    val questionThreeAnswer = calculateLongestRunWithoutUK(flightData)
    questionThreeAnswer.show()

    // Question 4
    val questionFourAnswer = findPassengersWithMoreThan3FlightsTogether(flightData)
    questionFourAnswer.show()

    // Bonus Question
    val bonusQuestionAnswer = flownTogether(3, "2017-01-01", "2017-12-31", flightData)
    bonusQuestionAnswer.show()

    spark.stop()
  }


  /*
  This function calculates the total number of flights for each month
  1. The variable "output" will firstly add a new column called month by taking the month and date column
  2. It proceeds to group the dataframe by the "month" column
  3. Lastly it will aggregate the distinct number of "flightId" value for each month and sort the dataframe in an order
  4. The output will be stored in the CSV_Output folder under the folder called qs1
  */
  def calculateFlightsPerMonth(flightData: DataFrame): DataFrame = {
    val output=flightData
      .withColumn("month", month(to_date(col("date"), "yyyy-MM-dd")))
      .groupBy("month")
      .agg(countDistinct("flightId").alias("Number of Flights"))
      .orderBy("month")

    output
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("src/main/CSV_Output/qs1")

    output

  }

  /*
  This function will be used to calculate the number of flights for each passenger, and orders the result by the number of flights in descending order.
  1. Firstly, the variable "flightsPerPassenger" is used to calculate the number of
  flights for each passenger and the column created will be called "Number of Flights"
  2. The "frequentflyers" variable will be used to join the passenger Data with the flightsPerPassengerData to order the result
  base on the 100 most frequent flyers
  3. The next select statement selects "PassengerId, firstname, last name and number of flights"  and writes it into a csv file
  */
  def calculateFrequentFlyers(passengerData: DataFrame, flightData: DataFrame): DataFrame = {
    val flightsPerPassenger = flightData.groupBy("passengerId").agg(count("*").alias("Number of Flights"))
    val frequentFlyers = passengerData.join(flightsPerPassenger, Seq("passengerId")).orderBy(desc("Number of Flights")).limit(100)

    frequentFlyers.select(
      col("passengerId").alias("Passenger ID"),
      col("Number of Flights"),
      col("firstName").alias("First name"),
      col("lastName").alias("Last name")
    )

    frequentFlyers
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("src/main/CSV_Output/qs2")

    frequentFlyers
  }
  /*
    This function will be used to find the longest consecutive run of flights that do not arrive in the UK for each person
    1. First, we exclude the flightData with destinations to the UK
    2. We calculate the number of runs when previous destinations to the uk is 1 and 0 if otherwise
    3. Next, it determines the consecutive runs by calculating the difference in the run ids with the row and number of each passenger's window,
    and it then groups the passengerId and consecutive runs and computes the length of each run
    4. Next, the variable "longestRunPerPassenger" will group the consecutive runs by passenger and finds the maximum run length of each passenger
    5. Lastly, we will write it into a csv file
    */
  def calculateLongestRunWithoutUK(flightData: DataFrame): DataFrame = {
    val runsWithoutUK = flightData
      .filter(col("to") =!= "UK")
      .withColumn("run", when(lag("to", 1).over(Window.partitionBy("passengerId").orderBy("date")) =!= "UK", 1).otherwise(0))
      .withColumn("run_id", sum("run").over(Window.partitionBy("passengerId").orderBy("date")))

    val consecutiveRuns = runsWithoutUK
      .withColumn("consecutive_run", col("run_id") - row_number().over(Window.partitionBy("passengerId").orderBy("date")))
      .groupBy("passengerId", "consecutive_run")
      .agg(count("*").alias("run_length"))

    // Find the longest run for each passenger
    val longestRunPerPassenger = consecutiveRuns
      .groupBy("passengerId")
      .agg(max("run_length").alias("Longest Run"))
    val output=longestRunPerPassenger.orderBy(desc("Longest Run"))
    output
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("src/main/CSV_Output/qs3")

    output
  }
  /*
    This function will be used to find pairs of passengers who have been on more than three flights together
    1. First, we group thhe flightId and passengerId together and filter out flights where there is only 1 passenger. We then
    select pairs of passengers
    2. Join the flight data to find flights together base on passengerId1 or passengerId2, to count number of flights taken together
    3. We filter out the pairs with more than 3 flights together in the variable "flightsTogether"
    4. We print the results onto a csv file
    */
  def findPassengersWithMoreThan3FlightsTogether(flightData: DataFrame): DataFrame = {


    val passengerIdsColumn = col("passengerIds")
    val passengerPairs = flightData
      .select("passengerId", "flightId")
      .groupBy("flightId")
      .agg(collect_set("passengerId").alias("passengerIds"))
      .filter(size(passengerIdsColumn) > 1)
      .selectExpr("explode(passengerIds) as passengerId1", "posexplode(passengerIds) as idx, passengerId2")
      .filter("passengerId1 < passengerId2")


    val flightsTogether = flightData
      .join(passengerPairs, flightData("flightId") === passengerPairs("flightId") &&
        (flightData("passengerId") === passengerPairs("passengerId1") || flightData("passengerId") === passengerPairs("passengerId2")))
      .groupBy("passengerId1", "passengerId2")
      .agg(count("*").alias("Number_of_flights_together"))


    val result = flightsTogether.filter(col("Number_of_flights_together") > 3)
    result
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("src/main/CSV_Output/qs4")

    result


  }

  /*
  This function will be used to  identify pairs of passengers who have flown together at least a  number of times within a specific given date range.
  1. First, the code will define a date format, and the code will extract pairs of passengers who have shared the same flight by exploding the sets
  2. Next, the pairs of passengers who have shared the same flight will be taken and generated first
  3. In order to count the number of flights together, the variable "flightsTogether" will join and match the pairs on passengers either by
  "passengerId1" or "PassengerId2"
  4. Next, it will filter the pairs with the least number of flights together inside the "result" variable
  5. Lastlly, the result will be printed onto a csv file
  */
  def flownTogether(atLeastNTimes: Int, from: String, to: String, flightData: DataFrame): DataFrame = {
    val dateFormat = "yyyy-MM-dd"


    val fromDate = to_date(lit(from), dateFormat)
    val toDate = to_date(lit(to), dateFormat)

    val passengerPairs = flightData
      .select("passengerId", "flightId")
      .groupBy("flightId")
      .agg(collect_set("passengerId").alias("passengerIds"))
      .filter(size(col("passengerIds")) > 1)
      .selectExpr("explode(passengerIds) as passengerId1")
      .crossJoin(
        flightData
          .selectExpr("explode(passengerIds) as passengerId2")
          .filter("passengerId1 < passengerId2")
      )

    val flightsTogether = flightData
      .join(passengerPairs, flightData("flightId") === passengerPairs("flightId") &&
        (flightData("passengerId") === passengerPairs("passengerId1") || flightData("passengerId") === passengerPairs("passengerId2")))
      .groupBy("passengerId1", "passengerId2")
      .agg(count("*").alias("Number_of_flights_together"))

    val result = flightsTogether.filter(col("Number_of_flights_together") > atLeastNTimes)

    result
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("src/main/CSV_Output/bonus")

    result
  }





}
