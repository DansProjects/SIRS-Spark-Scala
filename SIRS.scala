// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._
var answers = table("answers_txt").drop("created_at").drop("updated_at")
var courses = table("courses_fixed_txt").drop("created_at").drop("updated_at")
//display(courses)
//val combined = courses.join(answers, courses.col("id") === answers.col("course_Id")).drop(answers("id")).cache()
//display(combined)

// COMMAND ----------

def parseSources = udf((source: String) => {
  if (source == "Online Survey") 1 else 0
})

def calc = udf((enrollments: Int, responses: Int) => {
  if (enrollments > 0) (responses.toFloat / enrollments) else 0
} )

courses = courses.withColumn("online", parseSources(courses.col("source")))
courses = courses.withColumn("response_rate", calc( courses.col("enrollments"),courses.col("responses") ))
courses = courses.drop("source")
display(courses)

// COMMAND ----------

courses.groupBy("school").agg(avg("response_rate")).take(5).foreach(println)

// COMMAND ----------

var answers_txt = sc.textFile("/FileStore/tables/answers.txt")
answers_txt.take(5).foreach(println)

// COMMAND ----------

def computeRatings(line: String) = {
  val fields = line.split("\t")
  if(fields(0) != "id"){
    var total = ((fields(3).toInt)*1)+((fields(4).toInt)*2)+((fields(5).toInt)*3)+((fields(6).toInt)*4)+((fields(7).toInt)*5);
    var count = (fields(3).toInt+fields(4).toInt+fields(5).toInt+fields(6).toInt+fields(7).toInt).toFloat;
    if (total == 0 || count == 0) Some(0.toInt, 0.0.toFloat) else Some(fields(2).toInt,(total/count).toFloat)
  }else{
    None
  }
}

val ratings = answers_txt.flatMap(computeRatings)

// COMMAND ----------

val avgRatings = ratings.mapValues((_, 1)) //adds a 1 to every row, used for count - perhaps should put this in computeRatings function
  .reduceByKey( (x ,y) => (x._1+y._1, x._2 + y._2)) //sum up the ratings and also count
  .mapValues{ case (sum, count) => (1.0 * sum) / count} //compute average rating by dividing total ratings with count of ratings
  .sortByKey(); //sort by question id asc

// COMMAND ----------

//TODO join results with question text
avgRatings.take(25).foreach(println)
