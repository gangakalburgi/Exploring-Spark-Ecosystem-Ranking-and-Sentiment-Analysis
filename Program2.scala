import org.apache.spark.sql.SQLContext
import org.apache.spark.util.ThreadUtils
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.{desc,asc}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.SQLExecution
import spark.implicits._ 
import org.apache.spark.SparkContext



//mapping tuples

def mapToTuple(line: String): (Int, (Float,Int)) = {
	val fields = line.split(',')
	return (fields(1).toInt,(fields(2).toFloat,1))
} 

def mapToTuple2(line: String):(Int,(String)) = {
	val fields = line.split(',')
	return(fields(0).toInt,(fields(1).toString))
}

 

//reading data from input file stored locally in system

val ratings = sc.textFile("/Users/gangakalburgi/Desktop/spark/set2/reviews_large.csv",20)
val movies = sc.textFile("/Users/gangakalburgi/Desktop/spark/set2/movies_large.csv",20)

//extracting header from data
val RatingHeader   = ratings.first();

val MovieHeader = movies.first();

//filter data
val  filterRatings = ratings.filter(row => row != RatingHeader) 
// filterRatings.take(10)

val filterMovies = movies.filter(row => row != MovieHeader)


val movieDetails = filterMovies.map(mapToTuple2)

 
//counting number of reviews for movies and extracting moviews with highest number of ratings and sorting in descending order
val reviewcount = filterRatings.map(mapToTuple).reduceByKey((movie1,movie2) => (movie1._1 + movie2._1,movie1._2 + movie2._2)).sortBy(key_val => key_val._2._2,false)

println("Query1 - top 10 movies with highest Number of Reviews")
val RatingsRank_rdd = movieDetails.join(reviewcount).sortBy(key_val => key_val._2._2._2,false)

RatingsRank_rdd.take(10).foreach(println)



println("****************************")
println(" ")
println(" ")
println("query 2 to print average rating")

val avgRating = filterRatings.map(mapToTuple).reduceByKey((movie1,movie2) => (movie1._1 + movie2._1,movie1._2 + movie2._2)).map(movie1 => (movie1._1,movie1._2._1 / movie1._2._2))


 
 val filteravg = avgRating.filter(x => x._2 > 4)
 //filteravg.take(10).foreach(println)

 val NumOfRatings = reviewcount.filter(x => x._2._2 > 10)

 val TopRating = filteravg.join(NumOfRatings)

 val TopAvgRated = movieDetails.join(TopRating)
TopAvgRated.take(20).foreach(println)













 
 

 





