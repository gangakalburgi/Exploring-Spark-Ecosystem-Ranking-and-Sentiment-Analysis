import org.apache.spark.sql.SQLContext
import org.apache.spark.util.ThreadUtils
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.{desc,asc}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._





case class Reviews(userId: Integer,movieId: Integer,rating: Double,timestamp: Integer);

val reviewsDs: Dataset[Reviews] = spark.read.option("header","true").option("inferSchema","true").csv("/Users/gangakalburgi/Desktop/spark/set2/reviews_large.csv").as[Reviews]



case class Movies(movieId: Integer,title: String,genres: String);

val moviesDs: Dataset[Movies] = spark.read.option("header","true").option("inferSchema","true").csv("/Users/gangakalburgi/Desktop/spark/set2/movies_large.csv").as[Movies]


println("query 1, top 10 movies with highest number of reviews")



moviesDs.join(reviewsDs,moviesDs("movieId") === reviewsDs("movieId")).groupBy(reviewsDs("movieId"),moviesDs("title")).agg(count("*").alias("review_cnt")).orderBy(desc("review_cnt")).show(10)



println(" ")
println(" ")
println("query 2 to print average rating")

val res1 = moviesDs.join(reviewsDs,moviesDs("movieId") === reviewsDs("movieId")).groupBy(reviewsDs("movieId"),moviesDs("title")).agg(avg("rating").alias("avg_rating")).filter("avg_rating > 4")
val res2 = moviesDs.join(reviewsDs,moviesDs("movieId") === reviewsDs("movieId")).groupBy(reviewsDs("movieId"),moviesDs("title")).agg(count("*").alias("NumOfRatings")).filter("NumOfRatings > 10").orderBy(desc("NumOfRatings"))
val res3 = res1.join(res2,"movieId").orderBy(desc("NumOfRatings"))

res3.show()






