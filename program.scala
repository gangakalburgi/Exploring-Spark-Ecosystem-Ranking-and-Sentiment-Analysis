import org.apache.spark.sql.SQLContext
import org.apache.spark.util.ThreadUtils
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.{desc,asc}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.SQLExecution


	val schema1 = new StructType().add("userId",IntegerType,true).add("movieId",IntegerType,true).add("rating",DoubleType,true).add("timestamp",IntegerType,true)


  val reviews = spark.read.format("csv").option("header","true").schema(schema1).option("mode","PERMISSIVE").load("/Users/gangakalburgi/Desktop/spark/set2/reviews_large.csv").toDF("userId" , "movieId","rating","timestamp")
  //reviews.repartition(10)

    val schema2 = new StructType().add("movieId",IntegerType,true).add("title",StringType,true).add("genre",StringType,true)


    val movies = spark.read.format("csv").option("header","true").schema(schema2).option("mode","PERMISSIVE").load("/Users/gangakalburgi/Desktop/spark/set2/movies_large.csv").toDF("movieId","title","genre")
    //movies.repartition(10)

 println(" ")
 println(" ")
 println(" ")

println("query 1, top 10 movies with highest number of reviews")
println("***********************************************************")
movies.join(reviews,movies("movieId") === reviews("movieId")).groupBy(reviews("movieId"),movies("title")).agg(count("*").alias("review_cnt")).orderBy(desc("review_cnt")).show(10)

 

println(" ")
println(" ")
println("query 2 to print average rating")
val   res1 = movies.join(reviews,movies("movieId") === reviews("movieId")).groupBy(reviews("movieId"),movies("title")).agg(avg("rating").alias("avg_rating")).filter("avg_rating > 4")

//res1.show()

val res2 = movies.join(reviews,movies("movieId") === reviews("movieId")).groupBy(reviews("movieId")).agg(count("*").alias("NumOfRatings")).filter("NumOfRatings > 10").orderBy(desc("NumOfRatings"))
//res2.show()

val res3 = res1.join(res2,"movieId").orderBy(desc("NumOfRatings"))

//printing the final result
res3.show()
  	
  	

