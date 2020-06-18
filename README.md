# Exploring-Spark-Ecosystem-Ranking-and-Sentiment-Analysis

They are two datasets -- two CSV files per dataset. 
The files are reviews.csv, movies.csv.  The input schema is the same for both these files. 
The only difference is the number of rows (100 thousand rows for small dataset vs. 26 million rows for a large dataset), i.e., the size of the datasets. 

 
The schema of these files is shown as follow:

Reviews(userId, movieId, rating, timestamp)

Movies(movieId, title, genres)

 

Problem Statement:

Problem-1: Rank the movie by their number of reviews(popularity) and select the top 10 highest ranked movies. 

Problem-2: Find all the movies that its average rating is greater than 4 stars and also each of these movies  should have more than 10 reviews

The problem is solved using  (1) Spark RDD, (2) Spark DataFrame (Spark SQL) and (3) Spark DataSet.
